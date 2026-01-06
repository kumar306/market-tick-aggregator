package worker

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/sony/gobreaker"
)

var WorkerTestingHook func()

// make worker a struct so it has its state within instead of passing state like a parameter
// and let the worker have process and flush function
type Worker struct {
	ID           int
	Channel      chan *constants.DispatchRecord
	SymbolState  map[string]*WindowState
	WindowConfig []*constants.WindowConfig
}

type WindowState struct {
	Exchange string
	Channel  string
	Symbol   string
	Windows  map[string]*constants.Window
}

func NewWorker(id int, ch chan *constants.DispatchRecord, cfg []*constants.WindowConfig) *Worker {
	symbolState := make(map[string]*WindowState)
	return &Worker{
		ID:           id,
		Channel:      ch,
		SymbolState:  symbolState,
		WindowConfig: cfg,
	}
}

func (w *Worker) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context shutdown. Stopping aggregator worker channel", "idx", w.ID)
			return
		case dispatchRec, ok := <-w.Channel:
			if !ok {
				logger.Log.Error("The worker channel is closed", "workerIdx", w.ID)
				return
			}
			switch dispatchRec.Event {
			case constants.ProcessEvent:
				w.ProcessTick(ctx, dispatchRec)
			case constants.FlushEvent:
				w.FlushWindow(ctx, dispatchRec)
			default:
				logger.Log.Info("Aggregator worker event received didn't match any known event", "event", dispatchRec.Event)
			}
		}
	}
}

func (w *Worker) ProcessTick(ctx context.Context,
	dispatchRec *constants.DispatchRecord) {
	// if not present, wire it and create all metrics - from the wired registry
	// else skip
	// update all window metrics
	start := time.Now().UnixMilli()

	workerState := w.SymbolState
	_, ok := workerState[dispatchRec.BufferKey]
	if !ok {
		// wire up the state
		// create window objects. for each window object, wire up the metrics
		// get window objects created via a window registry
		windowState := &WindowState{
			Windows:  internal.BuildWindows(w.WindowConfig),
			Exchange: dispatchRec.Tick.Exchange,
			Channel:  dispatchRec.Tick.Channel,
			Symbol:   dispatchRec.Tick.Symbol,
		}

		w.SymbolState[dispatchRec.BufferKey] = windowState

		metrics.Aggregator_WindowsPerSymbol.
			WithLabelValues(strconv.Itoa(w.ID), windowState.Exchange, windowState.Channel, windowState.Symbol).
			Set(float64(len(windowState.Windows)))
		metrics.Aggregator_SymbolsPerWorker.
			WithLabelValues(strconv.Itoa(w.ID)).
			Set(float64(len(w.SymbolState)))
	}

	tick := dispatchRec.Tick

	for _, window := range w.SymbolState[dispatchRec.BufferKey].Windows {
		for _, metric := range window.Metrics {
			metric.Update(tick)
		}
	}

	processingTime := time.Now().UnixMilli() - start
	metrics.Aggregator_TickProcessingDurationMs.
		WithLabelValues(strconv.Itoa(w.ID)).
		Observe(float64(processingTime))

	if nil != WorkerTestingHook {
		WorkerTestingHook()
	}
}

func (w *Worker) FlushWindow(ctx context.Context, flushRec *constants.DispatchRecord) {
	// get the worker state - get those windows having particular ID
	// flushing for a particular window ID
	// call apply
	// post into kafka aggregated_ticks
	cfg := flushRec.WindowConfig

	logger.Log.Info("Preparing to flush for window", "ID", cfg.Id, "Duration Ms", cfg.DurationMs, "Flush Cadency Ms", cfg.FlushCadencyMs, "Flush Timestamp", time.UnixMilli(flushRec.FlushTsMs))

	// per symbol window, create an aggregated tick,
	// enrich it with its window metric information
	// then persist to kafka

	for _, windowState := range w.SymbolState {
		start := time.Now().UnixMilli()

		window := windowState.Windows[cfg.Id]
		if window == nil {
			continue
		}

		aggregatedTick := &generated.AggregatedTick{}
		aggregatedTick.Symbol = windowState.Symbol
		aggregatedTick.Exchange = windowState.Exchange
		aggregatedTick.Channel = windowState.Channel
		aggregatedTick.WindowId = window.Id
		aggregatedTick.EndTsMs = flushRec.FlushTsMs
		aggregatedTick.StartTsMs = flushRec.FlushTsMs - cfg.DurationMs

		for _, metric := range window.Metrics {

			metric.Apply(aggregatedTick)

			// all metrics should implement reset
			// rolling metrics have no-op for Reset()
			// tumbling metrics are reset here
			metric.Reset()
		}

		if kafka.KafkaBreaker.State() == gobreaker.StateOpen {
			metrics.Aggregator_AggregatesDroppedTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
		} else {
			kafka.PublishAggregate(aggregatedTick)
			processingTime := time.Now().UnixMilli() - start
			metrics.Aggregator_WindowFlushDurationMs.WithLabelValues(
				aggregatedTick.WindowId, strconv.Itoa(w.ID)).
				Observe(float64(processingTime))
			metrics.Aggregator_AggregatesProducedTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
		}
	}
}
