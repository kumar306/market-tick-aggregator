package worker

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/dedupe"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
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

func (w *Worker) Run(ctx context.Context, client utils.KafkaClient) {
	for {
		select {
		case <-ctx.Done():
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
				w.FlushWindow(ctx, dispatchRec, client)
			default:
				logger.Log.Warn("Aggregator worker event received didn't match any known event", "event", dispatchRec.Event)
			}
		}
	}
}

// before processing it, check if exists in redis
// publish and then mark dedupe
// mark committed records and then commit offsets

func (w *Worker) ProcessTick(ctx context.Context,
	dispatchRec *constants.DispatchRecord) {
	// if not present, wire it and create all metrics - from the wired registry
	// else skip
	// update all window metrics
	start := time.Now().UnixMilli()

	// dedupe mark occurs only after publish.
	// one record will be used to flush for all windows. as soon as its applied, if we mark for dedupe, then if service crashed, its data loss for us
	// but we know metrics converge and they are not source of truth. they are constructed
	// its better if immediately dup check, then apply to windows and store in redis. as only metrics are persisted down and not actual record
	// commit offsets manually after done with the record

	dedupeStartTime := time.Now()
	dedupeKey := dedupe.ConstructDedupeKey(dispatchRec.Record.Topic, dispatchRec.Record.Partition, dispatchRec.Record.Offset)
	dedupeExists, err := dedupe.IsDuplicate(ctx, dedupeKey)
	if err != nil {
		metrics.Aggregator_DedupeErrorsTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Symbol).Inc()
		logger.Log.Error("Error in worker dedupe check", "err", err, "key", dedupeKey)
		return
	}

	metrics.Aggregator_DedupeChecksTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Symbol).Inc()

	if dedupeExists {
		metrics.Aggregator_DedupeHitsTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Symbol).Inc()
		return
	}

	dedupeLatency := time.Since(dedupeStartTime).Seconds()
	metrics.Aggregator_DedupeLatencySeconds.WithLabelValues(
		dispatchRec.Exchange,
		dispatchRec.Symbol).Observe(dedupeLatency)

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

	// mark for dedupe and mark for commit
	dedupeErr := dedupe.MarkForDedupe(ctx, dedupe.ConstructDedupeKey(dispatchRec.Record.Topic, dispatchRec.Record.Partition, dispatchRec.Record.Offset))
	if dedupeErr != nil {
		metrics.Aggregator_DedupeStoreErrorsTotal.WithLabelValues(dispatchRec.Exchange, dispatchRec.Symbol).Inc()
	}

	if kafka.Client != nil {
		kafka.Client.MarkCommitRecords(dispatchRec.Record)
	}

}

func (w *Worker) FlushWindow(ctx context.Context, flushRec *constants.DispatchRecord, client utils.KafkaClient) {
	// get the worker state - get those windows having particular ID
	// flushing for a particular window ID
	// call apply
	// post into kafka aggregated_ticks
	cfg := flushRec.WindowConfig

	// per symbol window, create an aggregated tick,
	// enrich it with its window metric information
	// then persist to kafka

	for _, windowState := range w.SymbolState {
		start := time.Now().UnixMilli()

		window := windowState.Windows[cfg.Id]
		if window == nil {
			logger.Log.Warn("Window is nil. Skipping", "worker", w.ID, "windowId", cfg.Id, "durationMs", cfg.DurationMs)
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
			logger.Log.Warn("Circuit breaker is open. Dropping the aggregate", "exchange", windowState.Exchange, "symbol", windowState.Symbol)
			metrics.Aggregator_AggregatesDroppedTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
		} else {
			kafka.PublishAggregate(aggregatedTick, client)
			processingTime := time.Now().UnixMilli() - start
			metrics.Aggregator_WindowFlushDurationMs.WithLabelValues(
				aggregatedTick.WindowId, strconv.Itoa(w.ID)).
				Observe(float64(processingTime))
			metrics.Aggregator_AggregatesProducedTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
		}

	}
}
