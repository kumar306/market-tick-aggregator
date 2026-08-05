package worker

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"shared/logger"
	"shared/metrics"
	"shared/polldeadline"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var WorkerTestingHook func()

type Worker struct {
	ID           int
	FlushChannel chan *constants.DispatchRecord
	SymbolState  map[string]*WindowState
	WindowConfig []*constants.WindowConfig
	Checkpoints  map[string]map[constants.MetricName]constants.Metric
	TxnFailed    atomic.Bool
	tickScratch  *generated.NormalizedTick
	poll         *polldeadline.PollDeadline
}

type WindowState struct {
	Exchange string
	Channel  string
	Symbol   string
	Windows  map[string]*constants.Window
}

func NewWorker(id int, flushCh chan *constants.DispatchRecord, cfg []*constants.WindowConfig, checkpoints map[string]map[constants.MetricName]constants.Metric) *Worker {
	return &Worker{
		ID:           id,
		FlushChannel: flushCh,
		SymbolState:  make(map[string]*WindowState),
		WindowConfig: cfg,
		Checkpoints:  checkpoints,
		tickScratch:  &generated.NormalizedTick{},
		poll:         polldeadline.New(),
	}
}

// owns the worker's transaction lifecycle.
// ticks, window flushes and the commit interval boundary are all serialized in 1 goroutine.
// no concurrent kgo session produce/begin/end, which is forbidden
func (w *Worker) Run(ctx context.Context, session *kgo.GroupTransactSession, commitInterval time.Duration) {
	defer session.Close()

	if err := session.Begin(); err != nil {
		logger.Log.Error("Failed to begin initial transaction", "worker", w.ID, "err", err)
		return
	}

	commitTicker := time.NewTicker(commitInterval)
	defer commitTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.endTransaction(context.Background(), session)
			return

		case flushRec, ok := <-w.FlushChannel:
			if !ok {
				return
			}
			w.FlushWindow(flushRec, session)

		case <-commitTicker.C:
			w.endTransaction(ctx, session)
			if err := session.Begin(); err != nil {
				logger.Log.Error("Failed to begin next transaction", "worker", w.ID, "err", err)
				return
			}

		default:
			// replace ctx.withTimeout with a custom pollDeadline which doesnt allocate heap memory on each fetch
			pollCtx := w.poll.Reset(200 * time.Millisecond)
			fetches := session.PollFetches(pollCtx)

			if fetches.IsClientClosed() {
				return
			}

			fetches.EachRecord(func(rec *kgo.Record) {
				w.ProcessTick(rec)
			})

			fetches.EachError(func(topic string, partition int32, err error) {
				logger.Log.Error("Fetch error", "worker", w.ID, "topic", topic, "partition", partition, "err", err)
			})
		}
	}
}

// endTransaction commits everything produced (window flushes), everything consumed (ticks) since the last boundary atomically.
// if any produce in this cycle failed, the whole cycle aborts instead.
// abort - nothing in this cycle was ever visible.
// commit - entire cycle is visible.
func (w *Worker) endTransaction(ctx context.Context, session *kgo.GroupTransactSession) {
	w.checkpointWindows(session)

	commit := kgo.TryCommit
	if w.TxnFailed.Swap(false) {
		commit = kgo.TryAbort
		logger.Log.Warn("Aborting transaction due to a produce failure this cycle", "worker", w.ID)
	}

	if _, err := session.End(ctx, commit); err != nil {
		logger.Log.Error("Transaction end failed", "worker", w.ID, "err", err)
	}
}

// publish checkpoint for each bufferkey + windowId at the txn boundary
func (w *Worker) checkpointWindows(session *kgo.GroupTransactSession) {
	for bufferKey, windowState := range w.SymbolState {
		for windowId, window := range windowState.Windows {
			kafka.PublishCheckpoint(session, bufferKey, windowId, window.Metrics, func() { w.TxnFailed.Store(true) })
		}
	}
}

func (w *Worker) ProcessTick(rec *kgo.Record) {
	start := time.Now().UnixMilli()

	// use a scratch object rather than allocating heap memory on every tick
	tick := w.tickScratch
	tick.Reset()
	if err := proto.Unmarshal(rec.Value, tick); err != nil {
		logger.Log.Error("Error in unmarshalling proto to normalized tick", "error", err)
		return
	}

	bufferKey := tick.Exchange + ":" + tick.Channel + ":" + tick.Symbol

	windowState, ok := w.SymbolState[bufferKey]
	if !ok {
		windowState = &WindowState{
			Windows:  internal.BuildWindows(w.WindowConfig),
			Exchange: tick.Exchange,
			Channel:  tick.Channel,
			Symbol:   tick.Symbol,
		}

		// rehydrate from the last checkpoint, if exists
		// this protects a long-duration window's accumulated state from a crash that
		// happens after its contributing ticks' offsets were already committed
		for windowId, window := range windowState.Windows {
			if checkpointed, found := w.Checkpoints[bufferKey+":"+windowId]; found {
				window.Metrics = checkpointed
			}
		}

		w.SymbolState[bufferKey] = windowState

		metrics.Aggregator_WindowsPerSymbol.
			WithLabelValues(strconv.Itoa(w.ID), windowState.Exchange, windowState.Channel, windowState.Symbol).
			Set(float64(len(windowState.Windows)))
		metrics.Aggregator_SymbolsPerWorker.
			WithLabelValues(strconv.Itoa(w.ID)).
			Set(float64(len(w.SymbolState)))
	}

	for _, window := range windowState.Windows {
		for _, metric := range window.Metrics {
			metric.Update(tick)
		}
	}

	processingTime := time.Now().UnixMilli() - start
	metrics.Aggregator_TickProcessingDurationMs.
		WithLabelValues(strconv.Itoa(w.ID)).
		Observe(float64(processingTime))

	if WorkerTestingHook != nil {
		WorkerTestingHook()
	}
}

func (w *Worker) FlushWindow(flushRec *constants.DispatchRecord, client utils.KafkaClient) {
	cfg := flushRec.WindowConfig

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

		kafka.PublishAggregate(client, aggregatedTick, func() { w.TxnFailed.Store(true) })

		processingTime := time.Now().UnixMilli() - start
		metrics.Aggregator_WindowFlushDurationMs.WithLabelValues(
			aggregatedTick.WindowId, strconv.Itoa(w.ID)).
			Observe(float64(processingTime))
		metrics.Aggregator_AggregatesProducedTotal.WithLabelValues(strconv.Itoa(w.ID)).Inc()
	}
}

func (w *Worker) GetTxnFailed() bool {
	return w.TxnFailed.Load()
}
