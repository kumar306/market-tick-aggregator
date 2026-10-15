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
	"shared/ingestts"
	"shared/polldeadline"
	"shared/srcoffset"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"google.golang.org/protobuf/proto"
)

var WorkerTestingHook func()

// reorder field for faster gc scan
type Worker struct {
	FlushChannel chan *constants.DispatchRecord
	SymbolState  map[string]*WindowState
	WindowConfig []*constants.WindowConfig
	Checkpoints  map[string]map[constants.MetricName]constants.Metric
	tickScratch  *generated.NormalizedTick
	poll         *polldeadline.PollDeadline
	idLabel      string
	ID           int
	TxnFailed    atomic.Bool

	// at-least-once mode only (see EnableAtLeastOnce)
	atLeastOnce   bool
	symbolCkpts   map[string]*kafka.SymbolCheckpoint
	dirty         map[string]struct{} // symbols whose state changed since their last checkpoint
	topics        map[string]string   // interned source topic names, so reading a header does not allocate
	ckptDone      chan ckptResult
	batchInFlight bool
	lastCommitted map[tp]int64
}

// a consumed (topic, partition)
type tp struct {
	topic     string
	partition int32
}

// outcome of one checkpoint batch, handed back to the worker loop from the produce callbacks
type ckptResult struct {
	positions map[string]map[int32]kgo.EpochOffset
	keys      []string
	failed    bool
}

type WindowState struct {
	Exchange string
	Channel  string
	Symbol   string
	Windows  map[string]*constants.Window
	// source offsets already applied to windows (at least once); saved with the state in every checkpoint
	Seen srcoffset.SeenSet
}

func NewWorker(id int, flushCh chan *constants.DispatchRecord, cfg []*constants.WindowConfig, checkpoints map[string]map[constants.MetricName]constants.Metric) *Worker {
	return &Worker{
		ID:           id,
		FlushChannel: flushCh,
		SymbolState:  make(map[string]*WindowState),
		WindowConfig: cfg,
		Checkpoints:  checkpoints,
		tickScratch:  &generated.NormalizedTick{},
		idLabel:      strconv.Itoa(id),
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
		if w.atLeastOnce {
			windowState.Seen = srcoffset.SeenSet{}
			if ckpt := w.symbolCkpts[bufferKey]; ckpt != nil {
				for windowId, window := range windowState.Windows {
					if checkpointed, found := ckpt.Windows[windowId]; found {
						window.Metrics = checkpointed
					}
				}
				for k, v := range ckpt.Seen {
					windowState.Seen[k] = v
				}
			}
		} else {
			for windowId, window := range windowState.Windows {
				if checkpointed, found := w.Checkpoints[bufferKey+":"+windowId]; found {
					window.Metrics = checkpointed
				}
			}
		}

		w.SymbolState[bufferKey] = windowState

		metrics.Aggregator_WindowsPerSymbol.
			WithLabelValues(w.idLabel, windowState.Exchange, windowState.Channel, windowState.Symbol).
			Set(float64(len(windowState.Windows)))
		metrics.Aggregator_SymbolsPerWorker.
			WithLabelValues(w.idLabel).
			Set(float64(len(w.SymbolState)))
	}

	// a producer that crashed and re-sent records since its last offset commit gives us copies of ticks we already applied
	if w.atLeastOnce {
		if w.alreadyApplied(rec, windowState) {
			metrics.Aggregator_DedupeHitsTotal.WithLabelValues(windowState.Exchange, windowState.Symbol).Inc()
			return
		}
		w.dirty[bufferKey] = struct{}{}
	}

	ingestMs, hasIngest := ingestts.Find(rec.Headers)
	for _, window := range windowState.Windows {
		for _, metric := range window.Metrics {
			metric.Update(tick)
		}
		if hasIngest && ingestMs > window.NewestIngestMs {
			window.NewestIngestMs = ingestMs
		}
	}

	metrics.Aggregator_TicksIngestedTotal.WithLabelValues(w.idLabel).Inc()
	metrics.Aggregator_E2ELatencySeconds.Observe(float64(time.Now().UnixMilli()-tick.EventTsMillis) / 1000.0)

	processingTime := time.Now().UnixMilli() - start
	metrics.Aggregator_TickProcessingDurationMs.
		WithLabelValues(w.idLabel).
		Observe(float64(processingTime))

	if WorkerTestingHook != nil {
		WorkerTestingHook()
	}
}

func (w *Worker) FlushWindow(flushRec *constants.DispatchRecord, client utils.KafkaClient) {
	cfg := flushRec.WindowConfig

	for bufferKey, windowState := range w.SymbolState {
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

		// only ticks since this window's last flush are "new"; a quiet symbol must not report an ever-growing age
		ingestMs := window.NewestIngestMs
		window.NewestIngestMs = 0

		kafka.PublishAggregate(client, aggregatedTick, ingestMs, func() { w.TxnFailed.Store(true) })

		// the flush reset this symbol's tumbling metrics, so its saved state is now stale
		if w.atLeastOnce {
			w.dirty[bufferKey] = struct{}{}
		}

		processingTime := time.Now().UnixMilli() - start
		metrics.Aggregator_WindowFlushDurationMs.WithLabelValues(
			aggregatedTick.WindowId, w.idLabel).
			Observe(float64(processingTime))
		metrics.Aggregator_AggregatesProducedTotal.WithLabelValues(w.idLabel).Inc()
	}
}

func (w *Worker) GetTxnFailed() bool {
	return w.TxnFailed.Load()
}

// EnableAtLeastOnce switches the worker to the non-transactional mode driven by RunAtLeastOnce.
// ckpts are the per-symbol checkpoints loaded at startup; a symbol is restored from them the first time it is seen.
func (w *Worker) EnableAtLeastOnce(ckpts map[string]*kafka.SymbolCheckpoint) {
	w.atLeastOnce = true
	w.symbolCkpts = ckpts
	w.dirty = make(map[string]struct{})
	w.topics = make(map[string]string)
	w.ckptDone = make(chan ckptResult, 1)
	w.lastCommitted = make(map[tp]int64)
}

// alreadyApplied reports whether this record's source coordinate was already applied to the symbol's state.
// Records without the header (nothing upstream of us stamped them) cannot be checked and are applied.
func (w *Worker) alreadyApplied(rec *kgo.Record, ws *WindowState) bool {
	for _, h := range rec.Headers {
		if h.Key != srcoffset.HeaderKey {
			continue
		}
		topic, partition, offset, ok := srcoffset.Decode(h.Value)
		if !ok {
			return false
		}
		name, seen := w.topics[string(topic)]
		if !seen {
			name = string(topic)
			w.topics[name] = name
		}
		return ws.Seen.Check(srcoffset.SrcKey{Topic: name, Partition: partition}, offset)
	}
	return false
}

// RunAtLeastOnce is Run without Kafka transactions.
//   - window flushes are produced straight away; persistence drops a repeated (symbol, window, start_ts) row
//   - every checkpointEvery the changed symbols are saved, each as ONE record holding its window state and the
//     source offsets already applied
//   - input offsets are committed only after the checkpoint covering them is acked
//
// After a crash the worker restarts from the last committed offset with the last checkpoint. Ticks since then are
// re-applied, and any the checkpoint already contains are recognised by their source offset and dropped.
func (w *Worker) RunAtLeastOnce(ctx context.Context, cl *kgo.Client, checkpointEvery time.Duration) {
	defer cl.Close()

	ckptTimer := time.NewTimer(checkpointEvery)
	defer ckptTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			w.finalCheckpoint(cl)
			return

		case flushRec, ok := <-w.FlushChannel:
			if !ok {
				return
			}
			w.FlushWindow(flushRec, cl)

		case res := <-w.ckptDone:
			w.finishBatch(cl, res, false)

		case <-ckptTimer.C:
			w.checkpointDirty(cl)
			ckptTimer.Reset(checkpointEvery)

		default:
			pollCtx := w.poll.Reset(200 * time.Millisecond)
			fetches := cl.PollFetches(pollCtx)

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

// THE RULE THIS FUNCTION EXISTS TO KEEP: a tick's input offset may be committed only AFTER the checkpoint that
// contains that tick's effect has been acked. Commit first and crash before the checkpoint lands, and the restart
// begins reading past that tick (it is "already consumed" as far as Kafka is concerned) while the restored state
// does not contain it: the tick is silently lost, and nothing downstream can tell. Checkpoint first and crash
// before the commit, and the worst case is the tick is read again and recognised as seen. That is why the commit
// is never issued from here but only from finishBatch, after every checkpoint record of the batch is acked.
//
// SEVERAL SYMBOLS SHARE ONE PARTITION. Kafka keeps one bookmark per (topic, partition), not one per symbol, so the
// partition's offsets interleave its symbols:
//
//	normalized.ticks partition 0:   100=ETH  101=BTC  102=ETH  103=BTC     position (next to read) = 104
//
// Committing 104 means "restart after 103", which skips ETH@100, BTC@101, ETH@102 and BTC@103 all at once.
// That is only safe if EVERY symbol with a tick in that range has its checkpoint acked. So every dirty symbol is
// encoded and produced in the same batch, and the one position is committed only when all of them are acked.
// There is no per-symbol offset to compare, and no min or max over symbols: the number committed is the
// partition's read position, taken before encoding so it matches exactly what the checkpoints contain.
//
// checkpointDirty saves every symbol that changed, then (once all are acked) commits the input offsets that were
// polled when the snapshot was taken. Ticks are applied synchronously on this goroutine, so state and positions
// are consistent with each other at the moment of the snapshot.
// One batch at a time: if one is still in flight this does nothing and the next timer tick tries again.
func (w *Worker) checkpointDirty(cl *kgo.Client) {
	if w.batchInFlight {
		return
	}
	positions := cl.UncommittedOffsets()

	if len(w.dirty) == 0 {
		w.commitPositions(cl, positions, false) // nothing to save, only dropped duplicates moved the position
		return
	}

	type encoded struct {
		key string
		val []byte
	}
	batch := make([]encoded, 0, len(w.dirty))
	for key := range w.dirty {
		ws := w.SymbolState[key]
		ckpt := kafka.SymbolCheckpoint{Windows: make(map[string]map[constants.MetricName]constants.Metric, len(ws.Windows)), Seen: ws.Seen}
		for id, window := range ws.Windows {
			ckpt.Windows[id] = window.Metrics
		}
		val, err := kafka.EncodeSymbolCheckpoint(ckpt)
		if err != nil {
			// without this symbol saved, committing the positions would lose its ticks on a crash
			logger.Log.Error("Failed to encode symbol checkpoint, skipping this batch", "worker", w.ID, "bufferKey", key, "err", err)
			return
		}
		batch = append(batch, encoded{key, val})
	}

	w.batchInFlight = true
	keys := make([]string, len(batch))
	// maintain size of the batch so on hits 0 , we fire a checkpoint done event which commits and brings upstream topic to latest
	remaining := atomic.Int32{}
	remaining.Store(int32(len(batch)))
	var failed atomic.Bool
	for i, e := range batch {
		keys[i] = e.key
		delete(w.dirty, e.key)
	}
	for _, e := range batch {
		kafka.ProduceSymbolCheckpoint(cl, e.key, e.val, func(err error) {
			if err != nil {
				failed.Store(true)
			}
			if remaining.Add(-1) == 0 {
				w.ckptDone <- ckptResult{positions: positions, keys: keys, failed: failed.Load()}
			}
		})
	}
}

func (w *Worker) finishBatch(cl *kgo.Client, res ckptResult, sync bool) {
	w.batchInFlight = false
	if res.failed {
		// not saved, so the positions must not move; the symbols are saved again next time
		for _, key := range res.keys {
			w.dirty[key] = struct{}{}
		}
		logger.Log.Error("Checkpoint batch failed; offsets not committed", "worker", w.ID)
		return
	}
	w.commitPositions(cl, res.positions, sync)
}

// commitPositions commits offsets that are already covered by an acked checkpoint. It is async, so its round trips
// are off the visibility path.
func (w *Worker) commitPositions(cl *kgo.Client, positions map[string]map[int32]kgo.EpochOffset, sync bool) {
	advanced := make(map[string]map[int32]kgo.EpochOffset)
	for topic, parts := range positions {
		for p, eo := range parts {
			k := tp{topic: topic, partition: p}
			if prev, ok := w.lastCommitted[k]; ok && eo.Offset <= prev {
				continue
			}
			w.lastCommitted[k] = eo.Offset
			if advanced[topic] == nil {
				advanced[topic] = make(map[int32]kgo.EpochOffset)
			}
			advanced[topic][p] = eo
		}
	}
	if len(advanced) == 0 {
		return
	}

	start := time.Now()
	onDone := func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			logger.Log.Error("Offset commit failed", "worker", w.ID, "err", err)
			return
		}
		metrics.Aggregator_CommitLatencySeconds.Observe(time.Since(start).Seconds())
	}
	if sync {
		cl.CommitOffsetsSync(context.Background(), advanced, onDone)
		return
	}
	cl.CommitOffsets(context.Background(), advanced, onDone)
}

// finalCheckpoint is the grac.eful stop: let an in-flight batch land, save what is left, commit synchronously.
func (w *Worker) finalCheckpoint(cl *kgo.Client) {
	wait := func() {
		if !w.batchInFlight {
			return
		}
		select {
		case res := <-w.ckptDone:
			w.finishBatch(cl, res, true)
		case <-time.After(5 * time.Second):
			logger.Log.Error("Timed out waiting for the last checkpoint batch", "worker", w.ID)
		}
	}
	wait()
	w.checkpointDirty(cl)
	wait()
}
