package batcher

import (
	"context"
	"encoding/json"
	"market-persistence/batcher/util"
	"market-persistence/config"
	"shared/ingestts"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BatchEvent int

const (
	AddEvent BatchEvent = iota
)

type BatchItem[T any] struct {
	Item   T
	Record *kgo.Record
}

// format for internal channel
type BatchMessage[T any] struct {
	BatchEvent BatchEvent
	Item       BatchItem[T]
}

type BatcherAdder[U any] interface {
	Add(item BatchItem[U])
}

// batching logic is shared across both consumers. only flush fn differs
// better to have 2 instantiations of the batcher rather than batcher interface
//
// A batch is flushed when it reaches batchSize OR when maxWait has passed since its FIRST row arrived, whichever is
// first. The clock starts with the first row (a per-batch timer), not on a free-running tick, so a row never waits
// longer than maxWait plus the flush itself. The previous design (a ticker plus "has a full interval passed since the
// last flush") flushed every OTHER tick whenever the two intervals matched, so rows waited up to 2x the interval.
type Batcher[T any] struct {
	pipeline       string
	ctx            context.Context
	batchCh        chan *BatchMessage[T]
	items          []*BatchItem[T]
	maxWait        time.Duration
	linger         *time.Timer
	lingerArmed    bool
	batchSize      int
	flushFn        func(context.Context, util.Tx, []T) error
	invalidCheckFn func(T) error
	sink           util.Sink
	eventProcessor util.EventProcessor
}

func NewBatcher[T any](
	ctx context.Context,
	pipeline string,
	batchSize int,
	maxWait time.Duration,
	flushFn func(context.Context, util.Tx, []T) error,
	invalidCheckFn func(T) error,
	sink util.Sink,
	eventProcessor util.EventProcessor) Batcher[T] {
	logger.Log.Info("Starting batcher", "batchSize", batchSize, "maxWait", maxWait)
	linger := time.NewTimer(time.Hour)
	linger.Stop()
	return Batcher[T]{
		batchSize:      batchSize,
		pipeline:       pipeline,
		batchCh:        make(chan *BatchMessage[T], batchSize*3),
		maxWait:        maxWait,
		linger:         linger,
		flushFn:        flushFn,
		invalidCheckFn: invalidCheckFn,
		items:          make([]*BatchItem[T], 0, batchSize),
		ctx:            ctx,
		sink:           sink,
		eventProcessor: eventProcessor,
	}
}

// loop reading from internal channel
// Add() - posts into batch channel
// linger timer - fires maxWait after the first row of the current batch arrived
func (b *Batcher[T]) Run() {
	for {
		select {
		case <-b.ctx.Done():
			logger.Log.Info("Received ctx done. Flushing and closing batcher internal channel loop")

			b.stopLinger()
			if len(b.items) > 0 {
				_ = b.Flush()
			}

			return
		case <-b.linger.C:
			b.lingerArmed = false
			if err := b.FlushIfNeeded(); err != nil {
				// nothing else would retry a failed batch until another row arrived, so retry after another maxWait
				logger.Log.Error("Error in batcher timed flush, will retry", "error", err, "pipeline", b.pipeline)
				b.armLinger()
			}
		case msg := <-b.batchCh:
			switch msg.BatchEvent {
			case AddEvent:
				_ = b.HandleBatchAndFlush(&msg.Item)
			}
		}
	}
}

// start the wait clock, unless one is already running for the current batch
func (b *Batcher[T]) armLinger() {
	if b.lingerArmed {
		return
	}
	b.linger.Reset(b.maxWait)
	b.lingerArmed = true
}

func (b *Batcher[T]) stopLinger() {
	b.linger.Stop()
	b.lingerArmed = false
}

// add event - struct with event type, item - posted into channel
// channel reads this struct - calls Add() which does add and flush
// ticker event - struct - posted into channel with event
// channel reads this struct - calls FlushIfNeeded()

// add to channel
func (b *Batcher[T]) Add(item BatchItem[T]) {
	select {
	case b.batchCh <- &BatchMessage[T]{
		BatchEvent: AddEvent,
		Item:       item,
	}:
	default:
		logger.Log.Warn("Dropping message from batch channel as its overloaded")
		metrics.Persistence_BatchDroppedItems.WithLabelValues(b.pipeline).Inc()
	}
}

// add a row. The first row of a batch starts the maxWait clock; reaching batchSize flushes immediately.
func (b *Batcher[T]) HandleBatchAndFlush(item *BatchItem[T]) error {
	b.items = append(b.items, item)
	b.armLinger()
	metrics.Persistence_BatchSize.WithLabelValues(b.pipeline).Observe(float64(len(b.items)))

	if len(b.items) >= b.batchSize {
		if err := b.Flush(); err != nil {
			logger.Log.Error("Error in batcher flush", "error", err, "pipeline", b.pipeline)
			return err
		}
	}

	return nil
}

// called when the maxWait clock fires: flush whatever the batch holds
func (b *Batcher[T]) FlushIfNeeded() error {
	if len(b.items) == 0 {
		return nil
	}
	return b.Flush()
}

// delegate the flush fn to callbacks which we will write for tick and book
// batcher opens db agnostic transaction. its not aware about postgres. can change the DB anytime by wiring up new Sink
func (b *Batcher[T]) Flush() error {
	start := time.Now()

	// error handling for corrupted records
	valid := make([]*BatchItem[T], 0, len(b.items))
	var maxOffsetPerPartitionMap map[int32]int64 = make(map[int32]int64)

	for _, batchItem := range b.items {
		if err := b.invalidCheckFn(batchItem.Item); err != nil {
			logger.Log.Error("invalid record, sending to DLQ",
				"pipeline", b.pipeline,
				"partition", batchItem.Record.Partition,
				"offset", batchItem.Record.Offset)

			errorMsg := err.Error()

			payload, jsonErr := ToBytes(batchItem.Item)
			if jsonErr != nil {
				logger.Log.Error("Payload data is corrupted", "jsonErr", jsonErr)
				errorMsg += " " + jsonErr.Error()
			}

			dlqMessage := &config.DLQMessage{
				Topic:     b.pipeline,
				Payload:   payload,
				ErrorMsg:  errorMsg,
				Timestamp: time.Now(),
			}

			if handleErrorErr := b.eventProcessor.HandleEventError(b.ctx, dlqMessage); handleErrorErr != nil {
				logger.Log.Error("Failed to handle event error", "err", handleErrorErr)
				return handleErrorErr
			}

			// if corrupted record was the last one in the batch, it will cause problem as it wouldnt be marked in map if invalid. so add it nevertheless
			maxOffsetPerPartitionMap[batchItem.Record.Partition] = max(maxOffsetPerPartitionMap[batchItem.Record.Partition], batchItem.Record.Offset)

		} else {
			valid = append(valid, batchItem)
		}
	}

	if len(valid) == 0 {
		// every row went to the DLQ above. They are handled, so clear them and move the offsets on; leaving them in
		// the batch would send them to the DLQ again on every later flush (every maxWait, once the wait is short)
		logger.Log.Warn("No valid records to flush after filtering out corrupted records. Clearing the batch")
		b.eventProcessor.MarkUpstreamProcessed(b.ctx, maxOffsetPerPartitionMap)
		b.items = b.items[:0]
		b.stopLinger()
		return nil
	}

	tx, err := b.sink.InitTx(b.ctx)
	if err != nil {
		logger.Log.Error("Error in beginning transaction", "error", err)
		return err
	}
	defer tx.Rollback(b.ctx)

	items := make([]T, 0)
	for _, item := range valid {
		items = append(items, item.Item)
	}

	flushStart := time.Now()
	if err := b.flushFn(b.ctx, tx, items); err != nil {
		logger.Log.Error("Error in flushing rows", "error", err)
		metrics.Persistence_TxnFailures.WithLabelValues(b.pipeline).Inc()
		return err
	}
	metrics.Persistence_BatchFlushDuration.WithLabelValues(b.pipeline).Observe(time.Since(flushStart).Seconds())

	if err := tx.Commit(b.ctx); err != nil {
		logger.Log.Error("Error in commit", "error", err)
		metrics.Persistence_TxnFailures.WithLabelValues(b.pipeline).Inc()
		return err
	}

	for _, item := range b.items {
		maxOffsetPerPartitionMap[item.Record.Partition] = max(maxOffsetPerPartitionMap[item.Record.Partition], item.Record.Offset)
	}

	// end-to-end latency, from the websocket read of the newest message each row reflects to NOW, the moment the
	// rows became visible. It has to be measured here, after Commit: inside the flush function the rows are still
	// invisible, which would under-report by the commit time, and it must include the time the item waited in
	// this batcher for the batch to fill or the timer to fire.
	committedAtMs := time.Now().UnixMilli()
	for _, item := range valid {
		if item.Record == nil {
			continue
		}
		if ingestMs, ok := ingestts.Find(item.Record.Headers); ok {
			metrics.Persistence_IngestToInsertSeconds.WithLabelValues(b.pipeline).Observe(float64(committedAtMs-ingestMs) / 1000)
		}
	}

	// commit offsets upon db write success
	// fire and forget
	b.eventProcessor.MarkUpstreamProcessed(b.ctx, maxOffsetPerPartitionMap)

	// clear the batch; the next row starts a new wait
	b.items = b.items[:0]
	b.stopLinger()

	metrics.Persistence_TxnDuration.WithLabelValues(b.pipeline).Observe(float64(time.Since(start).Seconds()))
	metrics.Persistence_BatchSize.WithLabelValues(b.pipeline).Observe(0)
	metrics.Persistence_FlushCount.WithLabelValues(b.pipeline).Inc()

	return nil
}

func ToBytes[T any](s T) ([]byte, error) {
	bytes, err := json.Marshal(s)
	if err != nil {
		return nil, logger.LogAndWrap("json marshal failed", err)
	}
	return bytes, nil
}
