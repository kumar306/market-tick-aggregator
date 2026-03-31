package batcher

import (
	"context"
	"encoding/json"
	"market-persistence/batcher/util"
	"market-persistence/config"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type BatchEvent int

const (
	AddEvent BatchEvent = iota
	TimerEvent
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
type Batcher[T any] struct {
	pipeline       string
	ctx            context.Context
	batchCh        chan *BatchMessage[T]
	items          []*BatchItem[T]
	intervalMs     time.Duration
	lastFlush      time.Time
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
	intervalMs time.Duration,
	flushFn func(context.Context, util.Tx, []T) error,
	invalidCheckFn func(T) error,
	sink util.Sink,
	eventProcessor util.EventProcessor) Batcher[T] {
	logger.Log.Info("Starting batcher", "batchSize", batchSize, "intervalMs", intervalMs)
	return Batcher[T]{
		batchSize:      batchSize,
		pipeline:       pipeline,
		batchCh:        make(chan *BatchMessage[T], batchSize*3),
		intervalMs:     intervalMs,
		flushFn:        flushFn,
		invalidCheckFn: invalidCheckFn,
		items:          make([]*BatchItem[T], 0, batchSize),
		lastFlush:      time.Now(),
		ctx:            ctx,
		sink:           sink,
		eventProcessor: eventProcessor,
	}
}

// loop reading from internal channel
// Add() - posts into batch channel
// ticker - posts into same batch channel
func (b *Batcher[T]) Run() {
	ticker := time.NewTicker(b.intervalMs)
	for {
		select {
		case <-b.ctx.Done():
			logger.Log.Info("Received ctx done. Flushing and closing batcher internal channel loop")

			if len(b.items) > 0 {
				_ = b.Flush()
			}

			return
		case <-ticker.C:
			select {
			case b.batchCh <- &BatchMessage[T]{
				BatchEvent: TimerEvent,
			}:
			default:
				logger.Log.Info("Dropping timer event because internal channel full.")
				metrics.Persistence_BatchDroppedTimers.WithLabelValues(b.pipeline).Inc()
			}
		case msg := <-b.batchCh:
			switch msg.BatchEvent {
			case AddEvent:
				b.HandleBatchAndFlush(&msg.Item)
			case TimerEvent:
				b.FlushIfNeeded()
			}
		}
	}
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

// flush on items size exceeds limit
func (b *Batcher[T]) HandleBatchAndFlush(item *BatchItem[T]) error {
	b.items = append(b.items, item)
	metrics.Persistence_BatchSize.WithLabelValues(b.pipeline).Observe(float64(len(b.items)))

	if len(b.items) >= b.batchSize {
		if err := b.Flush(); err != nil {
			logger.Log.Error("Error in batcher flush", "error", err, "pipeline", b.pipeline)
			return err
		}
	}

	return nil
}

// timer based flush
func (b *Batcher[T]) FlushIfNeeded() error {
	if len(b.items) == 0 {
		logger.Log.Info("Batch periodic flush -- No items present in batcher", "pipeline", b.pipeline)
		return nil
	}

	if time.Since(b.lastFlush) >= b.intervalMs {
		logger.Log.Info("Time since last flush >= Interval", "pipeline", b.pipeline)
		return b.Flush()
	}

	return nil
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
		logger.Log.Warn("No valid records to flush after filtering out corrupted records. Returning from Flush()")
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
	metrics.Persistence_BatchFlushDuration.WithLabelValues(b.pipeline).Observe(float64(time.Since(flushStart)))

	if err := tx.Commit(b.ctx); err != nil {
		logger.Log.Error("Error in commit", "error", err)
		metrics.Persistence_TxnFailures.WithLabelValues(b.pipeline).Inc()
		return err
	}

	for _, item := range b.items {
		maxOffsetPerPartitionMap[item.Record.Partition] = max(maxOffsetPerPartitionMap[item.Record.Partition], item.Record.Offset)
	}

	// commit offsets upon db write success
	// fire and forget
	b.eventProcessor.MarkUpstreamProcessed(b.ctx, maxOffsetPerPartitionMap)

	// clear the batch
	b.items = b.items[:0]
	b.lastFlush = time.Now()

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
