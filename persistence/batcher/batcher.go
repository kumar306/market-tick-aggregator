package batcher

import (
	"context"
	"market-persistence/batcher/util"
	"shared/logger"
	"time"
)

type BatchEvent int

const (
	AddEvent BatchEvent = iota
	TimerEvent
)

type BatchItem[T any] struct {
	Item      T
	Partition int32
	Offset    int64
}

// format for internal channel
type BatchMessage[T any] struct {
	BatchEvent BatchEvent
	Item       BatchItem[T]
}

// batching logic is shared across both consumers. only flush fn differs
// better to have 2 instantiations of the batcher rather than batcher interface
type Batcher[T any] struct {
	ctx             context.Context
	batchCh         chan *BatchMessage[T]
	items           []*BatchItem[T]
	intervalMs      time.Duration
	lastFlush       time.Time
	batchSize       int
	flushFn         func(context.Context, util.Tx, []T) error
	sink            util.Sink
	offsetCommitter util.OffsetCommitter
}

func NewBatcher[T any](
	ctx context.Context,
	batchSize int,
	intervalMs time.Duration,
	flushFn func(context.Context, util.Tx, []T) error,
	sink util.Sink,
	offsetCommitter util.OffsetCommitter) Batcher[T] {
	return Batcher[T]{
		batchSize:       batchSize,
		batchCh:         make(chan *BatchMessage[T], batchSize*3),
		intervalMs:      intervalMs,
		flushFn:         flushFn,
		items:           make([]*BatchItem[T], 0, batchSize),
		lastFlush:       time.Now(),
		ctx:             ctx,
		sink:            sink,
		offsetCommitter: offsetCommitter,
	}
}

// loop reading from internal channel
// Add() - posts into batch channel
// ticker - posts into same batch channel
func (b *Batcher[T]) Run() {
	ticker := time.NewTicker(b.intervalMs * time.Millisecond)
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
		Item: BatchItem[T]{
			Item:      item.Item,
			Partition: item.Partition,
			Offset:    item.Offset,
		},
	}:
	default:
		logger.Log.Warn("Dropping message from batch channel as its overloaded")
	}
}

// flush on items size exceeds limit
func (b *Batcher[T]) HandleBatchAndFlush(item *BatchItem[T]) error {
	b.items = append(b.items, item)

	if len(b.items) >= b.batchSize {
		if err := b.Flush(); err != nil {
			logger.Log.Error("Error in batcher flush", "error", err)
			return err
		}
	}

	return nil
}

// timer based flush
func (b *Batcher[T]) FlushIfNeeded() error {
	if len(b.items) == 0 {
		logger.Log.Info("Batch periodic flush -- No items present in batcher")
		return nil
	}

	if time.Since(b.lastFlush) >= b.intervalMs {
		logger.Log.Info("Time since last flush >= Interval")
		return b.Flush()
	}

	return nil
}

// delegate the flush fn to callbacks which we will write for tick and book
// batcher opens db agnostic transaction. its not aware about postgres. can change the DB anytime by wiring up new Sink
func (b *Batcher[T]) Flush() error {
	tx, err := b.sink.InitTx(b.ctx)
	if err != nil {
		logger.Log.Error("Error in beginning transaction", "error", err)
		return err
	}
	defer tx.Rollback(b.ctx)

	items := make([]T, 0)
	for _, item := range b.items {
		items = append(items, item.Item)
	}

	if err := b.flushFn(b.ctx, tx, items); err != nil {
		logger.Log.Error("Error in flushing aggregated tick rows", "error", err)
		return err
	}

	if err := tx.Commit(b.ctx); err != nil {
		logger.Log.Error("Error in commit", "error", err)
		return err
	}

	var maxOffsetPerPartitionMap map[int32]int64 = make(map[int32]int64)
	for _, item := range b.items {
		maxOffsetPerPartitionMap[item.Partition] = max(maxOffsetPerPartitionMap[item.Partition], item.Offset)
	}

	// commit offsets upon db write success
	// fire and forget
	b.offsetCommitter.CommitOffsets(b.ctx, maxOffsetPerPartitionMap)

	// clear the batch
	b.items = b.items[:0]

	return nil
}
