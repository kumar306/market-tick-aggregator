package batcher

import (
	"context"
	"shared/logger"
	"time"
)

// batching logic is shared across both consumers. only flush fn differs
// better to have 2 instantiations of the batcher rather than batcher interface
type Batcher[T any] struct {
	ctx        context.Context
	items      []T
	intervalMs time.Duration
	lastFlush  time.Time
	batchSize  int
	wasFlushed bool
	flushFn    func(context.Context, Tx, []T) error
	sink       Sink
}

func NewBatcher[T any](
	ctx context.Context,
	batchSize int,
	intervalMs time.Duration,
	flushFn func(context.Context, Tx, []T) error,
	sink Sink) Batcher[T] {
	return Batcher[T]{
		batchSize:  batchSize,
		intervalMs: intervalMs,
		flushFn:    flushFn,
		items:      make([]T, 0, batchSize),
		lastFlush:  time.Now(),
		ctx:        ctx,
		sink:       sink,
	}
}

// flush on items size exceeds limit
func (b *Batcher[T]) Add(item T) error {
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
	defer tx.Rollback(b.ctx)
	if err != nil {
		logger.Log.Error("Error in beginning transaction", "error", err)
		return err
	}

	if err := b.flushFn(b.ctx, tx, b.items); err != nil {
		logger.Log.Error("Error in flushing aggregated tick rows", "error", err)
		return err
	}

	if err := tx.Commit(b.ctx); err != nil {
		logger.Log.Error("Error in commit", "error", err)
		return err
	}
	return nil
}

// using it to check whether we can commit offsets post db commit
func (b *Batcher[T]) WasFlushed() bool {
	wasFlushed := b.wasFlushed
	b.wasFlushed = false
	return wasFlushed
}
