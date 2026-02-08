package batcher

import (
	"context"
	"market-persistence/db"
	"shared/logger"
	"time"

	"github.com/jackc/pgx/v5"
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
	flushFn    func(context.Context, pgx.Tx, []T) error
}

func NewBatcher[T any](
	ctx context.Context,
	batchSize int,
	intervalMs time.Duration,
	flushFn func(context.Context, pgx.Tx, []T) error) Batcher[T] {
	return Batcher[T]{
		batchSize:  batchSize,
		intervalMs: intervalMs,
		flushFn:    flushFn,
		items:      make([]T, 0, batchSize),
		lastFlush:  time.Now(),
		ctx:        ctx,
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
func (b *Batcher[T]) Flush() error {
	if len(b.items) == 0 {
		return nil
	}

	tx, err := db.Pool.BeginTx(b.ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})
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
