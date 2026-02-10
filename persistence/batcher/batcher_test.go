package batcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"market-persistence/batcher/util"
	"shared/metrics"
)

var initMetricsOnce sync.Once

func initTestMetrics() {
	initMetricsOnce.Do(func() {
		metrics.InitPersistenceMetrics()
	})
}

// first create mock wrappers over sink, tx, offset committer to track calls
type mockTx struct {
	commitErr      error
	rollbackErr    error
	commitCalls    int
	rollbackCalls  int
	execCallsCount int
}

func (m *mockTx) Exec(context.Context, string, ...any) (int64, error) {
	m.execCallsCount++
	return 1, nil
}

func (m *mockTx) Commit(context.Context) error {
	m.commitCalls++
	return m.commitErr
}

func (m *mockTx) Rollback(context.Context) error {
	m.rollbackCalls++
	return m.rollbackErr
}

type mockSink struct {
	tx  util.Tx
	err error
}

func (m *mockSink) InitTx(context.Context) (util.Tx, error) {
	return m.tx, m.err
}

type mockOffsetCommitter struct {
	calls   int
	offsets map[int32]int64
}

func (m *mockOffsetCommitter) CommitOffsets(_ context.Context, offsets map[int32]int64) error {
	m.calls++
	m.offsets = offsets
	return nil
}

// also in flush fn, just track flush calls and assert it
func TestHandleBatchAndFlushFlushesAtBatchSize(t *testing.T) {
	initTestMetrics()

	ctx := context.Background()
	tx := &mockTx{}
	offsetCommitter := &mockOffsetCommitter{}
	flushCalls := 0

	b := NewBatcher(
		ctx,
		"tickPipeline",
		1,
		time.Second,
		func(_ context.Context, _ util.Tx, rows []int) error {
			flushCalls++
			if len(rows) != 1 || rows[0] != 7 {
				t.Fatalf("unexpected rows: %v", rows)
			}
			return nil
		},
		&mockSink{tx: tx},
		offsetCommitter,
	)

	err := b.HandleBatchAndFlush(&BatchItem[int]{
		Item:      7,
		Partition: 2,
		Offset:    10,
	})
	if err != nil {
		t.Fatalf("HandleBatchAndFlush() error = %v", err)
	}

	if flushCalls != 1 {
		t.Fatalf("flush calls = %d, wanted 1", flushCalls)
	}
	if len(b.items) != 0 {
		t.Fatalf("batch size after flush = %d, wanted 0", len(b.items))
	}
	if tx.commitCalls != 1 {
		t.Fatalf("commit calls = %d, wanted 1", tx.commitCalls)
	}
	if offsetCommitter.calls != 1 {
		t.Fatalf("offset commit calls = %d, wanted 1", offsetCommitter.calls)
	}
	if got := offsetCommitter.offsets[2]; got != 10 {
		t.Fatalf("committed offset = %d, wanted 10", got)
	}
}

func TestFlushIfNeededNoItems(t *testing.T) {
	initTestMetrics()

	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return nil },
		&mockSink{tx: &mockTx{}},
		&mockOffsetCommitter{},
	)

	if err := b.FlushIfNeeded(); err != nil {
		t.Fatalf("FlushIfNeeded() error = %v, wanted nil", err)
	}
}

func TestFlushIfNeededFlushesAfterInterval(t *testing.T) {
	initTestMetrics()

	called := 0
	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		2*time.Second,
		func(context.Context, util.Tx, []int) error {
			called++
			return nil
		},
		&mockSink{tx: &mockTx{}},
		&mockOffsetCommitter{},
	)
	b.items = append(b.items, &BatchItem[int]{Item: 1, Partition: 0, Offset: 1})
	b.lastFlush = time.Now().Add(-5 * time.Second)

	if err := b.FlushIfNeeded(); err != nil {
		t.Fatalf("FlushIfNeeded() error = %v", err)
	}
	if called != 1 {
		t.Fatalf("flush called = %d, want 1", called)
	}
}

func TestFlushInitTxError(t *testing.T) {
	initTestMetrics()

	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return nil },
		&mockSink{err: errors.New("init tx failed")},
		&mockOffsetCommitter{},
	)
	b.items = append(b.items, &BatchItem[int]{Item: 1})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
}

func TestFlushFlushFnError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{}
	offsetCommitter := &mockOffsetCommitter{}
	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return errors.New("flush fn failed") },
		&mockSink{tx: tx},
		offsetCommitter,
	)
	b.items = append(b.items, &BatchItem[int]{Item: 1, Partition: 0, Offset: 1})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
	if tx.commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", tx.commitCalls)
	}
	if offsetCommitter.calls != 0 {
		t.Fatalf("offset commit calls = %d, want 0", offsetCommitter.calls)
	}
}

func TestFlushCommitError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{commitErr: errors.New("commit failed")}
	offsetCommitter := &mockOffsetCommitter{}
	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return nil },
		&mockSink{tx: tx},
		offsetCommitter,
	)
	b.items = append(b.items, &BatchItem[int]{Item: 1, Partition: 0, Offset: 1})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
	if offsetCommitter.calls != 0 {
		t.Fatalf("offset commit calls = %d, want 0", offsetCommitter.calls)
	}
}
