package batcher

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"market-persistence/batcher/util"
	"market-persistence/config"
	"shared/metrics"

	"github.com/twmb/franz-go/pkg/kgo"
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

type mockEventProcessor struct {
	calls             int
	processedOffsets  map[int32]int64
	dlqMessages       []*config.DLQMessage
	handleEventErrors []error
}

func (m *mockEventProcessor) MarkUpstreamProcessed(_ context.Context, offsets map[int32]int64) error {
	m.calls++
	m.processedOffsets = offsets
	return nil
}

func (m *mockEventProcessor) HandleEventError(_ context.Context, message *config.DLQMessage) error {
	m.dlqMessages = append(m.dlqMessages, message)
	return nil
}

// also in flush fn, just track flush calls and assert it
func TestHandleBatchAndFlushFlushesAtBatchSize(t *testing.T) {
	initTestMetrics()

	ctx := context.Background()
	tx := &mockTx{}
	eventProcessor := &mockEventProcessor{}
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
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{tx: tx},
		eventProcessor,
	)

	err := b.HandleBatchAndFlush(&BatchItem[int]{
		Item: 7,
		Record: &kgo.Record{
			Partition: 2,
			Offset:    10,
		},
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
	if eventProcessor.calls != 1 {
		t.Fatalf("event processor calls = %d, wanted 1", eventProcessor.calls)
	}
	if got := eventProcessor.processedOffsets[2]; got != 10 {
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
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{tx: &mockTx{}},
		&mockEventProcessor{},
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
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{tx: &mockTx{}},
		&mockEventProcessor{},
	)
	b.items = append(b.items, &BatchItem[int]{
		Item: 1,
		Record: &kgo.Record{
			Partition: 0,
			Offset:    1,
		},
	})
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
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{err: errors.New("init tx failed")},
		&mockEventProcessor{},
	)
	b.items = append(b.items, &BatchItem[int]{
		Item:   1,
		Record: &kgo.Record{},
	})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
}

func TestFlushFlushFnError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{}
	eventProcessor := &mockEventProcessor{}
	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return errors.New("flush fn failed") },
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{tx: tx},
		eventProcessor,
	)
	b.items = append(b.items, &BatchItem[int]{
		Item: 1,
		Record: &kgo.Record{
			Partition: 0,
			Offset:    1,
		},
	})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
	if tx.commitCalls != 0 {
		t.Fatalf("commit calls = %d, want 0", tx.commitCalls)
	}
	if eventProcessor.calls != 0 {
		t.Fatalf("event processor calls = %d, want 0", eventProcessor.calls)
	}
}

func TestFlushCommitError(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{commitErr: errors.New("commit failed")}
	eventProcessor := &mockEventProcessor{}
	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error { return nil },
		func(item int) error { return nil }, // invalidCheckFn
		&mockSink{tx: tx},
		eventProcessor,
	)
	b.items = append(b.items, &BatchItem[int]{
		Item: 1,
		Record: &kgo.Record{
			Partition: 0,
			Offset:    1,
		},
	})

	if err := b.Flush(); err == nil {
		t.Fatalf("Flush() error = nil, want non-nil")
	}
	if eventProcessor.calls != 0 {
		t.Fatalf("event processor calls = %d, want 0", eventProcessor.calls)
	}
}

func TestFlushWithInvalidRecord(t *testing.T) {
	initTestMetrics()

	tx := &mockTx{}
	eventProcessor := &mockEventProcessor{}
	flushCalls := 0

	b := NewBatcher(
		context.Background(),
		"tickPipeline",
		10,
		time.Second,
		func(context.Context, util.Tx, []int) error {
			flushCalls++
			return nil
		},
		func(item int) error {
			if item == 999 {
				return errors.New("invalid item value")
			}
			return nil
		}, // invalidCheckFn rejects 999
		&mockSink{tx: tx},
		eventProcessor,
	)

	// Add one invalid and one valid item
	b.items = append(b.items, &BatchItem[int]{
		Item: 999,
		Record: &kgo.Record{
			Partition: 0,
			Offset:    1,
		},
	})
	b.items = append(b.items, &BatchItem[int]{
		Item: 5,
		Record: &kgo.Record{
			Partition: 0,
			Offset:    2,
		},
	})

	if err := b.Flush(); err != nil {
		t.Fatalf("Flush() error = %v", err)
	}

	// Should have sent invalid item to DLQ
	if len(eventProcessor.dlqMessages) != 1 {
		t.Fatalf("dlq messages = %d, want 1", len(eventProcessor.dlqMessages))
	}

	dlqMsg := eventProcessor.dlqMessages[0]
	if dlqMsg.ErrorMsg != "invalid item value" {
		t.Fatalf("dlq error msg = %s, want 'invalid item value'", dlqMsg.ErrorMsg)
	}
	if dlqMsg.Topic != "tickPipeline" {
		t.Fatalf("dlq topic = %s, want 'tickPipeline'", dlqMsg.Topic)
	}

	// Should have flushed only the valid item
	if flushCalls != 1 {
		t.Fatalf("flush calls = %d, want 1", flushCalls)
	}

	// Should have committed offsets for both items
	if eventProcessor.calls != 1 {
		t.Fatalf("event processor calls = %d, want 1", eventProcessor.calls)
	}
	if got := eventProcessor.processedOffsets[0]; got != 2 {
		t.Fatalf("committed offset = %d, want 2", got)
	}
}
