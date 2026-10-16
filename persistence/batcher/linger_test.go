package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"market-persistence/batcher/util"

	"github.com/twmb/franz-go/pkg/kgo"
)

// records when each flush happened and what it held
type flushLog struct {
	mu    sync.Mutex
	times []time.Time
	rows  [][]int
}

func (f *flushLog) fn(_ context.Context, _ util.Tx, rows []int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.times = append(f.times, time.Now())
	f.rows = append(f.rows, append([]int(nil), rows...))
	return nil
}

func (f *flushLog) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.times)
}

func runBatcher(t *testing.T, batchSize int, maxWait time.Duration, flush func(context.Context, util.Tx, []int) error) (*Batcher[int], context.CancelFunc) {
	initTestMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	b := NewBatcher(ctx, "linger_test", batchSize, maxWait, flush, func(int) error { return nil }, &mockSink{tx: &mockTx{}}, &mockEventProcessor{})
	go b.Run()
	return &b, cancel
}

func add(b *Batcher[int], v int) {
	b.Add(BatchItem[int]{Item: v, Record: &kgo.Record{Partition: 0, Offset: int64(v)}})
}

// The wait is counted from the FIRST row of the batch. Rows that arrive later do not push the flush out, and the
// flush does not come early either.
func TestLinger_FlushesMaxWaitAfterTheFirstRow_NotAfterTheLast(t *testing.T) {
	const maxWait = 60 * time.Millisecond
	log := &flushLog{}
	b, cancel := runBatcher(t, 100, maxWait, log.fn)
	defer cancel()

	first := time.Now()
	add(b, 1)
	time.Sleep(20 * time.Millisecond)
	add(b, 2)
	time.Sleep(20 * time.Millisecond)
	add(b, 3)

	time.Sleep(maxWait + 80*time.Millisecond)
	if log.count() != 1 {
		t.Fatalf("expected one flush holding all three rows, got %d flushes", log.count())
	}
	if len(log.rows[0]) != 3 {
		t.Fatalf("the flush should hold rows 1..3, got %v", log.rows[0])
	}
	waited := log.times[0].Sub(first)
	if waited < maxWait-5*time.Millisecond || waited > maxWait+40*time.Millisecond {
		t.Fatalf("flushed %v after the first row, want about %v (the deadline is set by the FIRST row)", waited, maxWait)
	}
}

// The regression this redesign exists for: with a ticker and a "full interval since the last flush" check, a row's
// wait was anything from 0 to 2x the interval. Here, whatever the arrival time, no row waits past maxWait + slack.
func TestLinger_NoRowWaitsLongerThanMaxWait(t *testing.T) {
	const maxWait = 40 * time.Millisecond
	log := &flushLog{}
	b, cancel := runBatcher(t, 1000, maxWait, log.fn)
	defer cancel()

	sentAt := map[int]time.Time{}
	for i := 1; i <= 60; i++ {
		sentAt[i] = time.Now()
		add(b, i)
		time.Sleep(7 * time.Millisecond) // deliberately not a divisor of maxWait: every phase gets covered
	}
	time.Sleep(maxWait + 100*time.Millisecond)

	log.mu.Lock()
	defer log.mu.Unlock()
	seen := 0
	var worst time.Duration
	for fi, rows := range log.rows {
		for _, r := range rows {
			seen++
			if w := log.times[fi].Sub(sentAt[r]); w > worst {
				worst = w
			}
		}
	}
	if seen != 60 {
		t.Fatalf("expected all 60 rows flushed exactly once, saw %d", seen)
	}
	if worst > maxWait+40*time.Millisecond {
		t.Fatalf("worst wait %v exceeds maxWait %v + slack: the old 'up to 2x' behaviour is back", worst, maxWait)
	}
}

func TestLinger_ReachingBatchSizeFlushesImmediately(t *testing.T) {
	log := &flushLog{}
	b, cancel := runBatcher(t, 3, 5*time.Second, log.fn) // the timer would be far too slow to explain this flush
	defer cancel()

	start := time.Now()
	add(b, 1)
	add(b, 2)
	add(b, 3)
	deadline := time.Now().Add(500 * time.Millisecond)
	for log.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	if log.count() != 1 || time.Since(start) > 400*time.Millisecond {
		t.Fatalf("a full batch must flush at once; flushes=%d after %v", log.count(), time.Since(start))
	}
}

func TestLinger_NextRowAfterAFlushStartsAFreshWait(t *testing.T) {
	const maxWait = 40 * time.Millisecond
	log := &flushLog{}
	b, cancel := runBatcher(t, 100, maxWait, log.fn)
	defer cancel()

	add(b, 1)
	time.Sleep(maxWait + 60*time.Millisecond) // flushed
	second := time.Now()
	add(b, 2)
	time.Sleep(maxWait + 60*time.Millisecond)

	if log.count() != 2 {
		t.Fatalf("expected two separate flushes, got %d", log.count())
	}
	if w := log.times[1].Sub(second); w < maxWait-5*time.Millisecond || w > maxWait+40*time.Millisecond {
		t.Fatalf("second batch flushed %v after its first row, want about %v", w, maxWait)
	}
}

// a failed flush must not strand the batch until some unrelated row shows up
func TestLinger_FailedFlushIsRetriedAfterAnotherMaxWait(t *testing.T) {
	const maxWait = 30 * time.Millisecond
	var mu sync.Mutex
	calls := 0
	b, cancel := runBatcher(t, 100, maxWait, func(_ context.Context, _ util.Tx, rows []int) error {
		mu.Lock()
		defer mu.Unlock()
		calls++
		if calls == 1 {
			return context.DeadlineExceeded
		}
		return nil
	})
	defer cancel()

	add(b, 1)
	time.Sleep(4 * maxWait)

	mu.Lock()
	defer mu.Unlock()
	if calls < 2 {
		t.Fatalf("the failed batch was never retried (flush calls = %d)", calls)
	}
}

// Rows that all failed validation go to the DLQ once and are cleared. They used to stay in the batch and be sent to
// the DLQ again on every later flush, which with a short wait would be a flood.
func TestFlush_AllInvalidBatchGoesToDLQOnceAndIsCleared(t *testing.T) {
	initTestMetrics()
	ep := &mockEventProcessor{}
	b := NewBatcher(context.Background(), "tickPipeline", 10, 10*time.Millisecond,
		func(context.Context, util.Tx, []int) error { return nil },
		func(int) error { return context.Canceled }, // everything is invalid
		&mockSink{tx: &mockTx{}}, ep)
	b.items = append(b.items,
		&BatchItem[int]{Item: 1, Record: &kgo.Record{Partition: 0, Offset: 7}},
		&BatchItem[int]{Item: 2, Record: &kgo.Record{Partition: 0, Offset: 8}})

	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(ep.dlqMessages) != 2 {
		t.Fatalf("expected 2 DLQ messages, got %d", len(ep.dlqMessages))
	}
	if len(b.items) != 0 {
		t.Fatalf("handled rows must be cleared, %d left", len(b.items))
	}
	if ep.processedOffsets[0] != 8 {
		t.Fatalf("offsets must move past the DLQ'd rows, got %v", ep.processedOffsets)
	}

	if err := b.Flush(); err != nil { // a later flush must not resend them
		t.Fatal(err)
	}
	if len(ep.dlqMessages) != 2 {
		t.Fatalf("DLQ'd again on a later flush: %d messages", len(ep.dlqMessages))
	}
}
