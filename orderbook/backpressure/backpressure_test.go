package backpressure

import (
	"market-orderbook/constants"
	"sync"
	"testing"
)

type mockPauseResumer struct {
	mu           sync.Mutex
	pauseCounts  map[int32]int
	resumeCounts map[int32]int
}

func resetBackpressureStateForTest() {
	bpMu.Lock()
	defer bpMu.Unlock()

	bpOnce = sync.Once{}
	workerBPMap = nil
	workerPartitionMap = nil
	partitionHotCount = nil
	highThreshold = 0
	lowThreshold = 0
	bpQueueCapacity = 0
	pauseResumerImpl = nil
	bpTopic = ""
}

// create fake wrapper over the pause resumer for testing
func newMockPauseResumer() *mockPauseResumer {
	return &mockPauseResumer{
		pauseCounts:  make(map[int32]int),
		resumeCounts: make(map[int32]int),
	}
}

func (m *mockPauseResumer) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partitions := range topicPartitions {
		for _, partition := range partitions {
			m.pauseCounts[partition]++
		}
	}
	return topicPartitions
}

func (m *mockPauseResumer) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, partitions := range topicPartitions {
		for _, partition := range partitions {
			m.resumeCounts[partition]++
		}
	}
}

func (m *mockPauseResumer) pauseCount(partition int32) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pauseCounts[partition]
}

func (m *mockPauseResumer) resumeCount(partition int32) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resumeCounts[partition]
}

func TestBackpressurePausesAndResumesSharedPartitionOnce(t *testing.T) {
	resetBackpressureStateForTest()
	mock := newMockPauseResumer()

	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
		ConfirmSeconds:          1,
		PollIntervalMs:          100,
	}
	InitBP(cfg, mock, "orderbook.upstream", 10)

	for i := 0; i < 9; i++ {
		OnEnqueue(0, 0, 0)
	}
	if got := mock.pauseCount(0); got != 1 {
		t.Fatalf("expected partition 0 to be paused once when first worker turns hot, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnEnqueue(1, 0, 0)
	}
	if got := mock.pauseCount(0); got != 1 {
		t.Fatalf("expected partition 0 pause count to remain 1 while already paused, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(0, 0, 0)
	}
	if got := mock.resumeCount(0); got != 0 {
		t.Fatalf("expected no resume after only one worker cools down, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(1, 0, 0)
	}
	if got := mock.resumeCount(0); got != 1 {
		t.Fatalf("expected one resume when final hot worker cools down, got %d", got)
	}
}

func TestBackpressurePausesAllKnownWorkerPartitions(t *testing.T) {
	resetBackpressureStateForTest()
	mock := newMockPauseResumer()
	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
		ConfirmSeconds:          1,
		PollIntervalMs:          100,
	}
	InitBP(cfg, mock, "orderbook.upstream", 10)

	OnEnqueue(2, 1, 0)
	OnEnqueue(2, 2, 0)
	for i := 0; i < 7; i++ {
		OnEnqueue(2, 1, 0)
	}

	if got := mock.pauseCount(1); got != 1 {
		t.Fatalf("expected partition 1 to be paused once, got %d", got)
	}
	if got := mock.pauseCount(2); got != 1 {
		t.Fatalf("expected partition 2 to be paused once, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(2, 0, 0)
	}

	if got := mock.resumeCount(1); got != 1 {
		t.Fatalf("expected partition 1 to be resumed once, got %d", got)
	}
	if got := mock.resumeCount(2); got != 1 {
		t.Fatalf("expected partition 2 to be resumed once, got %d", got)
	}
}
