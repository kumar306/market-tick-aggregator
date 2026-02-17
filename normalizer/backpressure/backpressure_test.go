package backpressure

import (
	"market-normalizer/constants"
	"sync"
	"testing"
)

type mockPauseResumer struct {
	mu           sync.Mutex
	pauseCounts  map[string]map[int32]int
	resumeCounts map[string]map[int32]int
}

func resetBackpressureStateForTest() {
	bpMu.Lock()
	defer bpMu.Unlock()

	bpOnce = sync.Once{}
	workerBPMap = nil
	workerPartitionMap = nil
	topicPartitionHotCount = nil
	highThreshold = 0
	lowThreshold = 0
	bpQueueCapacity = 0
	pauseResumerImpl = nil
}

func newMockPauseResumer() *mockPauseResumer {
	return &mockPauseResumer{
		pauseCounts:  make(map[string]map[int32]int),
		resumeCounts: make(map[string]map[int32]int),
	}
}

func (m *mockPauseResumer) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, partitions := range topicPartitions {
		if _, exists := m.pauseCounts[topic]; !exists {
			m.pauseCounts[topic] = map[int32]int{}
		}
		for _, partition := range partitions {
			m.pauseCounts[topic][partition]++
		}
	}

	return topicPartitions
}

func (m *mockPauseResumer) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for topic, partitions := range topicPartitions {
		if _, exists := m.resumeCounts[topic]; !exists {
			m.resumeCounts[topic] = map[int32]int{}
		}
		for _, partition := range partitions {
			m.resumeCounts[topic][partition]++
		}
	}
}

func (m *mockPauseResumer) pauseCount(topic string, partition int32) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.pauseCounts[topic][partition]
}

func (m *mockPauseResumer) resumeCount(topic string, partition int32) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resumeCounts[topic][partition]
}

func TestBackpressurePausesAndResumesSharedTopicPartitionOnce(t *testing.T) {
	resetBackpressureStateForTest()
	mock := newMockPauseResumer()

	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
	}
	InitBackpressureController(mock, cfg, 10)

	const topic = "coinbase.raw.ticks"
	const partition int32 = 3

	for i := 0; i < 9; i++ {
		OnEnqueue(0, topic, partition)
	}

	if got := mock.pauseCount(topic, partition); got != 1 {
		t.Fatalf("expected topic %s partition %d to pause once when first worker turns hot, got %d", topic, partition, got)
	}

	for i := 0; i < 9; i++ {
		OnEnqueue(1, topic, partition)
	}

	if got := mock.pauseCount(topic, partition); got != 1 {
		t.Fatalf("expected topic %s partition %d pause count to remain 1 while already paused, got %d", topic, partition, got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(0)
	}

	if got := mock.resumeCount(topic, partition); got != 0 {
		t.Fatalf("expected no resume while one worker is still hot, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(1)
	}

	if got := mock.resumeCount(topic, partition); got != 1 {
		t.Fatalf("expected one resume after final worker cools down, got %d", got)
	}
}

func TestBackpressurePausesAndResumesAllKnownWorkerTopicPartitions(t *testing.T) {
	resetBackpressureStateForTest()
	mock := newMockPauseResumer()

	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
	}
	InitBackpressureController(mock, cfg, 10)

	OnEnqueue(2, "coinbase.raw.ticks", 1)
	OnEnqueue(2, "coinbase.raw.level2", 2)
	for i := 0; i < 7; i++ {
		OnEnqueue(2, "coinbase.raw.ticks", 1)
	}

	if got := mock.pauseCount("coinbase.raw.ticks", 1); got != 1 {
		t.Fatalf("expected coinbase.raw.ticks partition 1 to pause once, got %d", got)
	}
	if got := mock.pauseCount("coinbase.raw.level2", 2); got != 1 {
		t.Fatalf("expected coinbase.raw.level2 partition 2 to pause once, got %d", got)
	}

	for i := 0; i < 9; i++ {
		OnDequeue(2)
	}

	if got := mock.resumeCount("coinbase.raw.ticks", 1); got != 1 {
		t.Fatalf("expected coinbase.raw.ticks partition 1 to resume once, got %d", got)
	}
	if got := mock.resumeCount("coinbase.raw.level2", 2); got != 1 {
		t.Fatalf("expected coinbase.raw.level2 partition 2 to resume once, got %d", got)
	}
}
