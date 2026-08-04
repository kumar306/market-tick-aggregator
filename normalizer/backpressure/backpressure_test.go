package backpressure

import (
	"market-normalizer/constants"
	"testing"
)

type mockPauseResumer struct {
	pauseCounts  map[string]map[int32]int
	resumeCounts map[string]map[int32]int
}

func newMockPauseResumer() *mockPauseResumer {
	return &mockPauseResumer{
		pauseCounts:  make(map[string]map[int32]int),
		resumeCounts: make(map[string]map[int32]int),
	}
}

func (m *mockPauseResumer) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
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
	for topic, partitions := range topicPartitions {
		if _, exists := m.resumeCounts[topic]; !exists {
			m.resumeCounts[topic] = map[int32]int{}
		}
		for _, partition := range partitions {
			m.resumeCounts[topic][partition]++
		}
	}
}

func TestControllerPausesAndResumesOnThreshold(t *testing.T) {
	mock := newMockPauseResumer()
	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
	}
	c := NewController(0, mock, cfg, 10)

	const topic = "coinbase.raw.ticks"
	const partition int32 = 3

	for i := 0; i < 9; i++ {
		c.OnEnqueue(topic, partition)
	}
	if got := mock.pauseCounts[topic][partition]; got != 1 {
		t.Fatalf("expected partition to pause once crossing high threshold, got %d", got)
	}

	// still above high threshold -- must not pause again
	c.OnEnqueue(topic, partition)
	if got := mock.pauseCounts[topic][partition]; got != 1 {
		t.Fatalf("expected pause count to remain 1 while already paused, got %d", got)
	}

	for i := 0; i < 9; i++ {
		c.OnDequeue()
	}
	if got := mock.resumeCounts[topic][partition]; got != 1 {
		t.Fatalf("expected one resume after dropping below low threshold, got %d", got)
	}
}

func TestControllerTracksMultiplePartitions(t *testing.T) {
	mock := newMockPauseResumer()
	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
	}
	c := NewController(2, mock, cfg, 10)

	c.OnEnqueue("coinbase.raw.ticks", 1)
	c.OnEnqueue("coinbase.raw.level2", 2)
	for i := 0; i < 7; i++ {
		c.OnEnqueue("coinbase.raw.ticks", 1)
	}

	if got := mock.pauseCounts["coinbase.raw.ticks"][1]; got != 1 {
		t.Fatalf("expected coinbase.raw.ticks partition 1 to pause once, got %d", got)
	}
	if got := mock.pauseCounts["coinbase.raw.level2"][2]; got != 1 {
		t.Fatalf("expected coinbase.raw.level2 partition 2 to pause once (paused alongside the hot partition), got %d", got)
	}

	for i := 0; i < 9; i++ {
		c.OnDequeue()
	}

	if got := mock.resumeCounts["coinbase.raw.ticks"][1]; got != 1 {
		t.Fatalf("expected coinbase.raw.ticks partition 1 to resume once, got %d", got)
	}
	if got := mock.resumeCounts["coinbase.raw.level2"][2]; got != 1 {
		t.Fatalf("expected coinbase.raw.level2 partition 2 to resume once, got %d", got)
	}
}
