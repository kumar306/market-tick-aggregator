package kafkatest

import "sync"

type PauseResumer interface {
	PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(topicPartitions map[string][]int32)
}

type MockKafkaClient struct {
	Paused  bool
	Resumed bool
	Mutex   sync.Mutex
}

func (m *MockKafkaClient) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Paused = true
	return topicPartitions
}

func (m *MockKafkaClient) ResumeFetchPartitions(topicPartitions map[string][]int32) {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	m.Resumed = true
}
