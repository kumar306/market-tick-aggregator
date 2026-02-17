package backpressure

import (
	"market-normalizer/constants"
	"market-normalizer/utils/kafkatest"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
)

// just keep track of worker, depth of messages in his channel.
// if its less than threshold and hot, then resume
type WorkerBPState struct {
	Depth  int64
	Paused bool
}

// worker bp map
var workerBPMap map[int]*WorkerBPState

// mapping of workers to topic/partitions
var workerPartitionMap map[int]map[string]map[int32]struct{}

// count of number of times a topic partition is paused at the present
var topicPartitionHotCount map[string]map[int32]int

var highThreshold, lowThreshold int64
var bpQueueCapacity int64
var pauseResumerImpl kafkatest.PauseResumer
var bpMu sync.Mutex
var bpOnce sync.Once

// track the metrics for worker queue usage and do the pause fetches here to slow down rate of consuming and let the system catch up
// pause/resume as per metrics
func InitBackpressureController(client kafkatest.PauseResumer,
	backpressureConfig *constants.BackpressureConfig,
	queueCapacity int64) {
	bpOnce.Do(func() {
		bpMu.Lock()
		defer bpMu.Unlock()
		highThreshold = int64(backpressureConfig.QueueUsageHighThreshold * float64(queueCapacity))
		lowThreshold = int64(backpressureConfig.QueueUsageLowThreshold * float64(queueCapacity))
		bpQueueCapacity = queueCapacity
		pauseResumerImpl = client
		workerBPMap = map[int]*WorkerBPState{}
		workerPartitionMap = map[int]map[string]map[int32]struct{}{}
		topicPartitionHotCount = map[string]map[int32]int{}
	})
}

func OnEnqueue(workerId int, topic string, partition int32) {
	bpMu.Lock()
	if workerBPMap == nil {
		workerBPMap = map[int]*WorkerBPState{}
	}

	if workerPartitionMap == nil {
		workerPartitionMap = map[int]map[string]map[int32]struct{}{}
	}

	if topicPartitionHotCount == nil {
		topicPartitionHotCount = map[string]map[int32]int{}
	}

	if _, exists := workerBPMap[workerId]; !exists {
		workerBPMap[workerId] = &WorkerBPState{}
	}

	if _, exists := workerPartitionMap[workerId]; !exists {
		workerPartitionMap[workerId] = map[string]map[int32]struct{}{}
	}

	if _, exists := workerPartitionMap[workerId][topic]; !exists {
		workerPartitionMap[workerId][topic] = map[int32]struct{}{}
	}

	if _, exists := topicPartitionHotCount[topic]; !exists {
		topicPartitionHotCount[topic] = map[int32]int{}
	}

	workerBPMap[workerId].Depth++
	workerPartitionMap[workerId][topic][partition] = struct{}{}

	if metrics.Normalizer_WorkerQueueUsage != nil && bpQueueCapacity > 0 {
		usage := float64(workerBPMap[workerId].Depth) / float64(bpQueueCapacity)
		metrics.Normalizer_WorkerQueueUsage.WithLabelValues(strconv.Itoa(workerId)).Set(usage)
	}

	topicPartitionsToPause := make(map[string][]int32, 0)
	if workerBPMap[workerId].Depth > highThreshold && workerBPMap[workerId].Paused == false {
		workerBPMap[workerId].Paused = true
		if metrics.Normalizer_BackpressureWorkerPaused != nil {
			metrics.Normalizer_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(workerId)).Set(1)
		}
		if metrics.Normalizer_BackpressureTransitionsTotal != nil {
			metrics.Normalizer_BackpressureTransitionsTotal.Inc()
		}
		for topic, topicPartitions := range workerPartitionMap[workerId] {
			for part := range topicPartitions {
				topicPartitionHotCount[topic][part]++
				if topicPartitionHotCount[topic][part] == 1 {
					if _, exists := topicPartitionsToPause[topic]; !exists {
						topicPartitionsToPause[topic] = make([]int32, 0)
					}
					topicPartitionsToPause[topic] = append(topicPartitionsToPause[topic], part)
				}
			}
		}
	}
	pauseResumer := pauseResumerImpl
	bpMu.Unlock()

	// construct the map to call pause fetch
	if pauseResumer == nil {
		return
	}

	for topic, parts := range topicPartitionsToPause {
		for _, part := range parts {
			pausedMap := constructPartitionMap(topic, part)
			pauseResumer.PauseFetchPartitions(pausedMap)
			logger.Log.Info("Paused fetch partition for topic, partition", "topic", topic, "partition", part)
		}
	}

}

func OnDequeue(workerId int) {
	bpMu.Lock()

	if workerBPMap == nil {
		workerBPMap = map[int]*WorkerBPState{}
	}

	workerState, exists := workerBPMap[workerId]
	if !exists {
		bpMu.Unlock()
		return
	}

	// decrement
	workerState.Depth--
	if workerState.Depth < 0 {
		workerState.Depth = 0
	}

	if metrics.Normalizer_WorkerQueueUsage != nil && bpQueueCapacity > 0 {
		usage := float64(workerState.Depth) / float64(bpQueueCapacity)
		metrics.Normalizer_WorkerQueueUsage.WithLabelValues(strconv.Itoa(workerId)).Set(usage)
	}

	if workerState.Depth < lowThreshold && workerState.Paused == true {
		workerState.Paused = false
		if metrics.Normalizer_BackpressureWorkerPaused != nil {
			metrics.Normalizer_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(workerId)).Set(0)
		}
		if metrics.Normalizer_BackpressureTransitionsTotal != nil {
			metrics.Normalizer_BackpressureTransitionsTotal.Inc()
		}

		topicPartitionsToResume := make(map[string][]int32, 0)

		for topic, partitions := range workerPartitionMap[workerId] {
			if _, exists := topicPartitionHotCount[topic]; !exists {
				topicPartitionHotCount[topic] = map[int32]int{}
			}
			for part := range partitions {
				topicPartitionHotCount[topic][part]--
				if topicPartitionHotCount[topic][part] <= 0 {
					topicPartitionHotCount[topic][part] = 0
					if _, exists := topicPartitionsToResume[topic]; !exists {
						topicPartitionsToResume[topic] = make([]int32, 0)
					}
					topicPartitionsToResume[topic] = append(topicPartitionsToResume[topic], part)
				}
			}
		}

		pauseResumer := pauseResumerImpl
		bpMu.Unlock()

		if pauseResumer == nil {
			logger.Log.Info("Pause resumer is nil on dequeue. Returning")
			return
		}

		for topic, partitions := range topicPartitionsToResume {
			for _, part := range partitions {
				resumedMap := constructPartitionMap(topic, part)
				pauseResumer.ResumeFetchPartitions(resumedMap)
				logger.Log.Info("Resumed fetch partition for topic, partition", "topic", topic, "partition", part)
			}
		}
		return
	}

	bpMu.Unlock()
}

func constructPartitionMap(topic string, partition int32) map[string][]int32 {
	pauseResumePartitionMap := make(map[string][]int32)
	pauseResumePartitionMap[topic] = make([]int32, 0)
	pauseResumePartitionMap[topic] = append(pauseResumePartitionMap[topic], partition)
	return pauseResumePartitionMap
}
