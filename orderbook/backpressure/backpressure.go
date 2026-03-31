package backpressure

import (
	"market-orderbook/constants"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
)

// per worker need to store whether its hot and its queue usage.
// on enqueue, will assign partitions
type WorkerBPState struct {
	depth int64
	hot   bool
}

type PauseResumer interface {
	PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32
	ResumeFetchPartitions(topicPartitions map[string][]int32)
}

// worker bp map
var workerBPMap map[int]*WorkerBPState

// mapping of workers to partitions
var workerPartitionMap map[int]map[int32]struct{}

// count of number of times a partition is paused at the present
var partitionHotCount map[int32]int

var highThreshold, lowThreshold int64
var bpQueueCapacity int64
var pauseResumerImpl PauseResumer
var bpTopic string
var bpMu sync.Mutex

var bpOnce sync.Once

func InitBP(cfg *constants.BackpressureConfig, pauseResumer PauseResumer, topic string, queueCapacity int64) {
	bpOnce.Do(func() {
		bpMu.Lock()
		defer bpMu.Unlock()

		highThreshold = int64(cfg.QueueUsageHighThreshold * float64(queueCapacity))
		lowThreshold = int64(cfg.QueueUsageLowThreshold * float64(queueCapacity))
		bpQueueCapacity = queueCapacity
		pauseResumerImpl = pauseResumer
		bpTopic = topic

		workerBPMap = map[int]*WorkerBPState{}
		workerPartitionMap = map[int]map[int32]struct{}{}
		partitionHotCount = map[int32]int{}
	})
}

// takes in workerId, partition - updates the depth by 1.
// updates the partition map if its not present
// if usage > high threshold, mark the worker as hot
// for each of its partitions update map. if partition count was 0 before inc, call pauseFetchPartition(partition)
func OnEnqueue(workerId int, partition int32, offset int64) {
	bpMu.Lock()
	if workerBPMap == nil {
		workerBPMap = map[int]*WorkerBPState{}
	}
	if workerPartitionMap == nil {
		workerPartitionMap = map[int]map[int32]struct{}{}
	}

	if _, ok := workerBPMap[workerId]; !ok {
		workerBPMap[workerId] = &WorkerBPState{}
	}

	if _, ok := workerPartitionMap[workerId]; !ok {
		workerPartitionMap[workerId] = map[int32]struct{}{}
	}

	workerBPMap[workerId].depth++
	workerPartitionMap[workerId][partition] = struct{}{}

	if metrics.Orderbook_BackpressureWorkerQueueUsage != nil && bpQueueCapacity > 0 {
		usage := float64(workerBPMap[workerId].depth) / float64(bpQueueCapacity)
		metrics.Orderbook_BackpressureWorkerQueueUsage.WithLabelValues(strconv.Itoa(workerId)).Set(usage)
	}

	partitionsToPause := make([]int32, 0)
	if workerBPMap[workerId].depth > highThreshold && workerBPMap[workerId].hot == false {
		workerBPMap[workerId].hot = true
		if metrics.Orderbook_BackpressureWorkerPaused != nil {
			metrics.Orderbook_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(workerId)).Set(1)
		}
		if metrics.Orderbook_BackpressureTransitionsTotal != nil {
			metrics.Orderbook_BackpressureTransitionsTotal.Inc()
		}
		for part := range workerPartitionMap[workerId] {
			partitionHotCount[part]++
			if partitionHotCount[part] == 1 {
				partitionsToPause = append(partitionsToPause, part)
			}
		}
	}
	topic := bpTopic
	pauseResumer := pauseResumerImpl
	bpMu.Unlock()

	if pauseResumer == nil {
		return
	}
	for _, part := range partitionsToPause {
		logger.Log.Warn("Pausing fetch for partition", "partition", strconv.Itoa(int(part)))
		pausedMap := constructPauseResumeMap(topic, part)
		pauseResumer.PauseFetchPartitions(pausedMap)
	}

}

// reduce the depth of worker. if gone under low threshold, then set it non hot
func OnDequeue(workerId int, partition int32, offset int64) {
	bpMu.Lock()
	if workerBPMap == nil {
		workerBPMap = map[int]*WorkerBPState{}
	}

	workerState, exists := workerBPMap[workerId]
	if !exists {
		bpMu.Unlock()
		return
	}

	workerState.depth--
	if workerState.depth < 0 {
		workerState.depth = 0
	}
	if metrics.Orderbook_BackpressureWorkerQueueUsage != nil && bpQueueCapacity > 0 {
		usage := float64(workerState.depth) / float64(bpQueueCapacity)
		metrics.Orderbook_BackpressureWorkerQueueUsage.WithLabelValues(strconv.Itoa(workerId)).Set(usage)
	}

	partitionsToResume := make([]int32, 0)
	if workerState.depth < lowThreshold && workerState.hot == true {
		workerState.hot = false
		if metrics.Orderbook_BackpressureWorkerPaused != nil {
			metrics.Orderbook_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(workerId)).Set(0)
		}
		if metrics.Orderbook_BackpressureTransitionsTotal != nil {
			metrics.Orderbook_BackpressureTransitionsTotal.Inc()
		}
		for part := range workerPartitionMap[workerId] {

			partitionHotCount[part]--
			if partitionHotCount[part] <= 0 {
				partitionHotCount[part] = 0
				partitionsToResume = append(partitionsToResume, part)
			}
		}
	}
	topic := bpTopic
	pauseResumer := pauseResumerImpl
	bpMu.Unlock()

	if pauseResumer == nil {
		return
	}
	for _, part := range partitionsToResume {
		logger.Log.Warn("Resuming fetch for partition", "partition", strconv.Itoa(int(part)))
		resumedMap := constructPauseResumeMap(topic, part)
		pauseResumer.ResumeFetchPartitions(resumedMap)
	}
}

func constructPauseResumeMap(topic string, partition int32) map[string][]int32 {
	m := make(map[string][]int32)
	m[topic] = make([]int32, 0)
	m[topic] = append(m[topic], partition)
	return m
}
