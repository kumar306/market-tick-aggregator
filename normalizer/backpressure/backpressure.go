package backpressure

import (
	"market-normalizer/constants"
	"market-normalizer/utils/kafkatest"
	"shared/metrics"
	"strconv"
)

// make controller per worker, rather than per instance.. now that each worker owns disjoint partition set as per kgo GroupTransactSession.
// tracks one worker's own queue depth and observed (topic, partition) set, pausing/resuming fetches on that worker's own session client when it crosses thresholds
type Controller struct {
	workerId      int
	depth         int64
	paused        bool
	highThreshold int64
	lowThreshold  int64
	capacity      int64
	partitions    map[string]map[int32]struct{}
	pauseResumer  kafkatest.PauseResumer
}

func NewController(workerId int, pauseResumer kafkatest.PauseResumer, cfg *constants.BackpressureConfig, queueCapacity int64) *Controller {
	return &Controller{
		workerId:      workerId,
		highThreshold: int64(cfg.QueueUsageHighThreshold * float64(queueCapacity)),
		lowThreshold:  int64(cfg.QueueUsageLowThreshold * float64(queueCapacity)),
		capacity:      queueCapacity,
		partitions:    map[string]map[int32]struct{}{},
		pauseResumer:  pauseResumer,
	}
}

func (c *Controller) OnEnqueue(topic string, partition int32) {
	if _, exists := c.partitions[topic]; !exists {
		c.partitions[topic] = map[int32]struct{}{}
	}
	c.partitions[topic][partition] = struct{}{}
	c.depth++

	if metrics.Normalizer_WorkerQueueUsage != nil && c.capacity > 0 {
		usage := float64(c.depth) / float64(c.capacity)
		metrics.Normalizer_WorkerQueueUsage.WithLabelValues(strconv.Itoa(c.workerId)).Set(usage)
	}

	if c.depth > c.highThreshold && !c.paused {
		c.paused = true
		if metrics.Normalizer_BackpressureWorkerPaused != nil {
			metrics.Normalizer_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(c.workerId)).Set(1)
		}
		if metrics.Normalizer_BackpressureTransitionsTotal != nil {
			metrics.Normalizer_BackpressureTransitionsTotal.Inc()
		}
		if c.pauseResumer != nil {
			c.pauseResumer.PauseFetchPartitions(c.snapshotPartitions())
		}
	}
}

func (c *Controller) OnDequeue() {
	c.depth--
	if c.depth < 0 {
		c.depth = 0
	}

	if metrics.Normalizer_WorkerQueueUsage != nil && c.capacity > 0 {
		usage := float64(c.depth) / float64(c.capacity)
		metrics.Normalizer_WorkerQueueUsage.WithLabelValues(strconv.Itoa(c.workerId)).Set(usage)
	}

	if c.depth < c.lowThreshold && c.paused {
		c.paused = false
		if metrics.Normalizer_BackpressureWorkerPaused != nil {
			metrics.Normalizer_BackpressureWorkerPaused.WithLabelValues(strconv.Itoa(c.workerId)).Set(0)
		}
		if metrics.Normalizer_BackpressureTransitionsTotal != nil {
			metrics.Normalizer_BackpressureTransitionsTotal.Inc()
		}
		if c.pauseResumer != nil {
			c.pauseResumer.ResumeFetchPartitions(c.snapshotPartitions())
		}
	}
}

func (c *Controller) snapshotPartitions() map[string][]int32 {
	out := make(map[string][]int32, len(c.partitions))
	for topic, parts := range c.partitions {
		list := make([]int32, 0, len(parts))
		for p := range parts {
			list = append(list, p)
		}
		out[topic] = list
	}
	return out
}
