package backpressure

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/dispatcher"
	"market-normalizer/worker"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type WorkerTime struct {
	ThresholdStartTime *time.Time
	PauseActive        bool
	CooldownActive     *atomic.Bool
}

// track the metrics for worker queue usage and do the pause fetches here to slow down rate of consuming and let the system catch up
// pause/resume as per metrics
func BackpressureController(ctx context.Context,
	client *kgo.Client,
	channelPool []chan *constants.DispatchRecord,
	backpressureConfig *constants.BackpressureConfig) {
	ticker := time.NewTicker(1 * time.Second)

	// per worker map to deadlines to pause his partitions
	var workerTimerMap = make(map[int]*WorkerTime)

	for workerId := range channelPool {
		c := &atomic.Bool{}
		c.Store(false)
		workerTimerMap[workerId] = &WorkerTime{
			CooldownActive: c,
		}
	}

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event.. shutting down backpressure controller")
			return

		case <-ticker.C:
			// iterate through worker channels to get the worker

			for workerId := range channelPool {

				// if worker is not assigned a partition during temporary partition rebalance, this can occur so check it
				parts := dispatcher.WorkerPartitionAssignmentsHandler.GetPartitionAssignments(workerId)
				if len(parts) == 0 {
					logger.Log.Info("No partitions present for the worker", "workerId", workerId)
					continue
				}

				if workerTimerMap[workerId].CooldownActive.Load() {
					continue
				}

				val := worker.WorkerQueueUsageHandler.GetQueueUsage(workerId)

				// if val is above high configurable threshold set in config
				// and val is high for >= x seconds - also configurable
				// then pause partitions fetch

				// identify all the topics and partitions assigned to a worker and store in memory
				if val >= backpressureConfig.QueueUsageHighThreshold {

					if workerTimerMap[workerId].PauseActive {
						continue
					}

					if workerTimerMap[workerId].ThresholdStartTime == nil {
						curTime := time.Now()
						workerTimerMap[workerId].ThresholdStartTime = &curTime
					}

					if time.Since(*(workerTimerMap[workerId].ThresholdStartTime)) >
						(time.Duration(backpressureConfig.ThresholdActiveMillis) * time.Millisecond) {

						// pause those partitions to be fetched for a cooldown period till queue usage reduces
						client.PauseFetchPartitions(
							constructPartitionMap(dispatcher.WorkerPartitionAssignmentsHandler.GetPartitionAssignments(workerId)))

						metrics.Normalizer_PausedPartitions.WithLabelValues(strconv.Itoa(workerId)).Set(1.0)
						logger.Log.Info("Pausing fetch partitions for worker", "worker", workerId)

						workerTimerMap[workerId].PauseActive = true
						workerTimerMap[workerId].CooldownActive.Store(true)

						time.AfterFunc(time.Duration(backpressureConfig.CooldownTimeMillis)*time.Millisecond, func() {
							workerTimerMap[workerId].CooldownActive.Store(false)
						})
					}

				} else {

					if workerTimerMap[workerId].ThresholdStartTime != nil {
						workerTimerMap[workerId].ThresholdStartTime = nil
					}

					if val <= backpressureConfig.QueueUsageLowThreshold && workerTimerMap[workerId].PauseActive {
						// resume partition fetch if queue usage is under low threshold and worker is in pause mode
						client.ResumeFetchPartitions(
							constructPartitionMap(dispatcher.WorkerPartitionAssignmentsHandler.GetPartitionAssignments(workerId)))

						metrics.Normalizer_PausedPartitions.WithLabelValues(strconv.Itoa(workerId)).Set(0.0)
						logger.Log.Info("Resuming fetch partitions for worker", "worker", workerId)

						workerTimerMap[workerId].PauseActive = false
					}
				}
			}
		}
	}
}

func constructPartitionMap(workerPartitionMap map[string]map[int32]bool) map[string][]int32 {
	pausedPartitionMap := make(map[string][]int32)
	for topic := range workerPartitionMap {
		for partition := range workerPartitionMap[topic] {
			pausedPartitionMap[topic] = append(pausedPartitionMap[topic], int32(partition))
		}
	}
	return pausedPartitionMap
}
