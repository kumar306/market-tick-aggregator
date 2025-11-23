package worker

import (
	"context"
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"sync"
	"time"
)

type WorkerQueueUsage struct {
	mu sync.RWMutex
	m  map[int]float64
}

var WorkerQueueUsageHandler *WorkerQueueUsage = &WorkerQueueUsage{
	m: make(map[int]float64),
}

func (w *WorkerQueueUsage) GetQueueUsage(workerId int) float64 {
	w.mu.RLock()
	val := w.m[workerId]
	w.mu.RUnlock()
	return val
}

func (w *WorkerQueueUsage) SetQueueUsage(workerId int, usage float64) {
	w.mu.Lock()
	w.m[workerId] = usage
	w.mu.Unlock()
}

func StartWorkerMetrics(ctx context.Context, channelPool []chan *constants.DispatchRecord) {

	// at ticker, loop through each worker channel and calc metrics
	// this is better than calculating worker metrics with each message. its not sustainable at high throughputs
	// anyway prom scrapes every 5 seconds so we can save on some latency
	ticker := time.NewTicker(3 * time.Second)
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context shutdown. Exiting worker metrics loop..")
			return
		case <-ticker.C:
			for workerID, ch := range channelPool {
				metrics.Normalizer_WorkerQueueSize.WithLabelValues(
					strconv.Itoa(workerID)).Set(
					float64(len(ch)))
				WorkerQueueUsageHandler.SetQueueUsage(workerID, float64(len(ch))/float64(cap(ch)))
				metrics.Normalizer_WorkerQueueUsage.WithLabelValues(
					strconv.Itoa(workerID)).Set(WorkerQueueUsageHandler.GetQueueUsage(workerID))
			}
		}
	}

}
