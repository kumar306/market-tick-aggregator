package worker

import (
	"context"
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"
)

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
				metrics.Normalizer_WorkerQueueUsage.WithLabelValues(
					strconv.Itoa(workerID)).Set(
					float64(len(ch)) / float64(cap(ch)))
			}
		}
	}

}
