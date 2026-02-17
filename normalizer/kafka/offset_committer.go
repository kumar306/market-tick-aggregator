package kafka

import (
	"context"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// start a ticker which emits ticker event every X milliseconds.
// upon every event, commit offsets to kafka
func OffsetCommitter(ctx context.Context, gapMillis int, topics []string) {
	var ticker *time.Ticker = time.NewTicker(time.Duration(gapMillis) * time.Millisecond)
	defer ticker.Stop()

	const commitTimeout = 5 * time.Second
	const listOffsetsTimeout = 3 * time.Second

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context done.. shutting down kafka commit loop")
			finalCommitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
			if err := Client.CommitMarkedOffsets(finalCommitCtx); err != nil {
				logger.Log.Error("Final commit failed", "error", err)
			}
			cancel()
			return
		case <-ticker.C:
			start := time.Now()
			commitCtx, cancel := context.WithTimeout(context.Background(), commitTimeout)
			err := Client.CommitMarkedOffsets(commitCtx)
			cancel()
			if err != nil {
				logger.Log.Error("CommitMarkedOffsets failed", "error", err)
				metrics.Normalizer_CommitOffsetErrorsTotal.Inc()
			} else {
				logger.Log.Info("CommitMarkedOffsets ok", "latencyMillis", time.Since(start).Milliseconds())
				// metrics.Normalizer_CommitOffsetsTotal.Set(client.)
				listCtx, listCancel := context.WithTimeout(context.Background(), listOffsetsTimeout)
				offsets, err := adm.ListCommittedOffsets(listCtx, topics...)
				listCancel()
				if err != nil {
					metrics.Normalizer_CommitOffsetErrorsTotal.Inc()
					logger.Log.Error("ListCommittedOffsets failed", "error", err)
				} else {
					offsets.Each(func(lo kadm.ListedOffset) {
						metrics.Normalizer_CommitOffsetsTotal.WithLabelValues(lo.Topic, strconv.Itoa(int(lo.Partition))).Set(float64(lo.Offset))
					})
				}
			}

			latency := time.Since(start).Seconds()
			metrics.Normalizer_CommitLatencySeconds.Observe(latency)
		}
	}
}
