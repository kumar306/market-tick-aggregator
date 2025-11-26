package kafka

import (
	"context"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// start a ticker which emits ticker event every X milliseconds.
// upon every event, commit offsets to kafka
func OffsetCommitter(ctx context.Context, gapMillis int, topics []string) {
	var ticker *time.Ticker = time.NewTicker(time.Duration(gapMillis) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context done.. shutting down kafka commit loop")
			if err := Client.CommitMarkedOffsets(context.Background()); err != nil {
				logger.Log.Error("Final commit failed", "error", err)
			}
			return
		case <-ticker.C:
			start := time.Now()
			err := Client.CommitMarkedOffsets(context.Background())
			if err != nil {
				logger.Log.Error("CommitMarkedOffsets failed", "error", err)
				metrics.Normalizer_CommitOffsetErrorsTotal.Inc()
			} else {
				logger.Log.Info("CommitMarkedOffsets ok")
				// metrics.Normalizer_CommitOffsetsTotal.Set(client.)
				offsets, err := adm.ListCommittedOffsets(ctx, topics...)
				if err != nil {
					metrics.Normalizer_CommitOffsetErrorsTotal.Inc()
				}
				offsets.Each(func(lo kadm.ListedOffset) {
					metrics.Normalizer_CommitOffsetsTotal.WithLabelValues(lo.Topic, string(lo.Partition)).Set(float64(lo.Offset))
				})
			}

			latency := time.Since(start).Seconds()
			metrics.Normalizer_CommitLatencySeconds.Observe(latency)
		}
	}
}
