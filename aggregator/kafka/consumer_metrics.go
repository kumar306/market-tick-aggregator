package kafka

import (
	"context"
	"market-aggregator/constants"
	"os"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// same as normalizer - refer the same func in normalizer for doc
func KafkaConsumerMetrics(ctx context.Context, cfg *constants.KafkaConfig) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		logger.Log.Error("Failed to create kafka client for consumer metrics", "err", err)
		return
	}
	defer client.Close()

	adm := kadm.NewClient(client)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lags, err := adm.Lag(ctx, cfg.ConsumerGroup)
			if err != nil {
				logger.Log.Error("Error fetching consumer group lag", "err", err)
				continue
			}

			lags.Each(func(l kadm.DescribedGroupLag) {
				if err := l.Error(); err != nil {
					logger.Log.Error("Error describing group lag", "group", l.Group, "err", err)
					return
				}
				for _, memberLag := range l.Lag.Sorted() {
					metrics.Aggregator_ConsumerLag.WithLabelValues(memberLag.Topic, strconv.Itoa(int(memberLag.Partition))).Set(float64(memberLag.Lag))
				}
			})
		}
	}
}
