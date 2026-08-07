package kafka

import (
	"context"
	"market-normalizer/constants"
	"os"
	"shared/kafkaauth"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// periodically reports true consumer-group lag (latest offset - group's committed offset) per topic/partition,
// independent of  transactional session - its read-only monitoring.
// one call covers the whole service's lag across every worker/partition combined
func KafkaConsumerMetrics(ctx context.Context, cfg *constants.KafkaConfig) {

	authOpts, authErr := kafkaauth.IAMOpts(ctx)
	if authErr != nil {
		logger.Log.Error("Error building MSK IAM auth", "error", authErr)
		os.Exit(1)
	}

	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	}, authOpts...)

	client, err := kgo.NewClient(opts...)

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
					metrics.Normalizer_ConsumerLag.WithLabelValues(memberLag.Topic, strconv.Itoa(int(memberLag.Partition))).Set(float64(memberLag.Lag))
				}
			})
		}
	}
}
