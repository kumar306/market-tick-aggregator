package kafka

import (
	"context"
	"market-normalizer/constants"
	"os"
	"shared/kafkaauth"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// MaxPartitionCount queries the broker for the live partition counts of all
// given topics and returns the largest, using a short-lived admin client of
// its own. main.go uses this once at startup to decide how many worker
// sessions to create.
func MaxPartitionCount(ctx context.Context, cfg *constants.KafkaConfig, topics ...string) (int, error) {

	authOpts, authErr := kafkaauth.IAMOpts(context.Background())
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
		return 0, err
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	details, err := adm.ListTopics(ctx, topics...)
	if err != nil {
		return 0, err
	}

	max := 0
	for _, topic := range topics {
		detail, ok := details[topic]
		if !ok || detail.Err != nil {
			return 0, logger.LogAndWrap("Could not fetch topic metadata", detail.Err, "topic", topic)
		}
		if n := len(detail.Partitions); n > max {
			max = n
		}
	}
	return max, nil
}
