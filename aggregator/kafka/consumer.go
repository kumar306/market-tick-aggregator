package kafka

import (
	"context"
	"market-aggregator/constants"
	"os"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var DownstreamTopic string

// PartitionCount queries the broker for topic's live partition count using a
// short-lived admin client of its own -- there is no longer a shared package
// client to reuse, since each worker now owns its own GroupTransactSession.
// main.go calls this once at startup to decide how many worker sessions to
// create; from there, Kafka's own group coordinator hands out the actual
// partition assignment to each session.
func PartitionCount(ctx context.Context, cfg *constants.KafkaConfig, topic string) (int, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	adm := kadm.NewClient(client)
	details, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return 0, err
	}
	detail, ok := details[topic]
	if !ok || detail.Err != nil {
		return 0, logger.LogAndWrap("Could not fetch topic metadata", detail.Err, "topic", topic)
	}
	return len(detail.Partitions), nil
}
