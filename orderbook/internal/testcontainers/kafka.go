package testcontainers

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	kf "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const kafkaImage = "confluentinc/confluent-local:7.4.4"

func StartKafka(ctx context.Context, t *testing.T, topics []string) (*kgo.Client, *kf.KafkaContainer) {
	t.Helper()

	kafkaContainer, err := kf.Run(ctx, kafkaImage, kf.WithClusterID("123"))
	if err != nil {
		t.Fatalf("Error in starting the kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error in fetching broker connections: %v", err)
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("orderbook-test-group"),
		kgo.DisableAutoCommit(),
	)
	if err != nil || client == nil {
		_ = testcontainers.TerminateContainer(kafkaContainer)
		t.Fatalf("Error in init kafka client: %v", err)
	}

	if len(topics) > 0 {
		adm := kadm.NewClient(client)
		numPartitions := int32(1)
		replicationFactor := int16(1)

		_, err = adm.CreateTopics(ctx, numPartitions, replicationFactor, nil, topics...)
		if err != nil {
			_ = testcontainers.TerminateContainer(kafkaContainer)
			client.Close()
			t.Fatalf("Error in creating test topics: %v", err)
		}

		client.AddConsumeTopics(topics...)

		deadline := time.Now().Add(10 * time.Second)
		for {
			metadata, _ := adm.Metadata(ctx, topics...)
			if len(metadata.Topics.Names()) >= len(topics) {
				break
			}

			if time.Now().After(deadline) {
				_ = testcontainers.TerminateContainer(kafkaContainer)
				client.Close()
				t.Fatalf("Client timed out waiting for topic metadata")
			}
		}
	}

	return client, kafkaContainer
}
