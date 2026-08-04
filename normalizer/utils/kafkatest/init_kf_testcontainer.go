package kafkatest

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kf "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// InitKafkaContainer starts a real Kafka container and creates the given
// topics. The returned client is a plain admin/produce client, not tied to
// any worker session -- callers that need to consume or produce test data
// build their own client from kafkaContainer.Brokers(ctx).
func InitKafkaContainer(t *testing.T, topics []string) (*kgo.Client, *kf.KafkaContainer) {
	ctx := context.Background()

	kafkaContainer, err := kf.Run(ctx,
		"confluentinc/confluent-local:7.4.4",
		kf.WithClusterID("123"))
	require.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error in fetching broker connections: %v", err)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil || client == nil {
		t.Fatalf("Error in init kafka client: %v", err)
	}

	adm := kadm.NewClient(client)

	numPartitions := int32(3)
	replicationFactor := int16(1)

	_, err = adm.CreateTopics(ctx, numPartitions, replicationFactor, nil, topics...)
	if err != nil {
		t.Fatalf("Error in creating the test topic: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		metadata, _ := adm.Metadata(ctx, topics...)
		t.Logf("Received metadata topics: %v", metadata.Topics.Names())
		if len(metadata.Topics.Names()) > 0 {
			t.Logf("Client ready to consume from topics")
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Client timed out waiting for topic metadata")
		}
	}

	return client, kafkaContainer
}
