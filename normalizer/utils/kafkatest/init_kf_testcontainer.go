package kafkatest

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	kf "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// func to init kafka produce client out of testcontainers
func InitKafkaContainer(t *testing.T) (*kgo.Client, *kf.KafkaContainer) {
	ctx := context.Background()

	kafkaContainer, err := kf.Run(ctx,
		"confluentinc/confluent-local:7.4.4",
		kf.WithClusterID("123"))
	require.NoError(t, err)

	if err != nil {
		t.Fatalf("Error in starting the kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error in fetching broker connections: %v", err)
	}

	// topics := []string{"binance.raw.ticks", "binance.raw.level2", "coinbase.raw.ticks",
	// 	"coinbase.raw.level2", "kraken.raw.ticks", "kraken.raw.book"}
	topics := []string{"normalized.ticks"}

	cfg := &constants.KafkaConfig{
		Brokers:               brokers,
		Topics:                topics,
		ConsumerGroup:         "normalizer-group-1",
		MaxBufferRecords:      5000,
		CBTimeoutMillis:       1000,
		CBConsecutiveFailures: 2,
		CBReqCount:            0,
	}

	client := kafka.Init(ctx, cfg)
	if err != nil || client == nil {
		t.Fatalf("Error in init kafka producer client: %v", err)
	}

	// create the topic with partitions and replication factor via kadm
	adm := kadm.NewClient(client)

	numPartitions := int32(3)
	replicationFactor := int16(1)

	_, err = adm.CreateTopics(ctx, numPartitions, replicationFactor, nil, topics...)
	if err != nil {
		t.Fatalf("Error in creating the test topic: %v", err)
	}

	// make sure client can consume from the topics - else timeout on poll
	client.AddConsumeTopics(topics...)

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
