package kafka_test

import (
	"context"
	"fmt"
	"market-normalizer/kafka"
	"market-normalizer/utils/kafkatest"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestConsumerLoop(t *testing.T) {
	metrics.InitNormalizerMetrics()

	ctx, _ := context.WithCancel(context.Background())
	wg := sync.WaitGroup{}

	topics := []string{"binance.raw.ticks", "binance.raw.level2", "coinbase.raw.ticks",
		"coinbase.raw.level2", "kraken.raw.ticks", "kraken.raw.book"}

	// start consumer client
	_, kafkaContainer := kafkatest.InitKafkaContainer(t, topics)
	defer func() {
		kafka.Close()
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

	brokers, _ := kafkaContainer.Brokers(ctx)

	// start producer client and pre produce some messages
	pClient, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ProduceRequestTimeout(5*time.Second),
		kgo.ProducerLinger(0),
	)
	require.NoError(t, err)
	defer pClient.Close()

	// create the topic with partitions and replication factor via kadm
	adm := kadm.NewClient(pClient)

	numPartitions := int32(3)
	replicationFactor := int16(1)

	_, err = adm.CreateTopics(ctx, numPartitions, replicationFactor, nil, topics...)
	if err != nil {
		t.Fatalf("Error in creating the test topic: %v", err)
	}

	for i := 0; i < 5; i++ {
		value := []byte(fmt.Sprintf(`{"id": %d}`, i))
		var topic string
		if i%2 == 0 {
			topic = "coinbase.raw.ticks"
		} else {
			topic = "binance.raw.level2"
		}
		rec := &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: value,
			Topic: topic,
		}

		wg.Add(1)
		pClient.Produce(ctx, rec, func(r *kgo.Record, err error) {
			require.NoError(t, err)
		})
	}

	require.NoError(t, pClient.Flush(ctx))

	dispatchChannel := make(chan *kgo.Record, 1000)

	kafka.TestingHook = func() {
		wg.Done()
	}

	go kafka.ConsumerLoop(ctx, kafka.Client, dispatchChannel)

	// wait till all messages are collected in the dispatch channel
	wg.Wait()

	// verify number of messages in dispatch ch = number of produced messages
	var collected []*kgo.Record

	require.Eventually(t, func() bool {
		for {
			select {
			case rec := <-dispatchChannel:
				collected = append(collected, rec)
				t.Logf("Collected message")
				if len(collected) == 5 {
					t.Logf("Successfully collected all the messages produced")
					return true
				}
			default:
				return false
			}
		}
	}, 5*time.Second, 200*time.Millisecond, "Number of messages in dispatch channel should be equal to number of produced messages")
}
