package kafka_test

import (
	"context"
	"encoding/json"
	kf "market-adapter/kafka"
	"shared/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
)

// start up kafka in testcontainer
// defer the container termination
// get the broker address, pass it to init so franz-go client can init the client and return
// create a 'coinbase_raw_ticker' topic programmatically and specify num_partitions - use kadm to create this
// create the key, value, etc and call kafka.produceAsync
// fetch from the same topic - if len(records) = 0 or some error, then means message didnt get produced so fail the test

func Test_ProduceAsync(t *testing.T) {
	metrics.InitAdapterMetrics()
	ctx := context.Background()

	kafkaContainer, err := kafka.Run(ctx,
		"confluentinc/confluent-local:7.4.4",
		kafka.WithClusterID("123"))
	require.NoError(t, err)

	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()
	if err != nil {
		t.Fatalf("Error in starting the kafka container: %v", err)
	}

	brokers, err := kafkaContainer.Brokers(ctx)
	if err != nil || len(brokers) == 0 {
		t.Fatalf("Error in fetching broker connections: %v", err)
	}

	client, err := kf.Init(brokers)
	defer kf.Close()
	if err != nil || client == nil {
		t.Fatalf("Error in init kafka producer client: %v", err)
	}

	// create the topic with partitions and replication factor via kadm
	adm := kadm.NewClient(client)

	topicName := "coinbase_raw_ticker"
	numPartitions := int32(3)
	replicationFactor := int16(1)

	createResponse, err := adm.CreateTopic(ctx, numPartitions, replicationFactor, nil, topicName)
	if err != nil {
		t.Fatalf("Error in creating the test topic: %v", err)
	}

	if createResponse.Topic == topicName {
		t.Logf("Created the topic successfully")
	}

	// make sure client can consume from this topic - else timeout on poll
	client.AddConsumeTopics(topicName)

	deadline := time.Now().Add(10 * time.Second)
	for {
		metadata, _ := adm.Metadata(ctx, topicName)
		t.Logf("Received metadata topics: %v", metadata.Topics.Names())
		if len(metadata.Topics.Names()) > 0 && topicName == metadata.Topics.Names()[0] {
			t.Logf("Client ready to consume from topic: %v", topicName)
			break
		}

		if time.Now().After(deadline) {
			t.Fatalf("Client timed out waiting for topic metadata")
		}
	}

	// now create and persist the message - get the json, write it
	val := []byte(`
	{
		"exchange": "coinbase",
		"channel": "ticker",
		"type": "ticker",
		"sequence": 37475248783,
		"product_id": "ETH-USD",
		"price": "1285.22",
		"open_24h": "1310.79",
		"volume_24h": "245532.79269678",
		"low_24h": "1280.52",
		"high_24h": "1313.8",
		"volume_30d": "9788783.60117027",
		"best_bid": "1285.04",
		"best_bid_size": "0.46688654",
		"best_ask": "1285.27",
		"best_ask_size": "1.56637040",
		"side": "buy",
		"time": "2022-10-19T23:28:22.061769Z",
		"trade_id": 370843401,
		"last_size": "11.4396987"
	}
	`)
	key := []byte("ETH-USD")

	kf.ProduceAsync(topicName, "coinbase", "ticker", key, val)
	// dont buffer this produce
	client.Flush(ctx)

	fetched := false
	// read the message. if message present, then test passes. add some assertions
	require.Eventually(t, func() bool {
		fetches := client.PollFetches(ctx)
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			var resultMap map[string]interface{}
			t.Logf("Consumed message: %v", string(rec.Value))
			require.NoError(t, json.Unmarshal(rec.Value, &resultMap))
			require.Equal(t, "ETH-USD", resultMap["product_id"])
			require.Equal(t, "coinbase", resultMap["exchange"])
			fetched = true
			break
		}
		return fetched
	}, 10*time.Second, 200*time.Millisecond, "No message consumed within timeout")

}
