package kafka_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"os"
	"shared/metrics"
	"strings"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kf "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// make the breaker to open state.
// then verify that record enters wal append method and not downstream produce
func TestAppend(t *testing.T) {

	// create the wal
	tmpDir := t.TempDir()
	walPath := tmpDir + "/wal.log"
	walCfg := constants.KafkaConfig{
		WALPath:                  walPath,
		WALMaxEntries:            10,
		WALBackpressureThreshold: 0.8,
		WALCooldownTimeMillis:    5000,
		Topics:                   []string{},
	}

	_, err := kafka.NewWAL(&walCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer func() {
		kafka.Wal.Close()
		kafka.Wal = nil
	}()

	kafka.KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:    "kafka-circuit-breaker",
		Timeout: time.Duration(1000) * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures >= 2
		},
	})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// fail the breaker - its in open state
	kafka.KafkaBreaker.Execute(func() (interface{}, error) { return nil, errors.New("break") })
	kafka.KafkaBreaker.Execute(func() (interface{}, error) { return nil, errors.New("break") })

	// create the message

	valJson := `
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
	`
	val := []byte(valJson)
	key := []byte("ETH-USD")
	topic := "normalized.ticks"

	rec := &kgo.Record{
		Key:   key,
		Value: val,
		Topic: topic,
	}

	msg := &constants.PipelineMessage{
		Exchange:   "binance",
		Channel:    "aggtrades",
		Symbol:     "btcusdt",
		SeqId:      5,
		Record:     rec,
		RawMessage: &valJson,
	}

	// so 5 wal appends should happen here
	for i := 0; i < 5; i++ {
		kafka.ProduceAsync(ctx, topic, msg, key, val)
	}

	// read wal and verify entries got written
	f, err := os.OpenFile(walPath, os.O_RDONLY, 0644)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer f.Close()

	var entries []kafka.WALEntry
	scanner := bufio.NewScanner(f)

	for scanner.Scan() {
		var e kafka.WALEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			t.Fatalf("Error in reading WAL: %v", err)
		}
		entries = append(entries, e)
	}

	require.Len(t, entries, 5)

}

// func to init kafka produce client out of testcontainers
func initKafkaContainer(t *testing.T) (*kgo.Client, *kf.KafkaContainer) {
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

// need to toggle the breaker to open. do some appends. then make it closed and then the replay triggers.
// need to ensure the downstream produce happened. and the file is cleared.
func TestReplay(t *testing.T) {

	metrics.InitNormalizerMetrics()

	_, kafkaContainer := initKafkaContainer(t)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
		kafka.Client.Close()
	}()

	// create the wal
	tmpDir := t.TempDir()
	walPath := tmpDir + "/wal.log"
	walCfg := constants.KafkaConfig{
		WALPath:                  walPath,
		WALMaxEntries:            10,
		WALBackpressureThreshold: 0.8,
		WALCooldownTimeMillis:    5000,
		Topics:                   []string{},
	}

	_, err := kafka.NewWAL(&walCfg)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	t.Cleanup(func() {
		kafka.Wal.Close()
		kafka.Wal = nil
		t.Logf("Closed wal after test execution over")
	})

	ctx, _ := context.WithCancel(context.Background())

	// create the message
	valJson := `
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
	`
	val := []byte(valJson)
	key := []byte("ETH-USD")
	topic := "normalized.ticks"

	rec := &kgo.Record{
		Key:   key,
		Value: val,
		Topic: topic,
	}

	msg := &constants.PipelineMessage{
		Exchange:   "coinbase",
		Channel:    "ticker",
		Symbol:     "ETH-USD",
		SeqId:      5,
		Record:     rec,
		RawMessage: &valJson,
	}

	// to consume success and trigger cb to closed which replays
	go kafka.MonitorKafkaBreakerState(ctx)

	// fail the breaker - its in open state
	kafka.KafkaBreaker.Execute(func() (interface{}, error) { return nil, errors.New("break") })
	kafka.KafkaBreaker.Execute(func() (interface{}, error) { return nil, errors.New("break") })

	// so 5 wal appends should happen here
	for i := 0; i < 5; i++ {
		kafka.ProduceAsync(ctx, topic, msg, key, val)
	}
	time.Sleep(1 * time.Second)
	// on state change to be triggered now
	kafka.ProduceAsync(ctx, topic, msg, key, val)

	kafka.ReplayDone = make(chan struct{})

	t.Logf("test: waiting on ReplayDone channel addr=%p value=%v", kafka.ReplayDone, kafka.ReplayDone)
	<-kafka.ReplayDone

	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	afterData, _ := io.ReadAll(f)
	f.Close()

	if len(strings.TrimSpace(string(afterData))) != 0 {
		t.Fatalf("Error: The WAL is not truncated after replay")
	}

	t.Logf("WAL is truncated after replay")

	count := 0

	// should read 1 half open message + 5 wal replay messages
	require.Eventually(t, func() bool {
		fetches := kafka.Client.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			var resultMap map[string]interface{}
			t.Logf("Consumed message: %v", string(rec.Value))
			if err := json.Unmarshal(rec.Value, &resultMap); err != nil {
				t.Logf("Error occurred: %v", err)
				return
			}
			if resultMap["product_id"] != "ETH-USD" || resultMap["exchange"] != "coinbase" {
				t.Logf("The message is not matching expected value")
				return
			}

			count++
		})

		return count == 6

	}, 10*time.Second, 250*time.Millisecond, "All the 6 messages were not consumed within timeout")
}

// mock a error for the 3rd or 4th record processing out of 10 records.
// then need to ensure the new file contains the 4th to 10th record.
func TestReplayError(t *testing.T) {

}

// test for new message to enter the pipeline at the same time replay is happening
// and it is blocked until the replay is done (testing replayLock)
func TestOrdering(t *testing.T) {

}
