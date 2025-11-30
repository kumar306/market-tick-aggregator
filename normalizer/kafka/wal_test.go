package kafka_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"market-normalizer/utils/kafkatest"
	"os"
	"runtime"
	"shared/metrics"
	"strings"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// make the breaker to open state.
// then verify that record enters wal append method and not downstream produce
func TestAppend(t *testing.T) {

	metrics.InitNormalizerMetrics()

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

// need to toggle the breaker to open. do some appends. then make it closed and then the replay triggers.
// need to ensure the downstream produce happened. and the file is cleared.
func TestReplay(t *testing.T) {

	metrics.InitNormalizerMetrics()

	_, kafkaContainer := kafkatest.InitKafkaContainer(t)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

	// create the wal
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\wal.log"
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
		kafka.Client.Close()
		time.Sleep(150 * time.Millisecond)
		kafka.Wal.Close()
		kafka.Wal = nil
		t.Logf("Closed wal after test execution over")
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
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
	time.Sleep(150 * time.Millisecond)

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
	metrics.InitNormalizerMetrics()

	_, kafkaContainer := kafkatest.InitKafkaContainer(t)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

	// create the wal
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\wal.log"
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
		kafka.Client.Close()
		time.Sleep(150 * time.Millisecond)
		kafka.Wal.Close()
		kafka.Wal = nil
		t.Logf("Closed wal after test execution over")
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
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

	kafka.ReplayFailureHook = func(idx int) error {
		return fmt.Errorf("Error during WAL replay at idx %v", idx)
	}
	kafka.ReplayFailureHookRecord = 2

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
	time.Sleep(150 * time.Millisecond)

	f, err := os.Open(walPath)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	afterData, _ := io.ReadAll(f)
	f.Close()

	newEntries, err := decodeWALEntries(afterData)
	require.NoError(t, err)
	require.Len(t, newEntries, 3)
}

// test for new message to enter the pipeline at the same time replay is happening
// and it is blocked until the replay is done (testing replayLock)
func TestOrdering(t *testing.T) {
	metrics.InitNormalizerMetrics()

	_, kafkaContainer := kafkatest.InitKafkaContainer(t)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

	// create the wal
	tmpDir := t.TempDir()
	walPath := tmpDir + "\\wal.log"
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
		kafka.Client.Close()
		time.Sleep(150 * time.Millisecond)
		kafka.Wal.Close()
		kafka.Wal = nil
		t.Logf("Closed wal after test execution over")
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
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

	kafka.ProduceAsync(ctx, topic, msg, key, val)

	kafka.ReplayStarted = make(chan struct{})
	kafka.ReplayDone = make(chan struct{})
	kafka.ProduceUnblocked = make(chan struct{})
	kafka.ReplayStartedHook = func() {
		close(kafka.ReplayStarted)
	}

	// now the state changed to closed is triggered and replay happens
	<-kafka.ReplayStarted
	go func() {
		t.Logf("Sending new message from pipeline not part of replay")
		kafka.ProduceAsync(ctx, topic, msg, key, val) // should block here til replay is done
		close(kafka.ProduceUnblocked)
	}()

	t.Logf("Submitted the new message mid replay")

	<-kafka.ReplayDone

	select {
	case <-kafka.ProduceUnblocked:
		t.Logf("Ordering is preserved. New message processed after wal replay ended")
	case <-time.After(5 * time.Second):
		t.Fatalf("Original produce did not unblock after replay finished")
	}
}

func decodeWALEntries(data []byte) ([]kafka.WALEntry, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	var entries []kafka.WALEntry

	for {
		var e kafka.WALEntry
		if err := dec.Decode(&e); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		entries = append(entries, e)
	}

	return entries, nil
}
