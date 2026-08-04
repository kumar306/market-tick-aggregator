package worker_test

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/proto/generated"
	"market-normalizer/utils/kafkatest"
	"market-normalizer/worker"
	"shared/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// exercises the real transactional path end to end:
// a worker with its GroupTransactSession consumes raw records from a real broker,
// normalizes them, and produces to the downstream topic
// this only passes if the worker's transaction actually commits, not just if Produce was called.
func TestWorker(t *testing.T) {
	metrics.InitNormalizerMetrics()

	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()

	rawTopic := "kraken.ticker"
	downstreamTopic := "normalized.ticks"

	_, kafkaContainer := kafkatest.InitKafkaContainer(t, []string{rawTopic, downstreamTopic})
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	cfg := &constants.KafkaConfig{
		Brokers:          brokers,
		Topics:           []string{rawTopic},
		ConsumerGroup:    "normalizer-group-test",
		MaxBufferRecords: 5000,
	}

	session, err := kafka.NewWorkerSession(ctx, cfg, 0)
	require.NoError(t, err)

	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), nil)
	go w.Run(ctx, session, 500*time.Millisecond) // short commit interval so the test doesn't wait long

	pClient, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer pClient.Close()

	valJson := `
	{
	"exchange": "kraken",
    "channel": "ticker",
    "type": "snapshot",
    "data": [
        {
            "symbol": "ETH/USD",
            "bid": 0.10025,
            "bid_qty": 740.0,
            "ask": 0.10036,
            "ask_qty": 1361.44813783,
            "last": 0.10035,
            "volume": 997038.98383185,
            "vwap": 0.10148,
            "low": 0.09979,
            "high": 0.10285,
            "change": -0.00017,
            "change_pct": -0.17
        }]
	}`
	val := []byte(valJson)

	for i := 0; i < 3; i++ {
		rec := &kgo.Record{Key: []byte("ETH/USD"), Topic: rawTopic, Value: val}
		require.NoError(t, pClient.ProduceSync(ctx, rec).FirstErr())
	}

	// read_committed isolation level
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(downstreamTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	require.NoError(t, err)
	defer consumer.Close()

	readCount := 0
	symbolCounts := map[string]int{}

	require.Eventually(t, func() bool {
		fetchCtx, fetchCancel := context.WithTimeout(ctx, 1*time.Second)
		defer fetchCancel()
		fetches := consumer.PollFetches(fetchCtx)
		fetches.EachRecord(func(r *kgo.Record) {
			readCount++
			m := &generated.NormalizedTicker{}
			require.NoError(t, proto.Unmarshal(r.Value, m))
			symbolCounts[m.Symbol]++
		})
		return readCount == 3
	}, 15*time.Second, 300*time.Millisecond, "expected 3 committed records in %s", downstreamTopic)

	require.Equal(t, 3, readCount)
	require.Equal(t, 3, symbolCounts["ETH/USD"])
}
