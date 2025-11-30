package worker_test

import (
	"context"
	"market-normalizer/dedupe"
	"market-normalizer/dispatcher"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/proto/generated"
	"market-normalizer/utils/kafkatest"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestWorker(t *testing.T) {

	metrics.InitNormalizerMetrics()

	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()

	ctx, _ := context.WithCancel(context.Background())
	dispatchChannel := make(chan *kgo.Record, 1000)
	workerChannels := dispatcher.CreateWorkerChannels(8)
	dispatcher.StartWorkerPool(ctx, workerChannels)
	go dispatcher.StartDispatcher(ctx, dispatchChannel, workerChannels)
	wg := sync.WaitGroup{}

	dedupe.TestingHook = func() error {
		return nil
	}

	dispatcher.WorkerTestingHook = func() {
		wg.Done()
	}

	_, kafkaContainer := kafkatest.InitKafkaContainer(t)
	defer func() {
		if err := testcontainers.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error in terminating the kafka container: %v", err)
		}
	}()

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

	rec1 := &kgo.Record{Key: []byte("ETH/USD"), Topic: "kraken.ticker", Value: val}
	rec2 := &kgo.Record{Key: []byte("ETH/USD"), Topic: "kraken.ticker", Value: val}
	rec3 := &kgo.Record{Key: []byte("ETH/USD"), Topic: "kraken.ticker", Value: val}

	wg.Add(1)
	dispatchChannel <- rec1
	wg.Add(1)
	dispatchChannel <- rec2
	wg.Add(1)
	dispatchChannel <- rec3

	wg.Wait()
	// by now all finished processing

	// verify that all worker channels are empty
	for idx := range workerChannels {
		require.Len(t, workerChannels[idx], 0, "Worker buffers must be cleared after processing")
	}

	client := kafka.Client

	ctx2, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	readCount := 0
	symbolCounts := map[string]int{}

	require.Eventually(t, func() bool {
		fetches := client.PollFetches(ctx2)
		fetches.EachRecord(func(r *kgo.Record) {
			readCount++
			m := &generated.NormalizedTicker{}
			err := proto.Unmarshal(r.Value, m)
			require.NoError(t, err)
			symbolCounts[m.Symbol]++
		})

		return readCount == 3
	}, 10*time.Second, 200*time.Millisecond)

	require.Equal(t, 3, readCount)
	require.Equal(t, 3, symbolCounts["ETH/USD"])
}
