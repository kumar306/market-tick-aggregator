package dispatcher_test

import (
	"context"
	"market-normalizer/dispatcher"
	"shared/metrics"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// dispatcher must route by partition, not by symbol hash: a worker has to
// exclusively own a partition's offset space so that marking/committing
// offsets for that partition never interleaves across workers.
// verify records from the same partition always land on the same worker
// different partitions land on their own, independent workers.
func TestRouting(t *testing.T) {

	metrics.InitNormalizerMetrics()
	ctx, _ := context.WithCancel(context.Background())
	dispatchChannel := make(chan *kgo.Record, 1000)
	workerChannels := dispatcher.CreateWorkerChannels(8, 1000)
	wg := sync.WaitGroup{}

	dispatcher.DispatchTestingHook = func() {
		wg.Done()
	}

	go dispatcher.StartDispatcher(ctx, dispatchChannel, workerChannels)

	value := []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")

	// 3 records for ETH-USD, all on partition 1
	for i := 0; i < 3; i++ {
		wg.Add(1)
		dispatchChannel <- &kgo.Record{Key: []byte("ETH-USD"), Topic: "coinbase.ticker", Partition: 1, Value: value}
	}

	// 3 records for BTC-USD, all on partition 4 - different symbol, different partition
	for i := 0; i < 3; i++ {
		wg.Add(1)
		dispatchChannel <- &kgo.Record{Key: []byte("BTC-USD"), Topic: "coinbase.ticker", Partition: 4, Value: value}
	}

	wg.Wait()

	require.Equal(t, 3, len(workerChannels[1]), "all partition-1 records should land on worker 1, regardless of symbol")
	require.Equal(t, 3, len(workerChannels[4]), "all partition-4 records should land on worker 4, regardless of symbol")
}
