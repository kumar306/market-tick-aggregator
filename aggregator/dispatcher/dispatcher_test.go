package dispatcher_test

import (
	"context"
	"market-aggregator/dispatcher"
	"market-aggregator/proto/generated"
	"shared/metrics"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// dispatcher must route by partition, not by symbol hash: a worker has to
// exclusively own a partition's offset space so that marking/committing
// offsets for that partition never interleaves across workers.
// verify records from the same partition always land on the same worker regardless of symbol, and different partitions land on their own, independent workers.
func TestRouting(t *testing.T) {

	metrics.InitAggregatorMetrics()

	ctx, _ := context.WithCancel(context.Background())
	dispatchChannel := make(chan *kgo.Record, 1000)
	workerChannels := dispatcher.CreateWorkerChannels(8, 1000)
	wg := sync.WaitGroup{}

	dispatcher.DispatchTestingHook = func() {
		wg.Done()
	}

	go dispatcher.RunDispatcher(ctx, dispatchChannel, workerChannels)

	mockProtoETH := &generated.NormalizedTick{
		Exchange: "coinbase",
		Channel:  "ticker",
		Symbol:   "ETH-USD",
	}
	valETH, err := proto.Marshal(mockProtoETH)
	require.NoError(t, err)

	mockProtoBTC := &generated.NormalizedTick{
		Exchange: "coinbase",
		Channel:  "ticker",
		Symbol:   "BTC-USD",
	}
	valBTC, err := proto.Marshal(mockProtoBTC)
	require.NoError(t, err)

	// 3 records for ETH-USD, all on partition 2
	for i := 0; i < 3; i++ {
		wg.Add(1)
		dispatchChannel <- &kgo.Record{Key: []byte("k"), Topic: "normalized.ticks", Partition: 2, Value: valETH}
	}

	// 3 records for BTC-USD, all on partition 5 -- different symbol, different partition
	for i := 0; i < 3; i++ {
		wg.Add(1)
		dispatchChannel <- &kgo.Record{Key: []byte("k"), Topic: "normalized.ticks", Partition: 5, Value: valBTC}
	}

	wg.Wait()

	require.Equal(t, 3, len(workerChannels[2]), "all partition-2 records should land on worker 2, regardless of symbol")
	require.Equal(t, 3, len(workerChannels[5]), "all partition-5 records should land on worker 5, regardless of symbol")
}
