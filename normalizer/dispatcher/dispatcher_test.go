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

// test whether the dispatcher routing to worker is correct. test whether similar messages go to the same worker that is
// send 3 coinbase ticker ETH-USD, 3 coinbase ticker BTC-USD
// coinbase ticker ETH-USD all should go to one worker. all cb ticker BTC-USD goes to another worker
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

	rec1 := &kgo.Record{Key: []byte("ETH-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}
	rec2 := &kgo.Record{Key: []byte("ETH-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}
	rec3 := &kgo.Record{Key: []byte("ETH-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}
	rec4 := &kgo.Record{Key: []byte("BTC-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}
	rec5 := &kgo.Record{Key: []byte("BTC-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}
	rec6 := &kgo.Record{Key: []byte("BTC-USD"), Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}

	wg.Add(1)
	dispatchChannel <- rec1
	wg.Add(1)
	dispatchChannel <- rec2
	wg.Add(1)
	dispatchChannel <- rec3
	wg.Add(1)
	dispatchChannel <- rec4
	wg.Add(1)
	dispatchChannel <- rec5
	wg.Add(1)
	dispatchChannel <- rec6

	wg.Wait()

	var workerIdx int = -1

	var count int = 0
	var usedWorkers []int = make([]int, 0, 2)

	for idx := range workerChannels {
		if len(workerChannels[idx]) == 3 {
			count++
			usedWorkers = append(usedWorkers, workerIdx)
		}
	}

	require.Equal(t, 2, count, "3 similar records should be routed to 2 separate workers")
	t.Logf("ETH-USD routed consistently to workers %d, %d", usedWorkers[0], usedWorkers[1])
}
