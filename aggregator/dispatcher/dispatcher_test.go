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

// test whether the dispatcher routing to worker is correct. test whether similar messages go to the same worker that is
// send 3 coinbase ticker ETH-USD, 3 coinbase ticker BTC-USD
// coinbase ticker ETH-USD all should go to one worker. all cb ticker BTC-USD goes to another worker
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

	mockProto1 := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         143.22,
		Volume:        27,
		EventTsMillis: 1000032331,
		Open:          141.09,
		Close:         144.65,
		Low:           140.05,
		High:          144.92,
		SeqId:         3562310,
	}

	val1, err := proto.Marshal(mockProto1)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}

	mockProto2 := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         147.22,
		Volume:        28,
		EventTsMillis: 1000032337,
		Open:          142.09,
		Close:         145.65,
		Low:           141.05,
		High:          145.92,
		SeqId:         3662310,
	}

	val2, err := proto.Marshal(mockProto2)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}

	mockProto3 := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         144.22,
		Volume:        30,
		EventTsMillis: 1010032331,
		Open:          136.09,
		Close:         140.65,
		Low:           136.05,
		High:          140.92,
		SeqId:         3566310,
	}

	val3, err := proto.Marshal(mockProto3)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}

	rec1 := &kgo.Record{Key: []byte("coinbase:ticker:ETH-USD"), Topic: "normalized.ticks", Value: val1}
	rec2 := &kgo.Record{Key: []byte("coinbase:ticker:ETH-USD"), Topic: "normalized.ticks", Value: val2}
	rec3 := &kgo.Record{Key: []byte("coinbase:ticker:ETH-USD"), Topic: "normalized.ticks", Value: val3}

	wg.Add(1)
	dispatchChannel <- rec1
	wg.Add(1)
	dispatchChannel <- rec2
	wg.Add(1)
	dispatchChannel <- rec3

	wg.Wait()

	var count int = 0
	var usedWorkers []int = make([]int, 0, 1)

	for idx := range workerChannels {
		if len(workerChannels[idx]) == 3 {
			count++
			usedWorkers = append(usedWorkers, idx)
		}
	}

	require.Equal(t, 1, count, "Similar records should be routed to separate worker")
	t.Logf("Record routed consistently to worker %d", usedWorkers[0])
}
