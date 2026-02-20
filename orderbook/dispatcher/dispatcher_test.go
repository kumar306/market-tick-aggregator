package dispatcher

import (
	"context"
	"hash/fnv"
	"market-orderbook/backpressure"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type noopPauseResumer struct{}

func (noopPauseResumer) PauseFetchPartitions(topicPartitions map[string][]int32) map[string][]int32 {
	return topicPartitions
}

func (noopPauseResumer) ResumeFetchPartitions(topicPartitions map[string][]int32) {}

var metricsOnce sync.Once

func initMetrics() {
	metricsOnce.Do(func() {
		metrics.InitOrderbookMetrics()
	})
}

func TestRunDispatcherRoutesToWorker(t *testing.T) {
	initMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dispatchCh := make(chan *kgo.Record, 1)
	workerChannels := CreateWorkerChannels(2, 10)
	backpressure.InitBP(&constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.9,
		QueueUsageLowThreshold:  0.4,
		ConfirmSeconds:          1,
		PollIntervalMs:          100,
	}, noopPauseResumer{}, "orderbook.upstream", int64(cap(workerChannels[0])))

	go RunDispatcher(ctx, dispatchCh, workerChannels)

	update := &generated.NormalizedBook{
		Exchange:        "coinbase",
		Symbol:          "ETH-USD",
		EventTimeMillis: 1000,
	}
	val, err := proto.Marshal(update)
	if err != nil {
		t.Fatalf("failed to marshal proto: %v", err)
	}

	rec := &kgo.Record{
		Value:     val,
		Offset:    7,
		Partition: 0,
	}

	dispatchCh <- rec

	hash := fnv.New32a()
	hash.Write([]byte("coinbase:ETH-USD"))
	workerID := int(hash.Sum32()) % len(workerChannels)

	select {
	case got := <-workerChannels[workerID]:
		if got.Event != constants.ProcessEvent {
			t.Fatalf("expected ProcessEvent, got %v", got.Event)
		}
		if got.Exchange != "coinbase" || got.Symbol != "ETH-USD" {
			t.Fatalf("unexpected exchange/symbol: %s/%s", got.Exchange, got.Symbol)
		}
		if got.Partition != 0 {
			t.Fatalf("expected partition=0, got %d", got.Partition)
		}
		if got.Offset != 7 {
			t.Fatalf("expected offset=7, got %d", got.Offset)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected record to be routed to worker")
	}
}
