package dispatcher

import (
	"context"
	"hash/fnv"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"shared/metrics"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

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
	bpCh := make(chan *constants.BackpressureEvent, 1)

	go RunDispatcher(ctx, dispatchCh, workerChannels, &constants.BackpressureConfig{}, bpCh)

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
		Value:  val,
		Offset: 7,
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
		if got.Offset != 7 {
			t.Fatalf("expected offset=7, got %d", got.Offset)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected record to be routed to worker")
	}

	select {
	case <-bpCh:
	default:
		t.Fatalf("expected backpressure event to be emitted")
	}
}
