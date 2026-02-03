package flush

import (
	"context"
	"market-orderbook/constants"
	"market-orderbook/kafka"
	"shared/metrics"
	"sync"
	"testing"
	"time"
)

var metricsOnce sync.Once

func initMetrics() {
	metricsOnce.Do(func() {
		metrics.InitOrderbookMetrics()
	})
}

func TestRunEpochFlushSchedulerDispatchesAndStartsEpoch(t *testing.T) {
	initMetrics()
	Epoch = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workerChannels := []chan *constants.DispatchRecord{
		make(chan *constants.DispatchRecord, 1),
		make(chan *constants.DispatchRecord, 1),
	}

	updateAckChannels := []chan *constants.Ack{
		make(chan *constants.Ack, 1),
		make(chan *constants.Ack, 1),
	}

	coordinator := kafka.NewCoordinator(len(workerChannels), updateAckChannels)

	go RunEpochFlushScheduler(ctx, 1, workerChannels, coordinator)

	for idx, ch := range workerChannels {
		select {
		case rec := <-ch:
			if rec.Event != constants.FlushEvent {
				t.Fatalf("expected FlushEvent, got %v", rec.Event)
			}
			if rec.FlushEpoch != 1 {
				t.Fatalf("expected flush epoch=1, got %d", rec.FlushEpoch)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for flush event on worker %d", idx)
		}
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := coordinator.EpochMap[1]; ok {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	state, ok := coordinator.EpochMap[1]
	if !ok {
		t.Fatalf("expected epoch 1 to be started")
	}
	if len(state.Participants) != len(workerChannels) {
		t.Fatalf("expected %d participants, got %d", len(workerChannels), len(state.Participants))
	}
	if Epoch != 1 {
		t.Fatalf("expected global epoch=1, got %d", Epoch)
	}
}
