package backpressure

import (
	"context"
	"market-orderbook/constants"
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

func waitForState(t *testing.T, expected constants.BackpressureState, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if GetBackpressureState() == expected {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("expected state %v within %v, got %v", expected, timeout, GetBackpressureState())
}

func TestBackpressureTransitions(t *testing.T) {
	initMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
		ConfirmSeconds:          0,
		PollIntervalMs:          10,
	}

	bpCh := make(chan *constants.BackpressureEvent, 10)

	// ensure controller state initialized before wait loops
	InitBPController()
	go RunBackpressureController(ctx, cfg, bpCh)

	bpCh <- &constants.BackpressureEvent{MaxQueueUsage: 0.9}
	waitForState(t, constants.Suspect, 200*time.Millisecond)

	// keep usage high to confirm throttling
	bpCh <- &constants.BackpressureEvent{MaxQueueUsage: 0.9}
	waitForState(t, constants.Throttling, 200*time.Millisecond)

	// drop usage below low threshold to return to healthy
	bpCh <- &constants.BackpressureEvent{MaxQueueUsage: 0.3}
	waitForState(t, constants.Healthy, 200*time.Millisecond)
}
