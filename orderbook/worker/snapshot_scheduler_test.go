package worker

import (
	"context"
	"market-orderbook/constants"
	"testing"
	"time"
)

func TestRunSnapshotPrepareSchedulerEnqueuesEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updateCh := make(chan *constants.DispatchRecord, 2)
	ackCh := make(chan *constants.Ack, 1)
	updateAckCh := make(chan *constants.Ack, 1)

	w := NewWorker(1, ctx, 10, 15, updateCh, ackCh, updateAckCh)
	w.SnapshotPrepareIntervalSeconds = 1

	go w.RunSnapshotPrepareScheduler()

	select {
	case rec := <-updateCh:
		if rec.Event != constants.SnapshotRequestEvent {
			t.Fatalf("expected SnapshotRequestEvent, got %v", rec.Event)
		}
	case <-time.After(1500 * time.Millisecond):
		t.Fatalf("timed out waiting for snapshot request event")
	}
}
