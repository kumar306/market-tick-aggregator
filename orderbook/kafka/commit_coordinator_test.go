package kafka

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

func TestStartEpochCreatesState(t *testing.T) {
	initMetrics()
	c := NewCoordinator(2, []chan *constants.Ack{make(chan *constants.Ack, 1), make(chan *constants.Ack, 1)})

	participants := map[int]struct{}{0: {}, 1: {}}
	c.StartEpoch(1, participants)

	state, ok := c.EpochMap[1]
	if !ok || state == nil {
		t.Fatalf("expected epoch state to exist for epoch 1")
	}
	if len(state.Participants) != 2 {
		t.Fatalf("expected 2 participants, got %d", len(state.Participants))
	}
	if len(state.Acks) != 0 {
		t.Fatalf("expected empty acks initially, got %d", len(state.Acks))
	}
}

func TestHandleFlushAckIgnoresStaleOrNonParticipant(t *testing.T) {
	initMetrics()
	c := NewCoordinator(2, []chan *constants.Ack{make(chan *constants.Ack, 1), make(chan *constants.Ack, 1)})
	c.StartEpoch(1, map[int]struct{}{0: {}, 1: {}})

	// stale epoch
	c.HandleFlushAck(FlushAckEvent, &constants.Ack{Epoch: 2, WorkerID: 0}, context.Background(), nil)
	if len(c.EpochMap[1].Participants) != 2 {
		t.Fatalf("expected participants to remain unchanged for stale epoch")
	}

	// non-participant
	c.HandleFlushAck(FlushAckEvent, &constants.Ack{Epoch: 1, WorkerID: 2}, context.Background(), nil)
	if len(c.EpochMap[1].Participants) != 2 {
		t.Fatalf("expected participants to remain unchanged for non-participant ack")
	}
}

func TestHandleFlushAckTracksAcksWithoutCommitWhenPending(t *testing.T) {
	initMetrics()
	c := NewCoordinator(2, []chan *constants.Ack{make(chan *constants.Ack, 1), make(chan *constants.Ack, 1)})
	c.StartEpoch(1, map[int]struct{}{0: {}, 1: {}})

	ack := &constants.Ack{
		Epoch:    1,
		WorkerID: 0,
		PartitionOffsets: map[int32]int64{
			0: 10,
		},
	}
	c.HandleFlushAck(FlushAckEvent, ack, context.Background(), nil)

	state := c.EpochMap[1]
	if len(state.Participants) != 1 {
		t.Fatalf("expected 1 participant remaining, got %d", len(state.Participants))
	}
	if _, ok := state.Acks[0]; !ok {
		t.Fatalf("expected ack to be recorded for worker 0")
	}
	if c.LastCommittedEpoch != 0 {
		t.Fatalf("expected LastCommittedEpoch to remain 0 before commit")
	}
}

func TestCheckEpochTimeoutsDropsEmptyAcks(t *testing.T) {
	initMetrics()
	c := NewCoordinator(1, []chan *constants.Ack{make(chan *constants.Ack, 1)})
	c.StartEpoch(1, map[int]struct{}{0: {}})

	// force timeout
	c.EpochMap[1].CreatedAt = time.Now().Add(-c.EpochTimeout - time.Second)

	c.CheckEpochTimeouts(CheckEpochTimeoutsEvent, context.Background(), nil)

	if _, ok := c.EpochMap[1]; ok {
		t.Fatalf("expected timed out epoch with no acks to be removed")
	}
}

func TestPostCommitProcessBroadcastsOnFlush(t *testing.T) {
	initMetrics()
	ch1 := make(chan *constants.Ack, 1)
	ch2 := make(chan *constants.Ack, 1)
	c := NewCoordinator(2, []chan *constants.Ack{ch1, ch2})
	c.StartEpoch(5, map[int]struct{}{0: {}, 1: {}})

	offsets := map[int32]int64{0: 10, 1: 20}
	c.PostCommitProcess(&CommitResult{
		Epoch:     5,
		Offsets:   offsets,
		EventType: FlushAckEvent,
	})

	select {
	case ack := <-ch1:
		if ack.Epoch != 5 || ack.PartitionOffsets[1] != 20 {
			t.Fatalf("unexpected ack on ch1: %+v", ack)
		}
	default:
		t.Fatalf("expected ack on ch1")
	}

	select {
	case ack := <-ch2:
		if ack.Epoch != 5 || ack.PartitionOffsets[0] != 10 {
			t.Fatalf("unexpected ack on ch2: %+v", ack)
		}
	default:
		t.Fatalf("expected ack on ch2")
	}

	if c.LastCommittedEpoch != 5 {
		t.Fatalf("expected LastCommittedEpoch=5, got %d", c.LastCommittedEpoch)
	}
	if _, ok := c.EpochMap[5]; ok {
		t.Fatalf("expected epoch 5 to be cleaned up after commit")
	}
}

func TestPostCommitProcessNoBroadcastOnTimeout(t *testing.T) {
	initMetrics()
	ch := make(chan *constants.Ack, 1)
	c := NewCoordinator(1, []chan *constants.Ack{ch})
	c.StartEpoch(2, map[int]struct{}{0: {}})

	c.PostCommitProcess(&CommitResult{
		Epoch:     2,
		Offsets:   map[int32]int64{0: 1},
		EventType: CheckEpochTimeoutsEvent,
	})

	select {
	case <-ch:
		t.Fatalf("did not expect ack broadcast for timeout event")
	default:
	}
}
