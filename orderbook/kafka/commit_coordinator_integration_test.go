package kafka

import (
	"context"
	"market-orderbook/constants"
	testcontainers "market-orderbook/internal/testcontainers"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
)

func TestCommitCoordinatorCommitsAndBroadcasts(t *testing.T) {
	initMetrics()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	topic := "orderbook.upstream.test"

	client, kafkaContainer := testcontainers.StartKafka(ctx, t, []string{topic})
	defer client.Close()
	defer func() {
		if err := tc.TerminateContainer(kafkaContainer); err != nil {
			t.Fatalf("Error terminating kafka container: %v", err)
		}
	}()

	UpstreamTopic = topic

	updateAckCh := make(chan *constants.Ack, 1)
	coord := NewCoordinator(1, []chan *constants.Ack{updateAckCh})
	go coord.Run(ctx, client)

	coord.StartEpoch(1, map[int]struct{}{0: {}})
	coord.FlushAckChannel <- &constants.Ack{
		Epoch:    1,
		WorkerID: 0,
		PartitionOffsets: map[int32]int64{
			0: 5,
		},
	}

	select {
	case ack := <-updateAckCh:
		if ack.Epoch != 1 {
			t.Fatalf("expected ack epoch 1, got %d", ack.Epoch)
		}
		if ack.PartitionOffsets[0] != 5 {
			t.Fatalf("expected committed offset 5, got %d", ack.PartitionOffsets[0])
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for update ack")
	}
}
