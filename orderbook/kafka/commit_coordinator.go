package kafka

import (
	"context"
	"market-orderbook/constants"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type EpochState struct {
	Epoch     int32
	CreatedAt time.Time
	// workers participating in this epoch. sometimes a flush event to a worker may be dropped due to blocked channel, overloaded worker, etc
	Participants map[int]struct{}

	// map of worker, partition to its max offset
	// for all workers, go through acks and construct the min offset per partition map.
	// it will be committed and broadcasted back to the workers and used in snapshot logic
	Acks map[int]map[int32]int64
}

type CommitCoordinator struct {
	EpochMap           map[int32]*EpochState
	EpochTimeout       time.Duration
	LastCommittedEpoch int32
	EventChannel       chan *CoordinatorEvent
	FlushAckChannel    chan *constants.Ack
	UpdateAckChannels  []chan *constants.Ack
}

var Coordinator *CommitCoordinator

type CoordinatorEventType int

const (
	FlushAckEvent CoordinatorEventType = iota
	CommitResultEvent
	CheckEpochTimeoutsEvent
)

func (c CoordinatorEventType) String() string {
	switch c {
	case FlushAckEvent:
		return "Normal"
	case CheckEpochTimeoutsEvent:
		return "Timed out"
	default:
		return ""
	}
}

type CoordinatorEvent struct {
	EventType    CoordinatorEventType
	Ack          *constants.Ack
	CommitResult *CommitResult
}

type CommitResult struct {
	Epoch     int32
	Offsets   map[int32]int64
	Err       error
	EventType CoordinatorEventType
}

func NewCoordinator(workerCount int, updateAckChannels []chan *constants.Ack) *CommitCoordinator {
	return &CommitCoordinator{
		EpochMap:           make(map[int32]*EpochState),
		LastCommittedEpoch: 0,
		EpochTimeout:       2 * time.Minute,
		EventChannel:       make(chan *CoordinatorEvent, 100),
		FlushAckChannel:    make(chan *constants.Ack, workerCount),
		UpdateAckChannels:  updateAckChannels,
	}
}

func (c *CommitCoordinator) StartEpoch(epoch int32, participants map[int]struct{}) {
	logger.Log.Info("Starting epoch in coordinator", "epoch", epoch)
	if _, exists := c.EpochMap[epoch]; exists {
		logger.Log.Error("Epoch already exists", "epoch", epoch)
		return
	}

	c.EpochMap[epoch] = &EpochState{
		Epoch:        epoch,
		CreatedAt:    time.Now(),
		Participants: participants,
		Acks:         make(map[int]map[int32]int64),
	}

	metrics.Orderbook_ActiveEpochPendingParticipants.Set(float64(len(participants)))
	metrics.Orderbook_CommitActiveEpochs.Inc()
}

func (c *CommitCoordinator) PostCommitProcess(res *CommitResult) {

	// broadcast it back to the workers via ack channel who then trigger a snapshot execute
	// broadcast it only if its a flush event. dont broadcast for old epoch timeout handling
	if res.EventType == FlushAckEvent {
		for _, ch := range c.UpdateAckChannels {
			select {
			case ch <- &constants.Ack{
				Epoch:            res.Epoch,
				PartitionOffsets: res.Offsets,
			}:
			default:
				logger.Log.Info("Update ack dropped as channel blocked or overloaded", "epoch", res.Epoch)
			}
		}

		// update the metric only for epochs which have not timed out
		for p, o := range res.Offsets {
			metrics.Orderbook_CommitPartitionOffsets.WithLabelValues(strconv.Itoa(int(p))).Set(float64(o))
		}
	}

	// cleanup epoch state
	c.LastCommittedEpoch = max(res.Epoch, c.LastCommittedEpoch)
	delete(c.EpochMap, res.Epoch)

	metrics.Orderbook_FlushEpochsTotal.WithLabelValues(res.EventType.String()).Add(1)
	metrics.Orderbook_CommitActiveEpochs.Dec()
}

// multi event handling single threaded coordinator loops
func (c *CommitCoordinator) Run(ctx context.Context, client *kgo.Client) {
	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done in coordinated committer loop.. returning")
			return
		case ev := <-c.EventChannel:
			switch ev.EventType {
			case FlushAckEvent:
				c.HandleFlushAck(ev.EventType, ev.Ack, ctx, client)
			case CommitResultEvent:
				c.PostCommitProcess(ev.CommitResult)
			case CheckEpochTimeoutsEvent:
				c.CheckEpochTimeouts(ev.EventType, ctx, client)
			}
		case ack := <-c.FlushAckChannel:
			c.EventChannel <- &CoordinatorEvent{
				EventType: FlushAckEvent,
				Ack:       ack,
			}
		case <-ticker.C:
			c.EventChannel <- &CoordinatorEvent{
				EventType: CheckEpochTimeoutsEvent,
			}
		}
	}
}

func (c *CommitCoordinator) CheckEpochTimeouts(ev CoordinatorEventType, ctx context.Context, client *kgo.Client) {
	now := time.Now()

	// for timeout handling of existing epochs in coordinator
	// in the event didnt get ack from all participants.. those offsets will not be committed
	// in that case i will commit the epoch with whatever offsets we have
	// we can commit offset < latest committed offset - double committing old offset is cool
	var expired []int32
	for epoch, state := range c.EpochMap {
		if now.Sub(state.CreatedAt) < c.EpochTimeout {
			continue
		}

		// commit this incomplete epoch's offsets
		logger.Log.Warn("Epoch timed out. Committing partial", "epoch", epoch)

		if len(state.Acks) == 0 {
			logger.Log.Warn("Received no acks for the epoch. Not proceeding with commit")
			delete(c.EpochMap, epoch)
			continue
		}

		c.commitEpochOffsets(ev, epoch, ctx, client)
		expired = append(expired, epoch)
	}

	// cleanup old epochs from state if not done
	for _, e := range expired {
		delete(c.EpochMap, e)
	}
}

func (c *CommitCoordinator) commitEpochOffsets(ev CoordinatorEventType, epoch int32, ctx context.Context, client *kgo.Client) {
	// assemble the last committed map
	// for each worker, the least offset value for each partition
	minOffsetPerPartition := make(map[int32]int64)

	for _, val := range c.EpochMap[epoch].Acks {
		for partition, offset := range val {
			if _, ok := minOffsetPerPartition[partition]; !ok {
				minOffsetPerPartition[partition] = offset
			} else {
				minOffsetPerPartition[partition] = min(minOffsetPerPartition[partition], offset)
			}
		}
	}

	uncommitted := make(map[string]map[int32]kgo.EpochOffset)
	uncommitted[UpstreamTopic] = make(map[int32]kgo.EpochOffset)

	for partition, offset := range minOffsetPerPartition {
		uncommitted[UpstreamTopic][partition] = kgo.EpochOffset{
			// written in doc to do Offset: the offset to read next from. so inc 1
			Offset: offset + 1,
			Epoch:  -1,
		}
	}

	// commit the offsets
	start := time.Now()
	client.CommitOffsets(ctx, uncommitted, func(client *kgo.Client,
		req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			logger.Log.Error("Offset commit failed", "error", err)
			// notify the coordinator of error so he can still cleanup the state
			select {
			case c.EventChannel <- &CoordinatorEvent{
				EventType: CommitResultEvent,
				CommitResult: &CommitResult{
					Epoch:     epoch,
					Offsets:   nil,
					Err:       err,
					EventType: ev,
				},
			}:
			default:
				logger.Log.Error("Coordinator event channel full.. dropping commit error result", "epoch", epoch)
			}
			return
		}

		for _, topic := range resp.Topics {
			for _, partition := range topic.Partitions {
				if partition.ErrorCode != 0 {
					logger.Log.Error("Partition commit failed",
						"partition", partition.Partition,
						"error", kerr.ErrorForCode(partition.ErrorCode))
					return
				}
			}
		}

		metrics.Orderbook_CommitLatencyMs.Observe(float64(time.Since(start).Milliseconds()))

		// shifted post commit state broadcast and state cleanup back to coordinator
		// for single ownership of mutable state
		select {
		case c.EventChannel <- &CoordinatorEvent{
			EventType: CommitResultEvent,
			CommitResult: &CommitResult{
				Epoch:     epoch,
				Offsets:   minOffsetPerPartition,
				Err:       err,
				EventType: ev,
			},
		}:
		default:
			logger.Log.Error("Coordinator event channel full.. dropping commit result", "epoch", epoch)
		}
	})
}

func (c *CommitCoordinator) HandleFlushAck(ev CoordinatorEventType, Ack *constants.Ack, ctx context.Context, client *kgo.Client) {

	// flush ack is from some worker
	// delete the participant from the remaining participants yet to ack
	// set flush ack's partition offsets to the flush ack's worker entry
	// if coordinator's epoch map's epoch's acks is of len(participants)
	// then calc the min

	epochState, ok := c.EpochMap[Ack.Epoch]

	// late/stale epoch
	if !ok {
		return
	}

	// to ensure its a valid ack for the epoch from some participant
	if _, ok := epochState.Participants[Ack.WorkerID]; !ok {
		return
	}

	delete(c.EpochMap[Ack.Epoch].Participants, Ack.WorkerID)
	c.EpochMap[Ack.Epoch].Acks[Ack.WorkerID] = Ack.PartitionOffsets
	metrics.Orderbook_ActiveEpochPendingParticipants.Dec()

	// if no participants left to ack for the epoch
	if len(c.EpochMap[Ack.Epoch].Participants) == 0 {
		c.commitEpochOffsets(ev, Ack.Epoch, ctx, client)
		metrics.Orderbook_ActiveEpochPendingParticipants.Set(0)
	}
}
