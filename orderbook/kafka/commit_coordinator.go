package kafka

import (
	"context"
	"market-orderbook/constants"
	"shared/logger"
	"time"

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
	LastCommittedEpoch int32
}

var Coordinator *CommitCoordinator

func InitCoordinator() {
	Coordinator = &CommitCoordinator{
		EpochMap:           make(map[int32]*EpochState),
		LastCommittedEpoch: 0,
	}
}

func StartEpoch(epoch int32, participants map[int]struct{}) {
	logger.Log.Info("Starting epoch in coordinator", "epoch", epoch)
	Coordinator.EpochMap[epoch] = &EpochState{
		Epoch:        epoch,
		CreatedAt:    time.Now(),
		Participants: participants,
		Acks:         make(map[int]map[int32]int64),
	}
}

func RunWorkerAckCommitter(ctx context.Context, client *kgo.Client, ackChannel chan *constants.FlushAck) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done in coordinated committer loop.. returning")
			return
		case flushAck := <-ackChannel:
			{
				// flush ack is from some worker
				// delete the participant from the remaining participants yet to ack
				// set flush ack's partition offsets to the flush ack's worker entry
				// if coordinator's epoch map's epoch's acks is of len(participants)
				// then calc the min

				if _, ok := Coordinator.EpochMap[flushAck.Epoch].Participants[flushAck.WorkerID]; !ok {
					continue
				}

				delete(Coordinator.EpochMap[flushAck.Epoch].Participants, flushAck.WorkerID)
				Coordinator.EpochMap[flushAck.Epoch].Acks[flushAck.WorkerID] = flushAck.PartitionOffsets

				// if no participants left to ack for the epoch
				if len(Coordinator.EpochMap[flushAck.Epoch].Participants) == 0 {
					// assemble the last committed map
					// for each worker, the least offset value for each partition
					newCommittedPartitionOffsets := make(map[int32]int64)

					for _, val := range Coordinator.EpochMap[flushAck.Epoch].Acks {
						for partition, offset := range val {
							if _, ok := newCommittedPartitionOffsets[partition]; !ok {
								newCommittedPartitionOffsets[partition] = offset
							} else {
								newCommittedPartitionOffsets[partition] = min(newCommittedPartitionOffsets[partition], offset)
							}
						}
					}

					uncommitted := make(map[string]map[int32]kgo.EpochOffset)

					for partition, offset := range newCommittedPartitionOffsets {
						uncommitted[ConsumerGroup][partition] = kgo.EpochOffset{
							// written in doc to do Offset: the offset to read next from. so inc 1
							Offset: offset + 1,
							Epoch:  -1,
						}
					}

					// commit the offsets
					client.CommitOffsetsSync(ctx, uncommitted, func(c *kgo.Client,
						req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {

					})

					// broadcast it back to the workers via ack channel

					// cleanup epoch state
				}
			}
		}
	}
}
