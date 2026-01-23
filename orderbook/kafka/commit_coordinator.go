package kafka

import (
	"context"
	"market-orderbook/constants"
	"shared/logger"
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
	LastCommittedEpoch int32
	FlushAckChannel    chan *constants.Ack
	UpdateAckChannels  []chan *constants.Ack
}

var Coordinator *CommitCoordinator

func NewCoordinator(workerCount int, updateAckChannels []chan *constants.Ack) *CommitCoordinator {
	return &CommitCoordinator{
		EpochMap:           make(map[int32]*EpochState),
		LastCommittedEpoch: 0,
		FlushAckChannel:    make(chan *constants.Ack, workerCount),
		UpdateAckChannels:  updateAckChannels,
	}
}

func (c *CommitCoordinator) StartEpoch(epoch int32, participants map[int]struct{}) {
	logger.Log.Info("Starting epoch in coordinator", "epoch", epoch)
	c.EpochMap[epoch] = &EpochState{
		Epoch:        epoch,
		CreatedAt:    time.Now(),
		Participants: participants,
		Acks:         make(map[int]map[int32]int64),
	}
}

func (c *CommitCoordinator) RunWorkerAckCommitter(ctx context.Context, client *kgo.Client) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done in coordinated committer loop.. returning")
			return
		case Ack := <-c.FlushAckChannel:
			{
				// flush ack is from some worker
				// delete the participant from the remaining participants yet to ack
				// set flush ack's partition offsets to the flush ack's worker entry
				// if coordinator's epoch map's epoch's acks is of len(participants)
				// then calc the min

				if _, ok := c.EpochMap[Ack.Epoch].Participants[Ack.WorkerID]; !ok {
					continue
				}

				delete(c.EpochMap[Ack.Epoch].Participants, Ack.WorkerID)
				c.EpochMap[Ack.Epoch].Acks[Ack.WorkerID] = Ack.PartitionOffsets

				// if no participants left to ack for the epoch
				if len(c.EpochMap[Ack.Epoch].Participants) == 0 {
					// assemble the last committed map
					// for each worker, the least offset value for each partition
					minOffsetPerPartition := make(map[int32]int64)

					for _, val := range c.EpochMap[Ack.Epoch].Acks {
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
					client.CommitOffsets(ctx, uncommitted, func(client *kgo.Client,
						req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
						if err != nil {
							logger.Log.Error("Offset commit failed", "error", err)
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

						// broadcast it back to the workers via ack channel
						for _, ch := range c.UpdateAckChannels {
							select {
							case ch <- &constants.Ack{
								Epoch:            Ack.Epoch,
								PartitionOffsets: minOffsetPerPartition,
							}:
							default:
								logger.Log.Info("Update ack dropped as channel blocked or overloaded", "epoch", Ack.Epoch)
							}
						}

						// trigger a snapshot execute event from worker as now offsets are committed and cloned book can be backed up

						// cleanup epoch state
						c.LastCommittedEpoch = Ack.Epoch
						delete(c.EpochMap, Ack.Epoch)
					})
				}
			}
		}
	}
}
