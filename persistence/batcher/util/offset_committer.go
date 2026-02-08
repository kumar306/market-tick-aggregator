package util

import (
	"context"
	"market-persistence/kafka"
)

// let batcher not know about kafka directly. it just calls some commit offset fn
type OffsetCommitter interface {
	CommitOffsets(context.Context, map[int32]int64) error
}

type KafkaOffsetCommitter struct {
	topic string
}

func NewKafkaOffsetCommitter(topic string) OffsetCommitter {
	return &KafkaOffsetCommitter{
		topic: topic,
	}
}

// call kafka package fn
func (kf *KafkaOffsetCommitter) CommitOffsets(ctx context.Context, maxOffsetPerPartitionMap map[int32]int64) error {
	// error handled in callback
	kafka.CommitOffsetsPostWrite(ctx, kf.topic, maxOffsetPerPartitionMap)
	return nil
}
