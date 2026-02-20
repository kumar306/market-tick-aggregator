package util

import (
	"context"
	"market-persistence/config"
	"market-persistence/kafka"
)

// let batcher not know about kafka directly. it just calls some notify processed fn and error handle
type EventProcessor interface {
	MarkUpstreamProcessed(context.Context, map[int32]int64) error
	HandleEventError(context.Context, *config.DLQMessage) error
}

type KafkaProcessor struct {
	topic string
}

// for unit testing it, let fn be separate
var commitOffsetsPostWrite = kafka.CommitOffsetsPostWrite
var publishToDlq = kafka.PublishToDlq

func NewKafkaProcessor(topic string) EventProcessor {
	return &KafkaProcessor{
		topic: topic,
	}
}

// call kafka package fn
func (kf *KafkaProcessor) MarkUpstreamProcessed(ctx context.Context, maxOffsetPerPartitionMap map[int32]int64) error {
	// error handled in callback
	commitOffsetsPostWrite(ctx, kf.topic, maxOffsetPerPartitionMap)
	return nil
}

func (kf *KafkaProcessor) HandleEventError(ctx context.Context, message *config.DLQMessage) error {
	return publishToDlq(ctx, message)
}
