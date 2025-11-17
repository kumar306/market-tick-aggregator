package kafka

import (
	"context"
	"market-adapter/metrics"
	"market-normalizer/constants"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(topic string, msg *constants.PipelineMessage, key, value []byte) {

	logger.Log.Info("Ready to publish normalized record to downstream services", "name", msg.Exchange, "channel", msg.Channel, "topic", topic, "key", string(key))

	record := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "name", msg.Exchange, "error", err)
			metrics.FeedErrors.WithLabelValues(msg.Exchange + "|" + msg.Channel).Inc()
		} else {
			logger.Log.Info("Published record to kafka topic", "name", msg.Exchange, "channel", msg.Channel, "topic", topic)
			metrics.KafkaPublishes.WithLabelValues(msg.Exchange + "|" + msg.Channel).Inc()
		}
	})

	// mark the record for commit.
	client.MarkCommitRecords(msg.Record)
}
