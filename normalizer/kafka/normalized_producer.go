package kafka

import (
	"context"
	"market-adapter/metrics"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(topic string, name string, channel string, key, value []byte) {

	logger.Log.Info("Ready to publish normalized record to downstream services", "name", name, "channel", channel, "topic", topic, "key", string(key))

	record := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "name", name, "error", err)
			metrics.FeedErrors.WithLabelValues(name + "|" + channel).Inc()
		} else {
			logger.Log.Info("Published record to kafka topic", "name", name, "channel", channel, "topic", topic)
			metrics.KafkaPublishes.WithLabelValues(name + "|" + channel).Inc()
		}
	})
}
