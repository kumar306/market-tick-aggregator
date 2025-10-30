package kafka

import (
	"context"
	"market-adapter/logger"
	"market-adapter/metrics"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	client *kgo.Client
	once   sync.Once
)

func Init(brokers []string) (*kgo.Client, error) {
	var err error
	once.Do(func() {
		client, err = kgo.NewClient(
			kgo.SeedBrokers(brokers...),
			kgo.ProduceRequestTimeout(5*time.Second),
			kgo.ProducerBatchCompression(kgo.GzipCompression()),
		)
	})
	return client, err
}

func ProduceAsync(topic string, name string, channel string, ctx context.Context, key, value []byte) {
	record := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "feed_name", name)
			metrics.FeedErrors.WithLabelValues(name + "|" + channel).Inc()
		}

		metrics.KafkaPublishes.WithLabelValues(name + "|" + channel).Inc()
	})
}

func Close() {
	if client != nil {
		client.Close()
	}
}
