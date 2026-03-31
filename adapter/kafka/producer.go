package kafka

import (
	"context"
	"os"
	"shared/logger"
	"shared/metrics"
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
			kgo.ProducerLinger(0),
			kgo.ProducerBatchMaxBytes(5*1024*1024),
			kgo.ProducerBatchCompression(kgo.GzipCompression()),
			kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
		)

		pingErr := client.Ping(context.Background())
		if pingErr != nil {
			logger.Log.Error("Error in pinging the seed broker")
			os.Exit(1)
		}

		if err != nil || client == nil {
			logger.Log.Error("Error when initializing kafka client", "error", err)
			os.Exit(1)
		}
	})
	return client, err
}

func ProduceAsync(topic string, name string, channel string, key, value []byte) {

	record := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	client.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "name", name, "error", err)
			metrics.Adapter_FeedErrors.WithLabelValues(name + "|" + channel).Inc()
		} else {
			metrics.Adapter_KafkaPublishes.WithLabelValues(name + "|" + channel).Inc()
		}
	})
}

func Close() {
	if client != nil {
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := client.Flush(flushCtx)
		if err != nil {
			logger.Log.Warn("Kafka flush timed out or canceled", "err", err)
		}

		client.Close()
	}
}
