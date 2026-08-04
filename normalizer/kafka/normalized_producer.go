package kafka

import (
	"context"
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// produces inside transaction is currently open on producer.
// onFailure used by worker to abort the current transaction at the next commit-interval boundary instead of committing it.
// an abort leaves the consumed offset uncommitted, so the next poll cycle naturally redelivers and retries.
// No wal fallback needed.
func ProduceAsync(producer constants.Producer, topic string, msg *constants.PipelineMessage, key, value []byte, onFailure func()) {
	start := time.Now()

	record := &kgo.Record{Key: key, Value: value, Topic: topic}

	producer.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "name", msg.Exchange, "error", err)
			metrics.Normalizer_ProducerPublishErrorsTotal.WithLabelValues(topic).Inc()
			onFailure()
			return
		}

		metrics.Normalizer_ProducerPublishesTotal.WithLabelValues(topic).Inc()
		latency := time.Since(start).Seconds()
		metrics.Normalizer_ProducerLatencySeconds.WithLabelValues(topic).Observe(latency)
	})
}

// ProduceSnapshotAsync publishes a synthetic snapshot (e.g. a Binance REST
// snapshot) with no backing consumed record. If it's lost, the next resync
// regenerates it, so a failure here doesn't need to abort the transaction.
func ProduceSnapshotAsync(producer constants.Producer, topic string, key, value []byte) {
	record := &kgo.Record{Key: key, Value: value, Topic: topic}

	producer.Produce(context.Background(), record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for snapshot", "topic", topic, "error", err)
			metrics.Normalizer_ProducerPublishErrorsTotal.WithLabelValues(topic).Inc()
			return
		}
		metrics.Normalizer_ProducerPublishesTotal.WithLabelValues(topic).Inc()
	})
}
