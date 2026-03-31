package kafka

import (
	"context"
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(ctx context.Context, topic string, msg *constants.PipelineMessage, key, value []byte) {

	if KafkaBreaker.State() == gobreaker.StateOpen {
		// fallback - persist to a disk backed file
		metrics.Normalizer_KafkaCB_FallbacksTotal.Inc()
		logger.Log.Warn("Kafka normalizer produce skipped as circuit breaker - OPEN state")

		if err := Wal.Append(topic, msg, key, value); err != nil {
			logger.Log.Error("Error in WAL append", "err", err)
		}

		return
	}

	// if file reaches 80 percent capacity (have a cap), then signal backpressure - pause fetch partitions

	start := time.Now()

	record := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: topic,
	}

	replayLock.RLock()
	defer replayLock.RUnlock()

	Client.Produce(ctx, record, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for topic", "topic", topic, "name", msg.Exchange, "error", err)
			metrics.Normalizer_ProducerPublishErrorsTotal.WithLabelValues(topic).Inc()
			// circuit breaker on broken produce
			producerErrors <- err

		} else {
			// mark the record for commit.
			Client.MarkCommitRecords(msg.Record)

			producerErrors <- nil

			metrics.Normalizer_ProducerPublishesTotal.WithLabelValues(topic).Inc()
			latency := time.Since(start).Seconds()
			metrics.Normalizer_ProducerLatencySeconds.WithLabelValues(topic).Observe(latency)
		}
	})
}

func MonitorKafkaBreakerState(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done.. shutting down kafka circuit breaker loop")
			return
		case err := <-producerErrors:

			KafkaBreaker.Execute(func() (interface{}, error) {
				return nil, err
			})
		}
	}
}
