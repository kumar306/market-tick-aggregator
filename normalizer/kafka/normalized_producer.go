package kafka

import (
	"context"
	"errors"
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(ctx context.Context, topic string, msg *constants.PipelineMessage, key, value []byte) {

	if kafkaBreaker.State() == gobreaker.StateOpen {
		// fallback - persist to a disk backed file
		if err := wal.Append(topic, key, value); err != nil {
			logger.Log.Error("Error in WAL append", "err", err)
		}

		return
	}

	// if file reaches 80 percent capacity (have a cap), then signal backpressure - pause fetch partitions

	logger.Log.Info("Ready to publish normalized record to downstream services", "name", msg.Exchange, "channel", msg.Channel, "topic", topic, "key", string(key))

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
			logger.Log.Info("Published record to kafka topic", "name", msg.Exchange, "channel", msg.Channel, "topic", topic)

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
			_, execErr := kafkaBreaker.Execute(func() (interface{}, error) {
				return nil, err
			})

			if errors.Is(execErr, gobreaker.ErrOpenState) || errors.Is(execErr, gobreaker.ErrTooManyRequests) {
				metrics.Normalizer_KafkaCB_FallbacksTotal.Inc()
				logger.Log.Warn("Kafka normalizer produce skipped as circuit breaker - OPEN state", "err", execErr)
			}
		}
	}
}
