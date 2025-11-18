package internal

import (
	"context"
	"market-adapter/constants"
	"market-adapter/kafka"
	"market-adapter/ring"
	"shared/logger"
	"shared/metrics"
	"sync"
	"time"
)

func PublishToKafkaLoop(wg *sync.WaitGroup,
	name string,
	channel string,
	kafkaTopic string,
	ctx context.Context,
	normalizer constants.Normalizer,
	ring *ring.SpscDropOldestRing[[]byte]) {
	defer wg.Done()
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Inc()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Shutting down kafka publish loop", "name", name)
			return
		default:
			// read from ring buffer
			msg, ok := ring.Pop()
			if !ok {
				// empty buffer case
				time.Sleep(1 * time.Millisecond)
				continue
			}

			// normalize after reading from ring buffer
			symbol, normalized, normalizeErr := normalizer.Normalize(msg)
			if normalizeErr != nil {
				logger.Log.Error("Failed to normalize message for feed", "name", name, "err", normalizeErr)
				metrics.Adapter_NormalizerErrors.WithLabelValues().Inc()
				continue
			}

			// publish to kafka
			kafka.ProduceAsync(kafkaTopic, name, channel, symbol, normalized)
		}
	}
}
