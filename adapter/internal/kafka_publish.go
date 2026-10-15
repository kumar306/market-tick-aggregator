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
	ring *ring.SpscDropOldestRing[constants.RawMessage]) {
	defer wg.Done()
	metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Inc()
	defer metrics.Adapter_SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// read from ring buffer
			msg, ok, dropped := ring.Pop()
			if !ok {
				// empty buffer case
				time.Sleep(1 * time.Millisecond)
				continue
			}

			// normalize after reading from ring buffer
			symbol, normalized, normalizeErr := normalizer.Normalize(msg.Data)

			if normalizeErr != nil {
				logger.Log.Error("Failed to normalize message for feed", "name", name, "err", normalizeErr, "msg", msg.Data)
				metrics.Adapter_NormalizerErrors.WithLabelValues(name).Inc()
				messageBufferPool.Put(&msg.Data)
				continue
			}

			// since normalized is a new byte[], msg is not used anymore, return it back to the buffer pool
			messageBufferPool.Put(&msg.Data)

			// to filter out kraken heartbeat messages. heartbeat returns nil
			if len(symbol) == 0 || len(normalized) == 0 {
				continue
			}

			// publish to kafka
			kafka.ProduceAsync(kafkaTopic, name, channel, symbol, normalized, dropped, msg.ReadAtMs)
		}
	}
}
