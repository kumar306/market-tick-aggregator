package kafka

import (
	"context"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(id int, ctx context.Context, client *kgo.Client, key, value []byte) {

	logger.Log.Info("Ready to flush kafka record to downstream")

	rec := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: DownstreamTopic,
	}

	start := time.Now()

	// fire and forget. its just a derived metric
	// if kafka publish fails, its okay. we will still track the offsets in memory and commit them
	// because we have correctly processed until offset X and cannot input correctness to downstream availability
	client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce to aggregate book failed", "error", err)
			metrics.Orderbook_FlushKafkaErrorsTotal.WithLabelValues(strconv.Itoa(id)).Add(1)
		} else {
			logger.Log.Info("Published record to aggregate book", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset)
			metrics.Orderbook_FlushSuccessTotal.WithLabelValues(strconv.Itoa(id)).Add(1)
			metrics.Orderbook_FlushLatencyMs.WithLabelValues(strconv.Itoa(id)).Observe(float64(time.Since(start).Milliseconds()))
		}
	})
}
