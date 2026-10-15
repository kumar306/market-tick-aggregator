package kafka

import (
	"context"
	"shared/ingestts"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ingestMs is the websocket-read time of the newest update in this flushed book (0 = none new since the last flush).
func ProduceAsync(id int, ctx context.Context, client *kgo.Client, key, value []byte, ingestMs int64) {

	logger.Log.Info("Ready to flush kafka record to downstream")

	rec := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: DownstreamTopic,
	}
	if ingestMs > 0 {
		rec.Headers = []kgo.RecordHeader{ingestts.Header(ingestMs)}
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
