package kafka

import (
	"context"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

func ProduceAsync(ctx context.Context, client *kgo.Client, key, value []byte) {

	logger.Log.Info("Ready to flush kafka record to downstream")

	rec := &kgo.Record{
		Key:   key,
		Value: value,
		Topic: DownstreamTopic,
	}

	// fire and forget. its just a derived metric
	// if kafka publish fails, its okay. we will still track the offsets in memory and commit them
	// because we have correctly processed until offset X and cannot input correctness to downstream availability
	client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce to aggregate book failed", "error", err)
		} else {
			logger.Log.Info("Published record to aggregate book")
		}
	})
}
