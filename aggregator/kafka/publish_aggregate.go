package kafka

import (
	"context"
	"market-aggregator/proto/generated"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func PublishAggregate(aggregate *generated.AggregatedTick) {
	logger.Log.Info("Ready to publish aggregate to Kafka")

	val, err := proto.Marshal(aggregate)
	if err != nil {
		logger.Log.Error("Error in marshalling aggregate to bytes", "err", err)
		return
	}
	// create the kafka record containing topic, value
	// call the kafka produce fn with success and failure callback
	rec := &kgo.Record{
		Key:   []byte(aggregate.Exchange + ":" + aggregate.Channel + ":" + aggregate.Symbol),
		Value: val,
		Topic: DownstreamTopic,
	}

	Client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for aggregated ticks", "error", err)
		}
	})
}
