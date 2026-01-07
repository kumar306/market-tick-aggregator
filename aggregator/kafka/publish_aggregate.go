package kafka

import (
	"context"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"shared/logger"
	"shared/metrics"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func PublishAggregate(aggregate *generated.AggregatedTick, client utils.KafkaClient) {
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

	client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for aggregated ticks", "error", err)
			metrics.Aggregator_ProduceFailuresTotal.WithLabelValues(aggregate.Exchange, aggregate.Channel, aggregate.Symbol, string(rec.Partition)).Inc()
			ProducerErrors <- err
		} else {
			metrics.Aggregator_ProduceSuccessesTotal.WithLabelValues(aggregate.Exchange, aggregate.Channel, aggregate.Symbol, string(rec.Partition)).Inc()
			ProducerErrors <- nil
		}
	})
}

// goroutine to read producer Errors channel and pass it into breaker
// breaker is triggered by this
func MonitorKafkaBreaker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done.. exiting monitor kafka breaker loop")
			return
		case err := <-ProducerErrors:
			logger.Log.Info("Reading err from producer err channel", "error", err)
			KafkaBreaker.Execute(func() (interface{}, error) {
				return nil, err
			})
			if KafkaBreakerTestingHook != nil {
				KafkaBreakerTestingHook()
			}
		}
	}
}
