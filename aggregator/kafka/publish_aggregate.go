package kafka

import (
	"context"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"shared/ingestts"
	"shared/logger"
	"shared/metrics"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// inside the open txn
// decides if the transaction gets committed or aborted at the next commit boundary.
// exec input callback on failure
// ingestMs is the websocket-read time of the newest tick in this aggregate (0 = none new since the last flush).
func PublishAggregate(client utils.KafkaClient, aggregate *generated.AggregatedTick, ingestMs int64, onFailure func()) {
	val, err := proto.Marshal(aggregate)
	if err != nil {
		logger.Log.Error("Error in marshalling aggregate to bytes", "err", err)
		onFailure()
		return
	}

	rec := &kgo.Record{
		Key:   []byte(aggregate.Exchange + ":" + aggregate.Channel + ":" + aggregate.Symbol),
		Value: val,
		Topic: DownstreamTopic,
	}
	if ingestMs > 0 {
		rec.Headers = []kgo.RecordHeader{ingestts.Header(ingestMs)}
	}

	client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Produce failed for aggregated ticks", "error", err)
			metrics.Aggregator_ProduceFailuresTotal.WithLabelValues(aggregate.Exchange, aggregate.Channel, aggregate.Symbol, strconv.Itoa(int(rec.Partition))).Inc()
			onFailure()
			return
		}
		metrics.Aggregator_ProduceSuccessesTotal.WithLabelValues(aggregate.Exchange, aggregate.Channel, aggregate.Symbol, strconv.Itoa(int(rec.Partition))).Inc()
	})
}
