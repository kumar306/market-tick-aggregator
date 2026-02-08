package dispatcher

import (
	"context"
	"market-persistence/db/model"
	"market-persistence/pipeline"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	AggregatedTicksTopic  string = "aggregated_ticks"
	OrderbookFlushesTopic string = "aggregated_book"
)

func RunDispatcher(ctx context.Context,
	dispatchCh chan *kgo.Record,
	tickPipeline pipeline.Pipeline[*model.AggregatedTick],
	bookPipeline pipeline.Pipeline[*model.OrderbookFlush]) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event.. returning dispatcher loop")
			return
		case rec := <-dispatchCh:
			// should be dumb. let pipeline take care
			topic := rec.Topic
			switch topic {
			case AggregatedTicksTopic:
				tickPipeline.Process(rec)
			case OrderbookFlushesTopic:
				bookPipeline.Process(rec)
			}
		}
	}
}
