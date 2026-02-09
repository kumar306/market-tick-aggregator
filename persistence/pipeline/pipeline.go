package pipeline

import (
	"context"
	"market-persistence/batcher"
	"market-persistence/batcher/util"
	"market-persistence/converter"
	"market-persistence/db/model"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// let pipeline own the batcher and converter
// batcher calls flush but doesnt know anything about postgres, let it know about generic sinks
// flushFn is referred by batcher - input fn which is tied to a generic sink
type Pipeline[U any] struct {
	Name      string
	Converter converter.Converter[U]
	Batcher   batcher.Batcher[U]
}

type FlushFn[T any] func(context.Context, util.Tx, []T) error

func InitTickPipeline(ctx context.Context,
	name string,
	topic string,
	batchSize int,
	intervalMs time.Duration,
	flushFn FlushFn[*model.AggregatedTick]) *Pipeline[*model.AggregatedTick] {

	batcher := batcher.NewBatcher(ctx,
		name,
		batchSize,
		intervalMs,
		(func(context.Context, util.Tx, []*model.AggregatedTick) error)(flushFn),
		util.NewPostgresSink(),
		util.NewKafkaOffsetCommitter(topic))

	go batcher.Run()

	return &Pipeline[*model.AggregatedTick]{
		Converter: converter.NewTickConverter(),
		Batcher:   batcher,
		Name:      name,
	}
}

func InitBookPipeline(ctx context.Context,
	name string,
	topic string,
	batchSize int,
	intervalMs time.Duration,
	flushFn FlushFn[*model.OrderbookFlush]) *Pipeline[*model.OrderbookFlush] {

	batcher := batcher.NewBatcher(ctx,
		name,
		batchSize,
		intervalMs,
		(func(context.Context, util.Tx, []*model.OrderbookFlush) error)(flushFn),
		util.NewPostgresSink(),
		util.NewKafkaOffsetCommitter(topic))

	go batcher.Run()

	return &Pipeline[*model.OrderbookFlush]{
		Converter: converter.NewBookConverter(),
		Batcher:   batcher,
		Name:      name,
	}
}

// orchestrator
// convert from bytestream to row and delegate to batcher who delegates it further
func (p *Pipeline[U]) Process(rec *kgo.Record) {
	u, err := p.Converter.Convert(rec.Value)
	if err != nil {
		// stop service if the unmarshalling fails?
		panic(err)
	}
	p.Batcher.Add(batcher.BatchItem[U]{
		Item:      u,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	})
}
