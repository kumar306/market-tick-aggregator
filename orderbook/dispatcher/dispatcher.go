package dispatcher

import (
	"context"
	"hash/fnv"
	"market-orderbook/backpressure"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"market-orderbook/worker"
	"shared/logger"
	"shared/metrics"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func RunDispatcher(ctx context.Context, dispatchCh chan *kgo.Record,
	workerChannels []chan *constants.DispatchRecord) {

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done event in orderbook dispatcher.. returning")
			return
		case rec, ok := <-dispatchCh:
			{
				if !ok {
					logger.Log.Info("The orderbook dispatch channel is closed. Returning")
					return
				}

				bookUpdate := &generated.NormalizedBook{}
				if err := proto.Unmarshal(rec.Value, bookUpdate); err != nil {
					logger.Log.Error("Error in unmarshalling record in orderbook dispatcher", "Error", err)
					continue
				}

				shardKey := bookUpdate.Exchange + ":" + bookUpdate.Symbol

				hash := fnv.New32a()
				hash.Write([]byte(shardKey))
				hashSum := hash.Sum32()

				workerId := int(hashSum) % len(workerChannels)

				// let dispatcher monitor worker queue metrics
				usage := float64(len(workerChannels[workerId])) / float64(cap(workerChannels[workerId]))
				metrics.Orderbook_WorkerQueueDepth.WithLabelValues(strconv.Itoa(workerId)).Set(usage)

				dispatchRec := &constants.DispatchRecord{
					Event:    constants.ProcessEvent,
					Offset:   rec.Offset,
					Record:   rec,
					Update:   bookUpdate,
					Exchange: bookUpdate.Exchange,
					Symbol:   bookUpdate.Symbol,
					TsMs:     bookUpdate.EventTimeMillis,
				}

				select {
				case workerChannels[workerId] <- dispatchRec:
					backpressure.OnEnqueue(workerId, rec.Partition)
				default:
					logger.Log.Warn("Dropping dispatch record due to full worker channel",
						"workerId", workerId,
						"partition", rec.Partition,
						"offset", rec.Offset)
				}
			}
		}
	}
}

func CreateWorkerChannels(workerCount int, chanSize int) []chan *constants.DispatchRecord {
	var workerChannels []chan *constants.DispatchRecord

	for i := 0; i < workerCount; i++ {
		ch := make(chan *constants.DispatchRecord, chanSize)
		workerChannels = append(workerChannels, ch)
	}

	logger.Log.Info("Created worker channels", "count", workerCount)
	return workerChannels
}

func CreateWorkerAckChannels(workerCount int, chanSize int) []chan *constants.Ack {
	var workerAckChannels []chan *constants.Ack

	for i := 0; i < workerCount; i++ {
		ch := make(chan *constants.Ack, chanSize)
		workerAckChannels = append(workerAckChannels, ch)
	}

	logger.Log.Info("Created worker ack channels", "count", workerCount)
	return workerAckChannels
}

func StartWorkerChannels(ctx context.Context,
	snapshotIntervalSec int,
	flushDepth int,
	workerChannels []chan *constants.DispatchRecord,
	AckCh chan *constants.Ack,
	workerAckChannels []chan *constants.Ack) {
	for idx, ch := range workerChannels {
		go startWorker(ctx, idx, snapshotIntervalSec, flushDepth, ch, AckCh, workerAckChannels[idx])
	}
}

func startWorker(ctx context.Context, idx int,
	snapshotIntervalSec int,
	flushDepth int,
	ch chan *constants.DispatchRecord,
	AckCh, updateAckCh chan *constants.Ack) {
	logger.Log.Info("Starting worker.", "workerIdx", idx)
	worker := worker.NewWorker(idx, ctx, snapshotIntervalSec, flushDepth, ch, AckCh, updateAckCh)
	worker.Run()
}
