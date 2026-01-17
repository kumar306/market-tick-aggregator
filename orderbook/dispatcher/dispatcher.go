package dispatcher

import (
	"context"
	"hash/fnv"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"market-orderbook/worker"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func RunDispatcher(ctx context.Context, dispatchCh chan *kgo.Record, workerChannels []chan *constants.DispatchRecord) {
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

				dispatchRec := &constants.DispatchRecord{
					Event:    constants.ProcessEvent,
					Offset:   rec.Offset,
					Record:   rec,
					Update:   bookUpdate,
					Exchange: bookUpdate.Exchange,
					Symbol:   bookUpdate.Symbol,
					TsMs:     bookUpdate.EventTimeMillis,
				}

				workerChannels[workerId] <- dispatchRec
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

func StartWorkerChannels(ctx context.Context, workerChannels []chan *constants.DispatchRecord) {
	for idx, ch := range workerChannels {
		go startWorker(ctx, idx, ch)
	}
}

func startWorker(ctx context.Context, idx int, ch chan *constants.DispatchRecord) {
	logger.Log.Info("Starting worker.", "workerIdx", idx)
	worker := worker.NewWorker(idx, ctx, ch)
	worker.Run()
}
