package dispatcher

import (
	"context"
	"hash/fnv"
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

func DispatchRecord(ctx context.Context, dispatchChannel chan *kgo.Record, workerChannels []chan *constants.DispatchRecord) {
	// goroutine reads from the dispatch channel, shards it and routes it to respective worker
	for {
		select {
		case rec, ok := <-dispatchChannel:
			if !ok {
				logger.Log.Error("The dispatch channel is closed")
				return
			}

			tick := &generated.NormalizedTick{}
			// rec - read the info, parse top level - and send it to the worker channel
			if err := proto.Unmarshal(rec.Value, tick); err != nil {
				logger.Log.Error("Error in unmarshalling proto to normalized tick", "error", err)
				continue
			}

			bufferKey := tick.Exchange + ":" + tick.Channel + ":" + tick.Symbol

			hash := fnv.New32a()
			hash.Write([]byte(bufferKey))
			hashSum := hash.Sum32()

			workerIdx := int(hashSum) % len(workerChannels)

			// passing pointer to proto so we dont need to copy entire proto and its mutexes. just pass the same pointer
			dispatchRecord := &constants.DispatchRecord{
				Event:     constants.ProcessEvent,
				Tick:      tick,
				Record:    rec,
				BufferKey: bufferKey,
				WorkerIdx: workerIdx,
			}

			select {
			case workerChannels[workerIdx] <- dispatchRecord:
			case <-ctx.Done():
			default:
				logger.Log.Warn("Dropping record as the worker channel is blocking", "worker", workerIdx)
			}

		case <-ctx.Done():
			logger.Log.Info("Received context done - stopping aggregator dispatcher")
			return
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

func StartWorkerChannels(ctx context.Context, workerChannels []chan *constants.DispatchRecord, cfg []*constants.WindowConfig) {
	for idx, ch := range workerChannels {
		go startWorker(ctx, idx, ch, cfg)
	}
}

func startWorker(ctx context.Context, idx int, ch chan *constants.DispatchRecord, cfg []*constants.WindowConfig) {
	logger.Log.Info("Starting worker.", "workerIdx", idx)
	worker := worker.NewWorker(idx, ch)
	worker.Run(ctx, idx, ch, cfg)
}
