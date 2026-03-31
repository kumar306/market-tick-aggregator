package dispatcher

import (
	"context"
	"hash/fnv"
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"market-aggregator/worker"
	"shared/logger"
	"shared/metrics"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var DispatchTestingHook func()

func RunDispatcher(ctx context.Context, dispatchChannel chan *kgo.Record, workerChannels []chan *constants.DispatchRecord) {
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
				metrics.Aggregator_TicksIngestedTotal.WithLabelValues(strconv.Itoa(workerIdx)).Inc()
			default:
				logger.Log.Warn("Dropping record as the worker channel is blocking", "worker", workerIdx)
				metrics.Aggregator_TicksDroppedTotal.WithLabelValues(strconv.Itoa(workerIdx)).Inc()
			}

			// injected only in testing to signal done
			if DispatchTestingHook != nil {
				DispatchTestingHook()
			}

		case <-ctx.Done():
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

	return workerChannels
}

func StartWorkerChannels(ctx context.Context, workerChannels []chan *constants.DispatchRecord, cfg []*constants.WindowConfig, client utils.KafkaClient) {
	for idx, ch := range workerChannels {
		go startWorker(ctx, idx, ch, cfg, client)
	}
}

func startWorker(ctx context.Context, idx int, ch chan *constants.DispatchRecord, cfg []*constants.WindowConfig, client utils.KafkaClient) {
	worker := worker.NewWorker(idx, ch, cfg)
	worker.Run(ctx, client)
}
