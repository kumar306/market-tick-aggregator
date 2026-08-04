package dispatcher

import (
	"context"
	"encoding/json"
	"market-normalizer/backpressure"
	"market-normalizer/constants"
	"market-normalizer/worker"
	"shared/logger"
	"shared/metrics"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var DispatchTestingHook func()
var WorkerTestingHook func()

/*
goroutine which reads from dispatch channel
parses the top level information
forwards to respective worker
*/
func StartDispatcher(ctx context.Context, dispatchChannel chan *kgo.Record, channelPool []chan *constants.DispatchRecord) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context done. Stopping dispatcher")
			return
		case rec := <-dispatchChannel:
			var symbol string = string(rec.Key)
			var header constants.Header

			if err := json.Unmarshal(rec.Value, &header); err != nil {
				logger.Log.Error("Error in unmarshalling record header fields", "error", err)
				continue
			}

			// compute hash - hash of feed + stream + symbol
			// route to respective worker
			dedupeKey := strings.ToLower(header.Exchange) + ":" + strings.ToLower(header.Channel) + ":" + strings.ToLower(symbol)

			// route by partition instead of symbol hash. worker must exclusively own
			// a partition's offset space otherwise two workers can mark/commit
			// offsets for the same partition out of order and silently skip an
			// unprocessed lower offset that a slower worker was still holding.
			workerId := uint32(rec.Partition) % uint32(len(channelPool))

			// let dispatcher monitor worker queue metrics similar to orderbook
			usage := float64(len(channelPool[workerId])) / float64(cap(channelPool[workerId]))
			metrics.Normalizer_WorkerQueueDepth.WithLabelValues(strconv.Itoa(int(workerId))).Set(usage)

			dispatchRec := &constants.DispatchRecord{
				Event:     constants.NewMessage,
				Record:    rec,
				BufferKey: dedupeKey,
				ShardKey:  workerId,
				Exchange:  header.Exchange,
				Channel:   header.Channel,
				Symbol:    symbol,
			}

			select {
			case channelPool[workerId] <- dispatchRec:
				backpressure.OnEnqueue(int(workerId), rec.Topic, rec.Partition)
			default:
				logger.Log.Warn("Dropping dispatch record due to full worker channel",
					"workerId", workerId,
					"partition", rec.Partition,
					"offset", rec.Offset)
			}

			// injected only in testing to signal done
			if DispatchTestingHook != nil {
				DispatchTestingHook()
			}

		}
	}
}

/*
method to create the worker channels
*/
func CreateWorkerChannels(numWorkers int, chanSize int) []chan *constants.DispatchRecord {
	var channelPool []chan *constants.DispatchRecord
	for i := 0; i < numWorkers; i++ {
		// bounded so it doesnt block
		workerChannel := make(chan *constants.DispatchRecord, chanSize)
		channelPool = append(channelPool, workerChannel)
	}
	return channelPool
}

/*
start the workers listening on those channels. shutdown worker pool on ctx shutdown
*/
func StartWorkerPool(ctx context.Context, channelPool []chan *constants.DispatchRecord) {
	for i, workerChannel := range channelPool {
		go func(workerId int, workerChannel chan *constants.DispatchRecord) {
			logger.Log.Info("Starting worker.", "worker", i)
			// in memory map per worker
			workerMap := make(map[string]*constants.SymbolState)
			dedupeBuf := make([]byte, 0, 96)
			for {
				select {
				case <-ctx.Done():
					logger.Log.Info("Worker stopping.", "worker", i)
					return
				case dispatchRec, ok := <-workerChannel:
					if !ok {
						logger.Log.Info("Channel is closed. Exiting")
						return
					}

					if dispatchRec == nil {
						continue
					}

					// dispatched record can be a new message or buffer flush event
					switch dispatchRec.Event {
					case constants.FlushBuffer:
						bufferFlushStart := time.Now()

						worker.FlushBuffer(ctx, dispatchRec, workerMap, dedupeBuf)

						bufferFlushLatency := time.Since(bufferFlushStart).Seconds()
						metrics.Normalizer_BufferFlushLatency.WithLabelValues(strconv.Itoa(i)).Observe(bufferFlushLatency)
						metrics.Normalizer_BufferFlushesTotal.WithLabelValues(strconv.Itoa(i)).Inc()

					case constants.SnapshotReady:
						worker.ProcessSnapshotReady(ctx, dispatchRec, workerMap, workerChannel, dedupeBuf)

					case constants.NewMessage:
						workerStartTime := time.Now()

						worker.ProcessRecord(ctx, dispatchRec, workerMap, workerChannel, dedupeBuf)
						workerLatency := time.Since(workerStartTime).Seconds()
						metrics.Normalizer_WorkerLatencySeconds.WithLabelValues(strconv.Itoa(i)).Observe(workerLatency)

						// call on dequeue after process for backpressure
						backpressure.OnDequeue(workerId)

						metrics.Normalizer_WorkerProcessedMessagesTotal.WithLabelValues(strconv.Itoa(i)).Inc()
						// used only in tests
						if WorkerTestingHook != nil {
							WorkerTestingHook()
						}
					}
				}
			}
		}(i, workerChannel)
	}
}
