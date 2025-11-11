package dispatcher

import (
	"context"
	"encoding/json"
	"hash/fnv"
	"market-normalizer/constants"
	"market-normalizer/worker"
	"shared/logger"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

/*
goroutine which reads from dispatch channel
parses the top level information
forwards to respective worker
*/
func StartDispatcher(ctx context.Context, dispatchChannel <-chan *kgo.Record, channelPool []chan *kgo.Record, numWorkers int) {
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context done. Stopping dispatcher")
			return
		case rec := <-dispatchChannel:
			var symbol string
			var header constants.Header

			if err := json.Unmarshal(rec.Key, &symbol); err != nil {
				logger.Log.Error("Error in unmarshalling record key", "error", err)
				continue
			}

			if err := json.Unmarshal(rec.Value, &header); err != nil {
				logger.Log.Error("Error in unmarshalling record header fields", "error", err)
				continue
			}

			// compute hash - hash of feed + stream + symbol
			// route to respective worker
			hash := fnv.New32a()
			hash.Write([]byte(strings.ToLower(header.Exchange) + "-" + strings.ToLower(header.Channel) + "-" + strings.ToLower(symbol)))
			sum := hash.Sum32()

			shardKey := sum % uint32(numWorkers)
			channelPool[shardKey] <- rec
		}
	}
}

/*
method to create the worker channels
*/
func CreateWorkerChannels(numWorkers int) []chan *kgo.Record {
	var channelPool []chan *kgo.Record
	for i := 0; i < numWorkers; i++ {
		// bounded so it doesnt block
		workerChannel := make(chan *kgo.Record, 1000)
		channelPool = append(channelPool, workerChannel)
	}
	return channelPool
}

/*
start the workers listening on those channels. shutdown worker pool on ctx shutdown
*/
func StartWorkerPool(ctx context.Context, channelPool []chan *kgo.Record) {
	for i, workerChannel := range channelPool {
		go func(i int, workerChannel <-chan *kgo.Record) {
			logger.Log.Info("Starting worker.", "worker", i)
			for {
				select {
				case <-ctx.Done():
					logger.Log.Info("Worker stopping.", "worker", i)
					return
				case rec := <-workerChannel:
					if rec == nil {
						continue
					}

					// executes after dispatcher route
					worker.ProcessRecord(ctx, rec)
				}
			}
		}(i, workerChannel)
	}
}
