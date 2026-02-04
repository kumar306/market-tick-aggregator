package flush

import (
	"context"
	"market-orderbook/constants"
	"market-orderbook/kafka"
	"shared/logger"
	"time"
)

var Epoch int32 = 0

func RunEpochFlushScheduler(ctx context.Context, flushInterval int,
	workerChannels []chan *constants.DispatchRecord,
	coordinator *kafka.CommitCoordinator) {

	// every X seconds, i will call the next epoch for flush
	// it should inc the epoch
	// broadcast this epoch to each worker channel
	// create new epoch state in coordinator
	ticker := time.NewTicker(time.Duration(flushInterval) * time.Second)

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received ctx done on epoch flush scheduler. Returning")
			return
		case <-ticker.C:
			{
				logger.Log.Info("Received ticker event in epoch flush scheduler. Moving to next epoch", "nextEpoch", Epoch+1)
				Epoch++
				participants := make(map[int]struct{})
				for id, ch := range workerChannels {
					select {
					case ch <- &constants.DispatchRecord{
						Event:      constants.FlushEvent,
						FlushEpoch: Epoch,
					}:
						participants[id] = struct{}{}
					default:
						// prom metric to drop flush
					}
				}

				// call coordinator to start epoch
				coordinator.StartEpoch(Epoch, participants)
			}
		}
	}
}
