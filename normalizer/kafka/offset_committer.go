package kafka

import (
	"context"
	"shared/logger"
	"time"
)

// start a ticker which emits ticker event every X milliseconds.
// upon every event, commit offsets to kafka
func OffsetCommitter(ctx context.Context, gapMillis int) {
	var ticker *time.Ticker = time.NewTicker(time.Duration(gapMillis) * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Received context done.. shutting down kafka commit loop")
			if err := client.CommitMarkedOffsets(context.Background()); err != nil {
				logger.Log.Error("Final commit failed", "error", err)
			}
			return
		case <-ticker.C:
			err := client.CommitMarkedOffsets(context.Background())
			if err != nil {
				logger.Log.Error("CommitMarkedOffsets failed", "error", err)
			} else {
				logger.Log.Info("CommitMarkedOffsets ok")
			}
		}
	}
}
