package flush

import (
	"context"
	"market-aggregator/constants"
	"time"
)

func StartFlushSchedulers(ctx context.Context, workerChannels []chan *constants.DispatchRecord, cfgs []*constants.WindowConfig) {
	for _, cfg := range cfgs {
		go startFlushScheduler(ctx, workerChannels, cfg)
	}
}

func startFlushScheduler(ctx context.Context, workerChannels []chan *constants.DispatchRecord, cfg *constants.WindowConfig) {
	ticker := time.NewTicker(time.Duration(cfg.FlushCadencyMs) * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			flushTs := t.UnixMilli()
			event := &constants.DispatchRecord{
				Event:        constants.FlushEvent,
				WindowConfig: cfg,
				FlushTsMs:    flushTs,
			}

			// so even if channel blocked, i'll drop the record as metrics anyway converge eventually
			for _, ch := range workerChannels {
				select {
				case ch <- event:
				default:
				}
			}
		}
	}
}
