package internal

import (
	"math"
	"math/rand"
	"shared/constants"
	"shared/logger"
	"time"
)

func Retry(feed *constants.Feed, streamCfg *constants.Stream, supervisor *constants.Supervisor) {
	for retry := 0; retry < streamCfg.MaxRetries; retry++ {
		select {
		case <-supervisor.Ctx.Done():
			logger.Log.Info("Stopping retry for feed", "name", streamCfg.Name)
			return
		default:
			// exponential backoff and jitter
			baseDelay := time.Duration(streamCfg.BaseDelay) * time.Second
			delay := baseDelay * time.Duration(math.Pow(2, float64(retry)))
			jitter := time.Duration(rand.Intn(streamCfg.MaxJitterMillis)) * time.Millisecond
			logger.Log.Warn("Retrying feed connection",
				"max_retries", streamCfg.MaxRetries,
				"retry_attempt", retry+1,
				"retries_left", streamCfg.MaxRetries-retry-1,
				"delay", delay+jitter)
			time.Sleep(delay + jitter)
			Connect(feed, streamCfg, supervisor, true)
		}
	}

	supervisor.StatusChan <- constants.StatusTerminated
}
