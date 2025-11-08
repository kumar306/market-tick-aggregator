package internal

import (
	"market-adapter/constants"
	"market-adapter/logger"
	"market-adapter/metrics"
	"sync"
)

// go routine for the supervisor to keep reading from this status channel and do its logic
func SupervisorLoop(
	feed *constants.Feed,
	stream *constants.Stream,
	supervisor *constants.Supervisor,
	wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case message := <-supervisor.StatusChan:
			switch message {
			case constants.StatusNew:

				Connect(feed, stream, supervisor, false)

			case constants.StatusBackoff:

				Retry(feed, stream, supervisor)

			case constants.StatusConnected:

				logger.Log.Info("Successfully connected to channel",
					"name", feed.Name,
					"channel", stream.Channel,
					"url", feed.Url)

				// prom metrics
				metrics.FeedConnections.WithLabelValues(feed.Name).Inc()

			case constants.StatusTerminated:

				logger.Log.Info("Terminated connection",
					"name", feed.Name,
					"channel", stream.Channel,
					"url", feed.Url)

				return
			}
		case <-supervisor.Ctx.Done():
			logger.Log.Info("Supervisor shutting down",
				"name", feed.Name,
				"channel", stream.Channel,
				"url", feed.Url)
			return
		}
	}
}
