package internal

import (
	"market-adapter/metrics"
	"shared/constants"
	"shared/logger"
	"time"

	"github.com/gorilla/websocket"
)

// require a logging mechanism for unit testing exponential backoff time
var RetryHook func()

func Connect(feed *constants.Feed, streamCfg *constants.Stream, supervisor *constants.Supervisor, isRetry bool) {
	logger.Log.Info("Attempting to make connection to feed",
		"name", feed.Name,
		"channel", streamCfg.Channel,
		"url", feed.Url)
	conn, _, err := websocket.DefaultDialer.Dial(feed.Url, nil)
	if err != nil {
		logger.Log.Error("Error when connecting to server. Retry queued", "err", err)
		metrics.FeedErrors.WithLabelValues(feed.Name).Inc()

		// hook for plugging in during testing
		if isRetry && nil != RetryHook {
			RetryHook()
		}

		if !isRetry {
			logger.Log.Info("Supervisor backing off after queueing retry", "feed", feed.Name, "channel", streamCfg.Channel)
			supervisor.StatusChan <- constants.StatusBackoff // if fresh retry signal backoff
		}
		return
	}
	defer conn.Close()

	supervisor.StatusChan <- constants.StatusConnected

	var streamHandler *constants.StreamHandler = supervisor.Handler

	// subscribe to the stream after making the connection
	err = streamHandler.Subscriber.Subscribe(conn)
	if err != nil {
		logger.Log.Error("Subscription failed for stream with error",
			"feed_name", feed.Name, "stream_channel", streamCfg.Channel, "error", err)
		metrics.FeedErrors.WithLabelValues(feed.Name).Inc()
		supervisor.StatusChan <- constants.StatusBackoff
		return
	}

	// create a metric to track last pong time
	conn.SetPongHandler(func(appData string) error {
		supervisor.LastPongTime = time.Now()
		logger.Log.Debug("Received pong",
			"name", feed.Name,
			"url", feed.Url,
			"last_pong_time", supervisor.LastPongTime)
		metrics.LastPongTimes.WithLabelValues(feed.Name).Set(float64(time.Since(supervisor.LastPongTime) * time.Second))
		return nil
	})

	// spawn goroutines to handle message reads, heartbeats, monitor
	supervisor.Wg.Add(1)
	go ReadMessages(conn, supervisor.Ctx, supervisor.Wg, streamHandler.Ring)

	supervisor.Wg.Add(1)
	go PublishToKafkaLoop(supervisor.Wg, feed.Name, streamCfg.Channel, streamCfg.KafkaTopic, supervisor.Ctx, streamHandler.Normalizer, streamHandler.Ring)

	ticker := time.NewTicker(time.Duration(streamCfg.HearbeatInterval) * time.Second)
	defer ticker.Stop()

	supervisor.Wg.Add(1)
	go SendHeartbeat(conn, supervisor.Ctx, supervisor.Wg, streamHandler, ticker, feed.Name)

	supervisor.Wg.Add(1)
	go MonitorConnection(supervisor, streamCfg, ticker)

	logger.Log.Info("Started the supervisor loops for feed after establishing connection",
		"name", feed.Name,
		"url", feed.Url,
		"channel", streamCfg.Channel,
		"heartbeat_interval", streamCfg.HearbeatInterval,
		"pong_timeout", streamCfg.PongTimeout)

	// inc metric for supervisor count
	metrics.Supervisors.Inc()
	// metric for ring cap upon supervisor init
	metrics.BufferCapacity.WithLabelValues(feed.Name).Set(float64(streamHandler.Ring.Capacity))

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	supervisor.Wg.Wait()

	logger.Log.Warn("Supervisor backing off.. Queuing retry",
		"name", feed.Name,
		"channel", streamCfg.Channel,
		"url", feed.Url)

	// dec metric for supervisor count
	metrics.Supervisors.Dec()

	// notify the supervisor its backed off and to retry
	supervisor.StatusChan <- constants.StatusBackoff
}
