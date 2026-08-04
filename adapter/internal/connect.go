package internal

import (
	"context"
	"market-adapter/constants"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/gorilla/websocket"
)

var RetryHook func()

func Connect(feed *constants.Feed, streamCfg *constants.Stream, supervisor *constants.Supervisor, isRetry bool) {
	conn, _, err := websocket.DefaultDialer.Dial(feed.Url, nil)
	if err != nil {
		logger.Log.Error("Error when connecting to server. Retry queued", "err", err)
		metrics.Adapter_FeedErrors.WithLabelValues(feed.Name).Inc()

		if isRetry && nil != RetryHook {
			RetryHook()
		}

		if !isRetry {
			supervisor.StatusChan <- constants.StatusBackoff
		}
		return
	}
	defer conn.Close()

	supervisor.StatusChan <- constants.StatusConnected

	var streamHandler *constants.StreamHandler = supervisor.Handler

	err = streamHandler.Subscriber.Subscribe(conn)
	if err != nil {
		logger.Log.Error("Subscription failed for stream with error",
			"feed_name", feed.Name, "stream_channel", streamCfg.Channel, "error", err)
		metrics.Adapter_FeedErrors.WithLabelValues(feed.Name).Inc()
		supervisor.StatusChan <- constants.StatusBackoff
		return
	}

	// reset before wiring pong handler - otherwise a stale LastPongTime will timeout on brand new conn
	supervisor.LastPongTime = time.Now()

	// create a metric to track last pong time
	conn.SetPongHandler(func(appData string) error {
		supervisor.LastPongTime = time.Now()
		metrics.Adapter_LastPongTimes.WithLabelValues(feed.Name).Set(float64(time.Since(supervisor.LastPongTime) * time.Second))
		return nil
	})

	// fix bug: context per attempt, not supervisor ctx. else if cancelling supervisor ctx, feed permanently dies instead of reconnecting
	attemptCtx, attemptCancel := context.WithCancel(supervisor.Ctx)
	defer attemptCancel()
	RegisterResyncCancel(streamCfg.KafkaTopic, attemptCancel)

	// spawn goroutines to handle message reads, heartbeats, monitor
	supervisor.Wg.Add(1)
	go ReadMessages(conn, attemptCtx, attemptCancel, supervisor.Wg, streamHandler.Ring)

	supervisor.Wg.Add(1)
	go PublishToKafkaLoop(supervisor.Wg, feed.Name, streamCfg.Channel, streamCfg.KafkaTopic, attemptCtx, streamHandler.Normalizer, streamHandler.Ring)

	ticker := time.NewTicker(time.Duration(streamCfg.HearbeatInterval) * time.Second)
	defer ticker.Stop()

	supervisor.Wg.Add(1)
	go SendHeartbeat(conn, attemptCtx, attemptCancel, supervisor.Wg, streamHandler, ticker, feed.Name)

	supervisor.Wg.Add(1)
	go MonitorConnection(supervisor, streamCfg, ticker, attemptCtx, attemptCancel)

	// inc metric for supervisor count
	metrics.Adapter_SupervisorCount.Inc()
	// metric for ring cap upon supervisor init
	metrics.Adapter_BufferCapacity.WithLabelValues(feed.Name).Set(float64(streamHandler.Ring.Capacity))

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	supervisor.Wg.Wait()

	logger.Log.Warn("Supervisor backing off.. Queuing retry",
		"name", feed.Name,
		"channel", streamCfg.Channel,
		"url", feed.Url)

	// dec metric for supervisor count
	metrics.Adapter_SupervisorCount.Dec()

	// notify the supervisor its backed off and to retry
	supervisor.StatusChan <- constants.StatusBackoff
}
