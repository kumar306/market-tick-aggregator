package main

import (
	"context"
	"market-adapter/config"
	"market-adapter/constants"
	feedFactory "market-adapter/feeds"
	"market-adapter/feeds/binance"
	"market-adapter/kafka"
	"market-adapter/logger"
	"market-adapter/metrics"
	"market-adapter/ring"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// start feeds
func main() {

	// register the feeds
	feedFactory.RegisterFeedFactory("binance", &binance.BinanceFactory{})

	// init and expose prometheus metrics
	metrics.Init()
	// inc app starts metric
	metrics.AppStarts.Inc()

	go exposeMetrics()

	// have a map of string ("feed key") to its Config struct
	var c *constants.Config
	c, err := config.GetConfig()
	// If any validation errors, return
	if err != nil {
		logger.Log.Error("Failed to load feed config. Stopping main()", "err", err)
		os.Exit(1)
	}

	c.FeedMap = make(map[string]*constants.Feed)

	// handle shutdown on SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// kafka setup
	kafka.Init(c.BootstrapServers)
	defer kafka.Close()

	// using this to coordinate to shutdown the supervisor goroutines
	var wg sync.WaitGroup

	// stream through config streams and start supervisors
	for _, feed := range c.Feeds {
		for _, stream := range feed.Streams {

			// create supervisor and get the stream handler
			handler, handlerErr := feedFactory.GetStreamHandler(feed.Name, stream)
			if handlerErr != nil {
				logger.Log.Error("Error occurred when retrieving stream handler for stream", handlerErr, "name", feed.Name)
				metrics.FeedErrors.WithLabelValues("feed_name", feed.Name).Inc()
				continue
			}

			// each supervisor has an internal waitgroup to manage its child goroutines - read, heartbeat, monitor
			supervisor := &constants.Supervisor{
				Wg:           &sync.WaitGroup{},
				Handler:      handler,
				StatusChan:   make(chan constants.Status, 3),
				LastPongTime: time.Now(),
			}

			go startSupervisor(supervisor, feed, stream, ctx, &wg)
		}
		c.FeedMap[feed.Name] = feed
	}

	logger.Log.Info("Supervisors startup execution completed")

	// blocks until SIGTERM
	<-ctx.Done()
	logger.Log.Info("Received termination signal. Stopping supervisors and its processes gracefully.")
	wg.Wait()
	logger.Log.Info("Stopped all supervisors and its processes")

	// inc app shutdown metric
	metrics.AppShutdowns.Inc()
}

// starts the supervisor per stream. each stream has a supervisor to manage everything
func startSupervisor(supervisor *constants.Supervisor,
	feed *constants.Feed,
	stream *constants.Stream,
	parentCtx context.Context,
	wg *sync.WaitGroup) {

	// keep trying to acquire the connection - until max retries
	// each feed has its configured retry limit and backoff time

	logger.Log.Info("Starting supervisor for stream",
		"name", feed.Name,
		"url", feed.Url)

	// pass into spawned goroutines to handle shutdown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	supervisor.Ctx = ctx
	supervisor.Cancel = cancel
	supervisor.StatusChan <- constants.StatusNew

	wg.Add(1)
	go childLoop(feed, stream, supervisor, wg)
}

// go routine for the supervisor to keep reading from this status channel and do its logic
func childLoop(
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

				connect(feed, stream, supervisor, false)

			case constants.StatusBackoff:

				retry(feed, stream, supervisor)

			case constants.StatusConnected:

				logger.Log.Info("Successfully connected to feed",
					"name", feed.Name,
					"url", feed.Url)

				// prom metrics
				metrics.FeedConnections.WithLabelValues(feed.Name).Inc()

			case constants.StatusTerminated:

				logger.Log.Info("Terminated connection",
					"name", feed.Name,
					"url", feed.Url)
				return
			}
		case <-supervisor.Ctx.Done():
			logger.Log.Info("Supervisor shutting down",
				"name", feed.Name,
				"url", feed.Url)
			return
		}
	}
}

func connect(feed *constants.Feed, streamCfg *constants.Stream, supervisor *constants.Supervisor, isRetry bool) {
	logger.Log.Info("Attempting to make connection to feed",
		"name", feed.Name,
		"url", feed.Url)
	conn, _, err := websocket.DefaultDialer.Dial(feed.Url, nil)
	if err != nil {
		logger.Log.Error("Error when connecting to server. Retry queued", "err", err)
		metrics.FeedErrors.WithLabelValues(feed.Name).Inc()
		if !isRetry {
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
	go readMessages(conn, supervisor.Ctx, supervisor.Wg, streamHandler.Ring)

	supervisor.Wg.Add(1)
	go publishToKafkaLoop(supervisor.Wg, feed.Name, streamCfg.KafkaTopic, supervisor.Ctx, streamHandler.Normalizer, streamHandler.Ring)

	ticker := time.NewTicker(time.Duration(streamCfg.HearbeatInterval) * time.Second)
	defer ticker.Stop()

	supervisor.Wg.Add(1)
	go sendHeartbeat(conn, supervisor.Ctx, supervisor.Wg, streamHandler.Mu, ticker, feed.Name)

	supervisor.Wg.Add(1)
	go monitorConnection(supervisor, streamCfg, ticker)

	logger.Log.Info("Started the supervisor loops for feed after establishing connection",
		"name", feed.Name,
		"url", feed.Url,
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
		"url", feed.Url)

	// dec metric for supervisor count
	metrics.Supervisors.Dec()

	// notify the supervisor its backed off and to retry
	supervisor.StatusChan <- constants.StatusBackoff
}

func retry(feed *constants.Feed, streamCfg *constants.Stream, supervisor *constants.Supervisor) {
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
			connect(feed, streamCfg, supervisor, true)
		}
	}
}

func readMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup, ring *ring.SpscDropOldestRing[[]byte]) {
	name := ring.Name
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Closing read loop. Returning", "name", name)
			return
		default:
			_, msg, err := conn.ReadMessage()
			if err != nil {
				logger.Log.Error("Failed to read message for feed", "name", name, "err", err)
				continue
			}

			ring.Push(msg)
			logger.Log.Info("Received message for feed", "name", name, "msg", string(msg))

		}
	}
}

func sendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	ticker *time.Ticker,
	name string) {
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second))
			mu.Unlock()
			if err != nil {
				logger.Log.Warn("Failed to send a ping message for feed", "name", name)
				return
			}

		case <-ctx.Done():
			logger.Log.Info("Shutting down heartbeat loop. Returning", "name", name)
			return
		}
	}
}

func monitorConnection(
	supervisor *constants.Supervisor,
	streamCfg *constants.Stream,
	ticker *time.Ticker) {
	metrics.SupervisorGoroutines.WithLabelValues(streamCfg.Name).Inc()
	defer supervisor.Wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(streamCfg.Name).Dec()
	for {
		select {
		case <-ticker.C:
			if time.Since(supervisor.LastPongTime) > time.Duration(streamCfg.PongTimeout)*time.Second {
				logger.Log.Warn("Pong timeout -- cancelling the connection", "name", streamCfg.Name)
				supervisor.Cancel()
				return
			}

		case <-supervisor.Ctx.Done():
			logger.Log.Info("Shutting down monitor loop", "name", streamCfg.Name)
			return
		}
	}
}

func publishToKafkaLoop(wg *sync.WaitGroup,
	name string,
	kafkaTopic string,
	ctx context.Context,
	normalizer constants.Normalizer,
	ring *ring.SpscDropOldestRing[[]byte]) {
	defer wg.Done()
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Shutting down kafka publish loop", "name", name)
			return
		default:
			// read from ring buffer
			msg, ok := ring.Pop()
			if !ok {
				// empty buffer case
				time.Sleep(1 * time.Millisecond)
				continue
			}

			// normalize after reading from ring buffer
			symbol, normalized, normalizeErr := normalizer.Normalize(msg)
			if normalizeErr != nil {
				logger.Log.Error("Failed to normalize message for feed", "name", name, "err", normalizeErr)
				continue
			}

			// publish to kafka
			kafka.ProduceAsync(kafkaTopic, name, ctx, symbol, normalized)
		}
	}
}

func exposeMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	logger.Log.Info("Exposed metrics endpoint at 2112", "url", ":2112/metrics")
	err := http.ListenAndServe(":2112", nil)
	if err != nil {
		logger.Log.Error("Metrics have stopped", "err", err)
	}
}
