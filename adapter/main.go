package main

import (
	"context"
	"market-adapter/config"
	"market-adapter/constants"
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

	// stream through config feeds and start supervisors
	for _, feed := range c.Feeds {
		go startSupervisor(feed, ctx, &wg)
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

// starts the supervisor per feed. each feed has a supervisor to manage everything for a feed
func startSupervisor(feedConfig *constants.Feed, parentCtx context.Context, wg *sync.WaitGroup) {
	// keep trying to acquire the connection - until max retries
	// each feed has its configured retry limit and backoff time
	// change it so all the configurable values for a client are taken from one struct

	logger.Log.Info("Starting supervisor for feed",
		"name", feedConfig.Name,
		"format", feedConfig.Format,
		"url", feedConfig.Url)

	// pass into spawned goroutines to handle shutdown
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	// maintain metric counters to observability
	feedConfig.StatusChan = make(chan constants.Status, 5)
	feedConfig.LastPongTime = time.Now()

	// init each feed's ring buffer
	feedConfig.Ring = ring.NewSpscDropOldestRing[[]byte](feedConfig.RingBufferSize, feedConfig.Name)

	feedConfig.StatusChan <- constants.StatusNew

	wg.Add(1)
	go childLoop(feedConfig, ctx, cancel, wg)
}

// go routine for the supervisor to keep reading from this status channel and do its logic
func childLoop(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case message := <-feedConfig.StatusChan:
			switch message {
			case constants.StatusNew:

				connect(feedConfig, ctx, cancel, false)

			case constants.StatusBackoff:

				retry(feedConfig, ctx, cancel)

			case constants.StatusConnected:

				logger.Log.Info("Successfully connected to feed",
					"name", feedConfig.Name,
					"url", feedConfig.Url)

				// prom metrics
				metrics.FeedConnections.WithLabelValues(feedConfig.Name).Inc()

			case constants.StatusTerminated:

				logger.Log.Info("Terminated connection",
					"name", feedConfig.Name,
					"url", feedConfig.Url)
				return
			}
		case <-ctx.Done():
			logger.Log.Info("Supervisor shutting down",
				"name", feedConfig.Name,
				"url", feedConfig.Url)
			return
		}
	}
}

func connect(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc, isRetry bool) {
	logger.Log.Info("Attempting to make connection to feed",
		"name", feedConfig.Name,
		"url", feedConfig.Url)
	conn, _, err := websocket.DefaultDialer.Dial(feedConfig.Url, nil)
	if err != nil {
		logger.Log.Error("Error when connecting to server. Retry queued", "err", err)
		metrics.FeedErrors.WithLabelValues(feedConfig.Name).Inc()
		if !isRetry {
			feedConfig.StatusChan <- constants.StatusBackoff // if fresh retry signal backoff
		}
		return
	}
	defer conn.Close()

	feedConfig.StatusChan <- constants.StatusConnected

	// create a metric to track last pong time
	conn.SetPongHandler(func(appData string) error {
		feedConfig.LastPongTime = time.Now()
		logger.Log.Debug("Received pong",
			"name", feedConfig.Name,
			"url", feedConfig.Url,
			"last_pong_time", feedConfig.LastPongTime)
		metrics.LastPongTimes.WithLabelValues(feedConfig.Name).Set(float64(time.Since(feedConfig.LastPongTime) * time.Second))
		return nil
	})

	// spawn goroutines to handle message reads, heartbeats, monitor
	feedConfig.Wg.Add(1)
	go readMessages(conn, ctx, &feedConfig.Wg, feedConfig.Ring)

	feedConfig.Wg.Add(1)
	go publishToKafkaLoop(feedConfig, ctx)

	ticker := time.NewTicker(time.Duration(feedConfig.HearbeatInterval) * time.Second)
	defer ticker.Stop()

	feedConfig.Wg.Add(1)
	go sendHeartbeat(conn, ctx, &feedConfig.Wg, &feedConfig.Mu, ticker, feedConfig.Name)

	feedConfig.Wg.Add(1)
	go monitorConnection(ctx, feedConfig, cancel, ticker, feedConfig.Name)

	logger.Log.Info("Started the supervisor loops for feed after establishing connection",
		"name", feedConfig.Name,
		"url", feedConfig.Url,
		"heartbeat_interval", feedConfig.HearbeatInterval,
		"pong_timeout", feedConfig.PongTimeout)

	// inc metric for supervisor count
	metrics.Supervisors.Inc()
	// metric for ring cap upon supervisor init
	metrics.BufferCapacity.WithLabelValues(feedConfig.Name).Set(float64(feedConfig.Ring.Capacity))

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	feedConfig.Wg.Wait()

	logger.Log.Warn("Supervisor backing off.. Queuing retry",
		"name", feedConfig.Name,
		"url", feedConfig.Url)

	// dec metric for supervisor count
	metrics.Supervisors.Dec()

	// notify the supervisor its backed off and to retry
	feedConfig.StatusChan <- constants.StatusBackoff
}

func retry(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	for retry := 0; retry < feedConfig.MaxRetries; retry++ {
		select {
		case <-ctx.Done():
			logger.Log.Info("Stopping retry for feed", "name", feedConfig.Name)
			return
		default:
			// exponential backoff and jitter
			baseDelay := time.Duration(feedConfig.BaseDelay) * time.Second
			delay := baseDelay * time.Duration(math.Pow(2, float64(retry)))
			jitter := time.Duration(rand.Intn(feedConfig.MaxJitterMillis)) * time.Millisecond
			logger.Log.Warn("Retrying feed connection",
				"max_retries", feedConfig.MaxRetries,
				"retry_attempt", retry+1,
				"retries_left", feedConfig.MaxRetries-retry-1,
				"delay", delay+jitter)
			time.Sleep(delay + jitter)
			connect(feedConfig, ctx, cancel, true)
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
			} else {
				ring.Push(msg)
				logger.Log.Info("Received message for feed", "name", name, "msg", string(msg))
			}
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
	ctx context.Context,
	feedConfig *constants.Feed,
	cancel context.CancelFunc,
	ticker *time.Ticker,
	name string) {
	metrics.SupervisorGoroutines.WithLabelValues(name).Inc()
	defer feedConfig.Wg.Done()
	defer metrics.SupervisorGoroutines.WithLabelValues(name).Dec()
	for {
		select {
		case <-ticker.C:
			if time.Since(feedConfig.LastPongTime) > time.Duration(feedConfig.PongTimeout)*time.Second {
				logger.Log.Warn("Pong timeout -- cancelling the connection", "name", name)
				cancel()
				return
			}

		case <-ctx.Done():
			logger.Log.Info("Shutting down monitor loop", "name", name)
			return
		}
	}
}

func publishToKafkaLoop(feed *constants.Feed, ctx context.Context) {
	defer feed.Wg.Done()
	metrics.SupervisorGoroutines.WithLabelValues(feed.Name).Inc()
	defer metrics.SupervisorGoroutines.WithLabelValues(feed.Name).Dec()
	for {
		select {
		case <-ctx.Done():
			logger.Log.Info("Shutting down kafka publish loop for feed " + feed.Name)
			return
		default:
			// read from ring buffer
			msg, ok := feed.Ring.Pop()
			if !ok {
				// empty buffer case
				time.Sleep(1 * time.Millisecond)
				continue
			}

			// publish to kafka
			kafka.ProduceAsync(feed.KafkaTopic, feed.Name, ctx, msg)
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
