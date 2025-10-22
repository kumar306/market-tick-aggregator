package main

import (
	"context"
	"market-adapter/config"
	"market-adapter/constants"
	"market-adapter/logger"
	"math"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// start feeds
func main() {

	// have a map of string ("feed key") to its Config struct
	var c *constants.Config
	c, err := config.GetConfig()
	// If any validation errors, return
	if err != nil {
		logger.Log.Error("Failed to load feed config. Stopping main()", "err", err)
		os.Exit(1)
	}

	c.FeedMap = make(map[string]*constants.Feed)

	// now we have the array of feed configs
	// stream through its feeds and start supervisors
	for _, feed := range c.Feeds {
		go startSupervisor(feed)
		c.FeedMap[feed.Name] = feed
	}

	logger.Log.Info("Supervisors startup execution completed")
}

// starts the supervisor per feed. each feed has a supervisor to manage everything for a feed
func startSupervisor(feedConfig *constants.Feed) {
	// keep trying to acquire the connection - until max retries
	// each feed has its configured retry limit and backoff time
	// change it so all the configurable values for a client are taken from one struct

	logger.Log.Info("Starting supervisor for feed",
		"name", feedConfig.Name,
		"format", feedConfig.Format,
		"url", feedConfig.Url)

	// pass into spawned goroutines to handle shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// maintain metric counters to observability and use it to trigger things
	feedConfig.StatusChan = make(chan constants.Status, 1)
	feedConfig.LastPongTime = time.Now()

	go readChildStatus(feedConfig, ctx, cancel)

	feedConfig.StatusChan <- constants.StatusNew

}

// go routine for the supervisor to keep reading from this status channel and do its logic
func readChildStatus(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	for {
		select {
		case message := <-feedConfig.StatusChan:
			switch message {
			case constants.StatusNew:
				connect(feedConfig, ctx, cancel)
			case constants.StatusBackoff:
				retry(feedConfig, ctx, cancel)
			case constants.StatusConnected:

				logger.Log.Info("Successfully connected to feed",
					"name", feedConfig.Name,
					"url", feedConfig.Url)

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

func connect(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	logger.Log.Info("Attempting to make connection to feed",
		"name", feedConfig.Name,
		"url", feedConfig.Url)
	conn, _, err := websocket.DefaultDialer.Dial(feedConfig.Url, nil)
	if err != nil {
		logger.Log.Error("Error when connecting to server. Retry queued", "err", err)
		feedConfig.StatusChan <- constants.StatusBackoff // retry connect again
		return
	}
	defer conn.Close()
	feedConfig.StatusChan <- constants.StatusConnected

	conn.SetPongHandler(func(appData string) error {
		feedConfig.LastPongTime = time.Now()
		logger.Log.Debug("Received pong",
			"name", feedConfig.Name,
			"url", feedConfig.Url,
			"last_pong_time", feedConfig.LastPongTime)
		return nil
	})

	// spawn goroutines to handle message reads, heartbeats, monitor
	feedConfig.Wg.Add(1)
	go readMessages(conn, ctx, &feedConfig.Wg, feedConfig.Name)

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

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	feedConfig.Wg.Wait()

	logger.Log.Warn("Supervisor backing off.. Queuing retry",
		"name", feedConfig.Name,
		"url", feedConfig.Url)

	// notify the supervisor its backed off and to retry
	feedConfig.StatusChan <- constants.StatusBackoff
}

func retry(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	for retry := 0; retry < feedConfig.MaxRetries; retry++ {
		// exponential backoff and jitter
		delay := time.Duration(float64(feedConfig.BaseDelay) + float64(time.Second)*math.Pow(2, float64(retry)))
		jitter := time.Duration(rand.Intn(feedConfig.MaxJitterMillis)) * time.Millisecond
		logger.Log.Warn("Retrying feed connection",
			"max_retries", feedConfig.MaxRetries,
			"retry_attempt", retry+1,
			"retries_left", feedConfig.MaxRetries-retry-1,
			"delay", delay+jitter)
		time.Sleep(delay + jitter)
		connect(feedConfig, ctx, cancel)
	}
}

func readMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup, name string) {
	defer wg.Done()
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
	defer wg.Done()
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
	defer feedConfig.Wg.Done()
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
