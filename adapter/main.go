package main

import (
	"context"
	"fmt"
	"log"
	"market-adapter/config"
	"market-adapter/constants"
	"math"
	"math/rand"
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
		log.Fatalf("Failed to load config: %v", err)
	}

	c.FeedMap = make(map[string]*constants.Feed)

	// now we have the array of feed configs
	// stream through its feeds and start supervisors
	for _, feed := range c.Feeds {
		go startSupervisor(feed)
	}
}

// starts the supervisor per feed. each feed has a supervisor to manage everything for a feed
func startSupervisor(feedConfig *constants.Feed) {
	// keep trying to acquire the connection - until max retries
	// each feed has its configured retry limit and backoff time
	// change it so all the configurable values for a client are taken from one struct

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
				fmt.Println("Successfully Connected")
			case constants.StatusTerminated:
				fmt.Println("Connection Terminated")
				return
			}
		case <-ctx.Done():
			fmt.Println("Supervisor shutting down")
			return
		}
	}
}

func connect(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	conn, _, err := websocket.DefaultDialer.Dial(feedConfig.Url, nil)
	if err != nil {
		fmt.Printf("Error when connecting to server: %v\n", err)
		feedConfig.StatusChan <- constants.StatusBackoff // retry connect again
		return
	}
	defer conn.Close()
	feedConfig.StatusChan <- constants.StatusConnected

	conn.SetPongHandler(func(appData string) error {
		fmt.Println("Received pong:", appData)
		feedConfig.LastPongTime = time.Now()
		return nil
	})

	// spawn goroutines to handle message reads, heartbeats, monitor
	feedConfig.Wg.Add(1)
	go readMessages(conn, ctx, &feedConfig.Wg)

	ticker := time.NewTicker(time.Duration(feedConfig.HearbeatInterval) * time.Second)
	defer ticker.Stop()

	feedConfig.Wg.Add(1)
	go sendHeartbeat(conn, ctx, &feedConfig.Wg, &feedConfig.Mu, ticker)

	feedConfig.Wg.Add(1)
	go monitorConnection(ctx, feedConfig, cancel, ticker)

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	feedConfig.Wg.Wait()

	// notify the supervisor its backed off and to retry
	feedConfig.StatusChan <- constants.StatusBackoff
}

func retry(feedConfig *constants.Feed, ctx context.Context, cancel context.CancelFunc) {
	for retry := 0; retry < feedConfig.MaxRetries; retry++ {
		// exponential backoff and jitter
		delay := time.Duration(float64(feedConfig.BaseDelay) + float64(time.Second)*math.Pow(2, float64(retry)))
		jitter := time.Duration(rand.Intn(feedConfig.MaxJitterMillis)) * time.Millisecond
		fmt.Printf("Retry %d in %v\n", retry+1, delay+jitter)
		time.Sleep(delay + jitter)
		connect(feedConfig, ctx, cancel)
	}
}

func readMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Closing read loop. Returning")
			return
		default:
			_, _, err := conn.ReadMessage()
			if err != nil {
				fmt.Printf("Failed to read message: %v\n", err)
				return
			}
		}
	}
}

func sendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
	mu *sync.Mutex,
	ticker *time.Ticker) {
	defer wg.Done()
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			err := conn.WriteControl(websocket.PingMessage, []byte{}, time.Now().Add(5*time.Second))
			mu.Unlock()
			if err != nil {
				fmt.Println("Failed to send a ping message")
				return
			}

		case <-ctx.Done():
			fmt.Println("Shutting down heartbeat loop")
			return
		}
	}
}

func monitorConnection(
	ctx context.Context,
	feedConfig *constants.Feed,
	cancel context.CancelFunc,
	ticker *time.Ticker) {
	defer feedConfig.Wg.Done()
	for {
		select {
		case <-ticker.C:
			if time.Since(feedConfig.LastPongTime) > time.Duration(feedConfig.PongTimeout)*time.Second {
				fmt.Println("Pong timeout -- cancelling the connection")
				cancel()
				return
			}

		case <-ctx.Done():
			fmt.Println("Shutting down monitor loop")
			return
		}
	}
}
