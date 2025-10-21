package main

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// creating a mock setup for a json feed
func main() {

	// have a map of string ("feed key") to its Config struct
	// this struct populated via YAML
	// inject this struct while fetching the config to populate the feed map at startup

	go startSupervisor()
}

// string identifier for status of connection. later modify this to take in select list of statuses (enum)
type Status string

const (
	StatusNew        Status = "New"
	StatusBackoff    Status = "Backoff"
	StatusConnected  Status = "Connected"
	StatusTerminated Status = "Terminated"
)

// use interfaces so no need to write multiple persistence logics for persisting - all under same contract

// obtain lock to write to the feed
var mu sync.Mutex

// url string, times to retry, backoff time,
type Config struct {
	url          string
	maxRetries   int
	backOffTime  int
	mu           sync.Mutex
	statusChan   chan Status
	lastPongTime time.Time
}

// starts the supervisor per feed. each feed has a supervisor to manage everything for a feed
func startSupervisor() {
	// keep trying to acquire the connection - until max retries.
	var maxRetries int = 3 // this should be changed to fetch from config
	// each feed has its configured retry limit and backoff time
	// change it so all the configurable values for a client are taken from one struct

	// pass into spawned goroutines to handle shutdown
	ctx, cancel := context.WithCancel(context.Background())

	// maintain metric counters to observability and use it to trigger things

	var statusChan chan Status = make(chan Status, 1)

	go readChildStatus(statusChan, ctx, cancel, maxRetries)

	statusChan <- StatusNew

}

// go routine for the supervisor to keep reading from this status channel and do its logic
func readChildStatus(ch chan Status, ctx context.Context, cancel context.CancelFunc, maxRetries int) {
	for {
		select {
		case message := <-ch:
			switch message {
			case StatusNew:
				connect("feed url", ch, ctx, cancel)
			case StatusBackoff:
				retry("feed url", ch, ctx, cancel, maxRetries)
			case StatusConnected:
				fmt.Println("Successfully Connected")
			case StatusTerminated:
				fmt.Println("Connection Terminated")
				return
			}
		case <-ctx.Done():
			fmt.Println("Supervisor shutting down")
			return
		}
	}
}

func connect(url string, statusChan chan Status, ctx context.Context, cancel context.CancelFunc) {
	conn, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		fmt.Printf("Error when connecting to server: %v\n", err)
		statusChan <- StatusBackoff // retry connect again
		return
	}
	defer conn.Close()
	statusChan <- StatusConnected

	var lastPongTime time.Time

	conn.SetPongHandler(func(appData string) error {
		fmt.Println("Received pong:", appData)
		lastPongTime = time.Now()
		return nil
	})

	var wg sync.WaitGroup

	// spawn goroutines to handle message reads, heartbeats, monitor
	wg.Add(1)
	go readMessages(conn, ctx, &wg)

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	wg.Add(1)
	go sendHeartbeat(conn, ctx, &wg, ticker)

	wg.Add(1)
	go monitorConnection(conn, ctx, &wg, cancel, ticker)

	// exit this connect only when the goroutines end. if it crosses this point, some connection failure (didnt receive pongs on time)
	wg.Wait()

	// notify the supervisor its backed off and to retry
	statusChan <- StatusBackoff
}

func retry(url string, ch chan Status, ctx context.Context, cancel context.CancelFunc, maxRetries int) {
	for retry := 0; retry < maxRetries; retry++ {
		// exponential backoff and jitter
		delay := time.Duration(float64(time.Second) * math.Pow(2, float64(retry)))
		jitter := time.Duration(rand.Intn(500)) * time.Millisecond
		fmt.Printf("Retry %d in %v\n", retry+1, delay+jitter)
		time.Sleep(delay + jitter)
		connect(url, ch, ctx, cancel)
	}
}

func readMessages(conn *websocket.Conn, ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("Failed to read message: %v\n", err)
			return
		}
	}
}

func sendHeartbeat(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
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

func monitorConnection(conn *websocket.Conn,
	ctx context.Context,
	wg *sync.WaitGroup,
	cancel context.CancelFunc,
	ticker *time.Ticker) {
	defer wg.Done()
	var lastPongTime time.Time
	for {
		select {
		case <-ticker.C:
			mu.Lock()
			if time.Since(lastPongTime) > 10*time.Second {
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
