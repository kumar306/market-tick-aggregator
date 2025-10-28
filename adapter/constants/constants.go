package constants

import (
	"market-adapter/ring"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// string identifier for status of connection. later modify this to take in select list of statuses (enum)
type Status string
type FormatType string

const (
	StatusNew        Status = "New"
	StatusBackoff    Status = "Backoff"
	StatusConnected  Status = "Connected"
	StatusTerminated Status = "Terminated"
)

const (
	FormatJson FormatType = "json"
	FormatCsv  FormatType = "csv"
	FormatFix  FormatType = "fix"
)

// use interfaces so no need to write multiple persistence logics for persisting - all under same contract

type Config struct {
	Feeds            []*Feed `yaml:"feeds"`
	FeedMap          map[string]*Feed
	BootstrapServers []string `yaml:"bootstrap_servers"`
}

type Feed struct {
	Name             string `yaml:"name"`
	Url              string `yaml:"url"`
	MaxRetries       int    `yaml:"maxRetries"`
	BaseDelay        int    `yaml:"baseDelay"`
	HearbeatInterval int    `yaml:"heartbeatInterval"`
	PongTimeout      int    `yaml:"pongTimeout"`
	KafkaTopic       string `yaml:"kafkaTopic"`
	RingBufferSize   uint64 `yaml:"ringBufferSize"`
	MaxJitterMillis  int
	Mu               sync.Mutex
	Wg               sync.WaitGroup
	StatusChan       chan Status
	LastPongTime     time.Time
	Ring             *ring.SpscDropOldestRing[[]byte]
	Normalizer       Normalizer
	Subscriber       Subscriber
	Pinger           Pinger
}

const ConfigFile string = "market-adapter/config/config.yaml"

// normalize different kind of messages for different exchanges
type Normalizer interface {
	Normalize(raw []byte) (symbol string, normalized []byte, err error)
}

type Subscriber interface {
	Subscribe(conn *websocket.Conn) error
}

type Pinger interface {
	Ping(conn *websocket.Conn) error
}

type AuthHandler interface {
	HandleAuth() error
}
