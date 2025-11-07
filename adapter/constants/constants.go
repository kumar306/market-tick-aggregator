package constants

import (
	"context"
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
	Feeds            []*Feed `yaml:"feeds" json:"feeds"`
	FeedMap          map[string]*Feed
	BootstrapServers []string `yaml:"bootstrap_servers" json:"bootstrap_servers"`
}

type Feed struct {
	Name    string    `yaml:"name" json:"name"`
	Url     string    `yaml:"url" json:"url"`
	Streams []*Stream `yaml:"streams" json:"streams"`
}

type Stream struct {
	Name             string   `yaml:"name" json:"name"`
	Channel          string   `yaml:"channel" json:"channel"`
	ProductIds       []string `yaml:"productIds" json:"productIds"`
	KafkaTopic       string   `yaml:"kafkaTopic" json:"kafkaTopic"`
	RingBufferSize   uint64   `yaml:"ringBufferSize" json:"ringBufferSize"`
	MaxRetries       int      `yaml:"maxRetries" json:"maxRetries"`
	BaseDelay        int      `yaml:"baseDelay" json:"baseDelay"`
	HearbeatInterval int      `yaml:"heartbeatInterval" json:"heartbeatInterval"`
	PongTimeout      int      `yaml:"pongTimeout" json:"pongTimeout"`
	MaxJitterMillis  int      `yaml:"maxJitterMillis" json:"maxJitterMillis"`
}

type Supervisor struct {
	Wg           *sync.WaitGroup
	Ctx          context.Context
	Cancel       context.CancelFunc
	StatusChan   chan Status
	LastPongTime time.Time
	Handler      *StreamHandler
}

type StreamHandler struct {
	Mu         *sync.Mutex
	Normalizer Normalizer
	Subscriber Subscriber
	Pinger     Pinger
	Ring       *ring.SpscDropOldestRing[[]byte]
}

const ConfigFile string = "./config/config.yaml"

// normalize different kind of messages for different exchanges
type Normalizer interface {
	Normalize(raw []byte) (symbol []byte, normalized []byte, err error)
}

type Subscriber interface {
	Subscribe(conn *websocket.Conn) error
}

type Pinger interface {
	Ping(conn *websocket.Conn, mu *sync.Mutex) error
}

type AuthHandler interface {
	HandleAuth() error
}
