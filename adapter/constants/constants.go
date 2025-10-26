package constants

import (
	"market-adapter/ring"
	"sync"
	"time"
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
	Name             string     `yaml:"name"`
	Url              string     `yaml:"url"`
	Format           FormatType `yaml:"format"`
	MaxRetries       int        `yaml:"maxRetries"`
	BaseDelay        int        `yaml:"baseDelay"`
	MaxJitterMillis  int        `yaml:"maxJitterMillis"`
	HearbeatInterval int        `yaml:"heartbeatInterval"`
	PongTimeout      int        `yaml:"pongTimeout"`
	KafkaTopic       string     `yaml:"kafkaTopic"`
	KafkaBatchSize   int        `yaml:"kafkaBatchSize"`
	RingBufferSize   uint64     `yaml:"ringBufferSize"`
	Mu               sync.Mutex
	Wg               sync.WaitGroup
	StatusChan       chan Status
	LastPongTime     time.Time
	Ring             *ring.SpscDropOldestRing[[]byte]
}

const ConfigFile string = "market-adapter/config/config.yaml"
