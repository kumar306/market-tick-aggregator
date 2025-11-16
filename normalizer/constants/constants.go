package constants

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	KafkaConfig *KafkaConfig `yaml:"kafka"`
	WorkerCount int          `yaml:"worker_count"`
}

type KafkaConfig struct {
	Brokers                    []string `yaml:"brokers"`
	Topics                     []string `yaml:"topics"`
	ConsumerGroup              string   `yaml:"consumer_group"`
	MaxBufferRecords           int      `yaml:"max_buffer_records"`
	CommitOffsetIntervalMillis int      `yaml:"commit_offset_interval_ms"`
}

type Header struct {
	Exchange string `json:"exchange"`
	Channel  string `json:"channel"`
}

// to handle new messages and buffer flushes in a single threaded channe;
type EventType int

const (
	NewMessage EventType = iota
	FlushBuffer
)

type DispatchRecord struct {
	Event     EventType
	Record    *kgo.Record
	BufferKey string
	ShardKey  uint32
	Exchange  string
	Channel   string
	Symbol    string
}

const (
	ConfigFilePath           string = "./config/config.yaml"
	DefaultConsumerGroupName string = "normalizer-group-1"
	Binance                  string = "binance"
	Coinbase                 string = "coinbase"
	Kraken                   string = "kraken"
	AggTrade                 string = "aggTrade"
	Ticker                   string = "ticker"
	Book                     string = "book"
	Depth                    string = "depth"
	Level2                   string = "level2"
	NormalizedTickerTopic    string = "normalized.ticker"
	NormalizedBookTopic      string = "normalized.book"
)

// worker state per product id
type SymbolState struct {
	// seq or ts ordering
	Orderer OrdererStrategy

	// seq ordering
	LastSeqId int64
	// buffer map of sequence id to record
	Buffer    []*PipelineMessage
	Gap       time.Timer
	GapActive bool

	Converter  ConverterStrategy
	Normalizer NormalizerStrategy
	Publisher  PublisherStrategy

	LastSeenTs time.Time
}

// uniform message type in pipeline
type PipelineMessage struct {
	Exchange   string
	Channel    string
	Symbol     string
	SeqId      int64
	Ts         string
	RawMessage interface{}
}

// converts the byte stream into the respective struct and returns it
type ConverterStrategy interface {
	Convert([]byte) (*PipelineMessage, error)
}

// orders the stream of messages
type OrdererStrategy interface {
	Order(*PipelineMessage, string, chan *DispatchRecord) ([]*PipelineMessage, error)
	InitOrdererState(*PipelineMessage)
	// comparator sort the buffer in order before flushing
	Less(i, j *PipelineMessage) bool
}

// publishes to downstream topic based on channel type
type PublisherStrategy interface {
	Publish(raw, partitionKey []byte, exchange, channel string)
	// fetch its publish topic
	PublishTopic() string
}

// converts the raw ordered message to normalized protobuf byte stream
type NormalizerStrategy interface {
	Normalize(msg *PipelineMessage) ([]byte, error)
}

type BinanceAggTradeMsg struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type BinanceDepthUpdateMsg struct {
	EventType       string     `json:"e"` // depthUpdate
	EventTime       int64      `json:"E"`
	TransactionTime int64      `json:"T"`
	Symbol          string     `json:"s"`
	FirstUpdateID   int64      `json:"U"` // first update ID
	FinalUpdateID   int64      `json:"u"` // final update ID
	PrevFinalID     int64      `json:"pu"`
	Bids            [][]string `json:"b"`
	Asks            [][]string `json:"a"`
}
