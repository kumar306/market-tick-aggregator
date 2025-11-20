package constants

import (
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	KafkaConfig     *KafkaConfig `yaml:"kafka"`
	WorkerCount     int          `yaml:"worker_count"`
	RedisTtlMinutes int          `yaml:"redis_ttl_minutes"`
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
	BufferSeqMap map[int64]*PipelineMessage
	BufferSeqId  []int64

	// time ordering
	BufferTimeMap map[int64][]*PipelineMessage
	BufferTime    []int64

	Gap       *time.Timer
	GapActive bool

	// pipeline
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
	Record     *kgo.Record
}

// converts the byte stream into the respective struct and returns it
type ConverterStrategy interface {
	Convert([]byte) (*PipelineMessage, error)
}

// orders the stream of messages
type OrdererStrategy interface {
	// delegate it from the worker to the orderer. the worker should not concern himself with the ordering semantics
	SetSymbolState(*SymbolState)
	// init orderer buffer internals
	InitOrdererState(*PipelineMessage)
	// places message into the buffer if needed
	Order(*PipelineMessage, string, chan *DispatchRecord) ([]*PipelineMessage, error)
	// sort strategy the buffer in order before flushing
	PrepareBufferFlush() []*PipelineMessage
	// ack fires after processing each message in buffer
	Ack(*PipelineMessage)
	// cleanup after buffer processed
	Cleanup()
	// get back orderer id for dedupe key construction
	GetOrderingId(*PipelineMessage) string
}

// publishes to downstream topic based on channel type
type PublisherStrategy interface {
	Publish(raw []byte, partitionKey string, msg *PipelineMessage)
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

type CoinbaseTickerMsg struct {
	Type        string `json:"type"`
	Sequence    int64  `json:"sequence"`
	ProductID   string `json:"product_id"`
	Price       string `json:"price"`
	Open24h     string `json:"open_24h"`
	Volume24h   string `json:"volume_24h"`
	Low24h      string `json:"low_24h"`
	High24h     string `json:"high_24h"`
	Volume30d   string `json:"volume_30d"`
	BestBid     string `json:"best_bid"`
	BestBidSize string `json:"best_bid_size"`
	BestAsk     string `json:"best_ask"`
	BestAskSize string `json:"best_ask_size"`
	Side        string `json:"side"`
	Time        string `json:"time"` // RFC3339 timestamp
	TradeID     int64  `json:"trade_id"`
	LastSize    string `json:"last_size"`
}
