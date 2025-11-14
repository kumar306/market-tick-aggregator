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

type DispatchRecord struct {
	Record    *kgo.Record
	BufferKey string
	Exchange  string
	Channel   string
	Symbol    string
}

const (
	ConfigFilePath           string = "./config/config.yaml"
	DefaultConsumerGroupName string = "normalizer-group-1"
)

// worker state per product id
type SymbolState struct {
	// next seq in the feed
	LastSeqId uint64
	// buffer map of sequence id to record
	BufferMap  map[uint64]*kgo.Record
	Orderer    *OrdererStrategy
	Normalizer *NormalizerStrategy
	Publisher  *PublisherStrategy
	LastSeenTs time.Time
}

type OrdererStrategy interface {
	Order()
}

type PublisherStrategy interface {
	Publish()
}

type NormalizerStrategy interface {
	Normalize()
}

type RawNormalizerStrategy interface {
	RawNormalize()
}
