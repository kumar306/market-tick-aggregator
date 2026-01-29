package constants

import (
	"market-orderbook/book"
	"market-orderbook/proto/generated"

	"github.com/twmb/franz-go/pkg/kgo"
)

type EventType int

// flush event - sends top N price levels to kafka
// snapshot event - occurs every 1 min, snapshot to redis

const (
	ProcessEvent EventType = iota
	FlushEvent
	SnapshotRequestEvent
	SnapshotExecuteEvent
	SnapshotPersistedEvent
)

const ConfigFile string = "./config/config.yaml"

type Config struct {
	KafkaConfig *KafkaConfig `yaml:"kafka"`
	RedisConfig *RedisConfig `yaml:"redis"`
	WorkerCount int          `yaml:"worker_count"`
}

type KafkaConfig struct {
	BootstrapServers       []string           `yaml:"bootstrap_servers"`
	TopicConfig            TopicConfig        `yaml:"topics"`
	ConsumerGroup          string             `yaml:"consumer_group"`
	BackpressureConfig     BackpressureConfig `yaml:"backpressure"`
	MaxBufferRecords       int                `yaml:"max_buffer_records"`
	CBReqCount             int                `yaml:"cb_req_count"`
	CBFailureRatio         float64            `yaml:"cb_failure_ratio"`
	ProduceErrorBufferSize int                `yaml:"produce_error_buffer_size"`
	FlushIntervalSeconds   int                `yaml:"flush_interval_seconds"`
}

type BackpressureConfig struct {
	QueueUsageHighThreshold float64 `yaml:"queue_usage_high_threshold"`
	QueueUsageLowThreshold  float64 `yaml:"queue_usage_low_threshold"`
	ConfirmSeconds          int64   `yaml:"confirm_seconds"`
	PollIntervalMs          int64   `yaml:"poll_interval_ms"`
}

type TopicConfig struct {
	Upstream   string `yaml:"upstream"`
	Downstream string `yaml:"downstream"`
}

type RedisConfig struct {
	TtlMinutes   int `yaml:"ttl_minutes"`
	PoolSize     int `yaml:"pool_size"`
	MinIdleConns int `yaml:"min_idle_conns"`
}

type DispatchRecord struct {
	Event      EventType
	Partition  int32
	Offset     int64
	Record     *kgo.Record
	Update     *generated.NormalizedBook
	Exchange   string
	Symbol     string
	TsMs       int64
	FlushEpoch int32
	BufferKey  string
}

type Ack struct {
	Epoch            int32
	WorkerID         int
	PartitionOffsets map[int32]int64
}

type SnapshotMsg struct {
	Snapshot *book.OrderBookSnapshot
	Key      string
}

type BackpressureState int

const (
	Healthy BackpressureState = iota
	Suspect
	Throttling
)

type BackpressureEvent struct {
	MaxQueueUsage float64
}
