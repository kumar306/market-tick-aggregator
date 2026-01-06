package constants

import (
	"market-aggregator/proto/generated"

	"github.com/twmb/franz-go/pkg/kgo"
)

const ConfigFile string = "./config/config.yaml"

type Config struct {
	KafkaConfig  *KafkaConfig    `yaml:"kafka"`
	WorkerCount  int             `yaml:"worker_count"`
	WindowConfig []*WindowConfig `yaml:"window"`
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
}

type BackpressureConfig struct {
	QueueUsageHighThreshold float64 `yaml:"queue_usage_high_threshold"`
	QueueUsageLowThreshold  float64 `yaml:"queue_usage_low_threshold"`
	ThresholdActiveMillis   int64   `yaml:"threshold_active_millis"`
	CooldownTimeMillis      int64   `yaml:"cooldown_time_millis"`
}

type TopicConfig struct {
	Upstream   string `yaml:"upstream"`
	Downstream string `yaml:"downstream"`
}

type WindowConfig struct {
	Id             string `yaml:"id"`
	DurationMs     int64  `yaml:"duration_ms"`
	FlushCadencyMs int64  `yaml:"flush_cadency_ms"`
	BucketSizeMs   int64  `yaml:"bucket_size_ms"`
}

type EventType int

const (
	ProcessEvent EventType = iota
	FlushEvent
)

type DispatchRecord struct {
	Event        EventType
	Tick         *generated.NormalizedTick
	Record       *kgo.Record
	BufferKey    string
	WorkerIdx    int
	WindowConfig *WindowConfig
	FlushTsMs    int64
}

type Metric interface {
	Update(*generated.NormalizedTick)
	Apply(*generated.AggregatedTick)
	Reset()
	GetValue() float64
}

type MetricName string

type Window struct {
	Id             string
	DurationMs     int64
	FlushCadencyMs int64
	Metrics        map[MetricName]Metric
	LastFlushTsMs  int64
}

// tick arrives - a particular symbol
// it is routed to the exact worker managing the long lived metrics for that symbol
// i have multiple windows per symbol
// - 5s, 10s, 30s, 1min, 2min, 5min, 10min, 30min, 1h, 2h, 6h, 12h, 24h
// all these windows has some existing metric for that symbol which needs to be updated
// some metrics are tumbling, some are rolling - use ring buffer for bucketed rolling to decide number of buckets needed
// each window has its flush timing, its window timing. its a global clock
// same duration windows across many symbols are flushed at the same time
// why are we routing it again to specific worker? because one symbol's stats needs to be aggregated in one place.
// and we need to have a pool of workers to process it for low latency
// its a global clock.
// at the window's flush timing, a flush event for that particular window is dispatched to the same worker so window flush for each worker happens without interfering with tick arrival
// window contains some metrics - it loops over its metrics and calls Snapshot() ? - we need one message of metrics - its to be stored in aggregated_ticks as protobuf
// then if the metric is tumbling - reset it - post kafka publish and commit
// if its a rolling metric (rolling vwap, rolling volume), metric owns a ring buffer of BucketWindow - Bucket contains like
// metric_name string, buckets []Bucket, idx int
// Bucket contains - values []float, bucket_size int (so we can get a bucket's value)
// on flush, need to advance to the next bucket. if idx > buffer size, we overwrite into new bucket
