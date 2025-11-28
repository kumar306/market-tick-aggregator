package metrics

import "github.com/prometheus/client_golang/prometheus"

var (

	// consumer metrics
	Normalizer_ConsumerMessagesTotal *prometheus.GaugeVec
	Normalizer_ConsumerLag           *prometheus.GaugeVec
	Normalizer_ConsumerErrorsTotal   *prometheus.CounterVec

	// dedupe metrics
	Normalizer_DedupeChecksTotal      *prometheus.CounterVec
	Normalizer_DedupeHitsTotal        *prometheus.CounterVec
	Normalizer_DedupeErrorsTotal      *prometheus.CounterVec
	Normalizer_RedisCB_FallbacksTotal prometheus.Counter
	Normalizer_RedisCB_State          prometheus.Gauge
	Normalizer_DedupeStoreErrorsTotal *prometheus.CounterVec
	Normalizer_DedupeLatencySeconds   *prometheus.HistogramVec

	// orderer metrics
	Normalizer_BufferSize         *prometheus.GaugeVec
	Normalizer_BufferFlushesTotal *prometheus.CounterVec
	Normalizer_BufferFlushLatency *prometheus.HistogramVec
	Normalizer_DroppedTimerTotal  *prometheus.CounterVec

	// publisher metrics
	Normalizer_ProducerPublishesTotal     *prometheus.CounterVec
	Normalizer_ProducerPublishErrorsTotal *prometheus.CounterVec
	Normalizer_ProducerQueueSize          *prometheus.GaugeVec

	// producer lag
	Normalizer_ProducerLatencySeconds  *prometheus.HistogramVec
	Normalizer_CommitOffsetsTotal      *prometheus.GaugeVec
	Normalizer_CommitOffsetErrorsTotal prometheus.Counter

	// commit lag
	Normalizer_CommitLatencySeconds prometheus.Histogram

	// worker metrics
	Normalizer_NormalizedMessagesTotal      *prometheus.CounterVec
	Normalizer_NormalizedMessageErrorsTotal *prometheus.CounterVec
	Normalizer_OrdererErrorsTotal           *prometheus.CounterVec
	Normalizer_WorkerQueueSize              *prometheus.GaugeVec
	Normalizer_PausedPartitions             *prometheus.GaugeVec
	Normalizer_WorkerQueueUsage             *prometheus.GaugeVec
	Normalizer_WorkerLatencySeconds         *prometheus.HistogramVec
	Normalizer_WorkerCrashesTotal           *prometheus.CounterVec
	Normalizer_WorkerProcessedMessagesTotal *prometheus.CounterVec

	// if kafka saturated or redis overloaded - for circuit breaker
	Normalizer_BackpressureTriggeredTotal *prometheus.CounterVec

	// circuit breaker state change metrics
	Normalizer_RedisCB_StateChanges   *prometheus.CounterVec
	Normalizer_KafkaCB_StateChanges   *prometheus.CounterVec
	Normalizer_KafkaCB_State          prometheus.Gauge
	Normalizer_KafkaCB_FallbacksTotal prometheus.Counter
)

func InitNormalizerMetrics() {
	// consumer metrics
	Normalizer_ConsumerMessagesTotal = NewGaugeVec(
		"consumer_messages_total",
		"Number of messages read from the consumer",
		[]string{"topic", "partition"},
	)

	Normalizer_ConsumerLag = NewGaugeVec(
		"consumer_lag",
		"Lag faced by the consumer",
		[]string{"topic", "partition"},
	)

	Normalizer_ConsumerErrorsTotal = NewCounterVec(
		"consumer_errors_total",
		"Number of errors occurred during consumer processing",
		[]string{"topic", "partition"},
	)

	// dedupe metrics
	Normalizer_DedupeChecksTotal = NewCounterVec(
		"dedupe_checks_total",
		"Number of dedupe checks in worker",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_DedupeHitsTotal = NewCounterVec(
		"dedupe_hits_total",
		"Number of duplicate messages detected",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_DedupeErrorsTotal = NewCounterVec(
		"dedupe_errors_total",
		"Number of errors when checking redis dedupe",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_RedisCB_FallbacksTotal = NewCounter(
		"dedupe_cb_fallbacks_total",
		"Number of dedupe skips when circuit in open state",
	)

	Normalizer_RedisCB_State = NewGauge(
		"dedupe_cb_state",
		"Current redis circuit breaker state",
	)

	Normalizer_DedupeStoreErrorsTotal = NewCounterVec(
		"dedupe_store_errors_total",
		"Number of errors when writing dedupe key to redis",
		[]string{"exchange", "channel", "symbol"},
	)

	// histogram
	Normalizer_DedupeLatencySeconds = NewHistogramVec(
		"dedupe_latency_seconds",
		"Latency taken by dedupe check in seconds",
		prometheus.DefBuckets,
		[]string{"exchange", "channel", "symbol"},
	)

	// orderer metrics
	Normalizer_BufferSize = NewGaugeVec(
		"buffer_size",
		"Size of orderer buffer",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_BufferFlushesTotal = NewCounterVec(
		"buffer_flushes_total",
		"Number of buffer flushes",
		[]string{"id"},
	)

	// histogram
	Normalizer_BufferFlushLatency = NewHistogramVec(
		"buffer_flush_latency_seconds",
		"Buffer flush latency in seconds",
		prometheus.ExponentialBuckets(0.001, 2, 12),
		[]string{"id"},
	)

	Normalizer_DroppedTimerTotal = NewCounterVec(
		"dropped_timer_total",
		"Number of dropped timer",
		[]string{"exchange", "channel", "symbol"},
	)

	// publisher metrics
	Normalizer_ProducerPublishesTotal = NewCounterVec(
		"producer_publishes_total",
		"Number of producer normalized publishes to downstream",
		[]string{"topic"},
	)

	Normalizer_ProducerPublishErrorsTotal = NewCounterVec(
		"producer_publish_errors_total",
		"Number of producer normalized publish errors to downstream",
		[]string{"topic"},
	)

	Normalizer_ProducerQueueSize = NewGaugeVec(
		"producer_queue_size",
		"Queue size of the normalizer producer",
		[]string{"topic"},
	)

	// producer lag
	Normalizer_ProducerLatencySeconds = NewHistogramVec(
		"producer_latency_seconds",
		"Producer latency in seconds",
		prometheus.ExponentialBuckets(0.001, 2, 12),
		[]string{"topic"},
	)

	Normalizer_CommitOffsetsTotal = NewGaugeVec(
		"commit_offsets_total",
		"Latest offset committed by offset committer",
		[]string{"topic", "partition"},
	)

	Normalizer_CommitOffsetErrorsTotal = NewCounter(
		"commit_offsets_errors_total",
		"Number of errors while committing kafka offsets",
	)

	// commit lag
	Normalizer_CommitLatencySeconds = NewHistogram(
		"commit_latency_seconds",
		"Commit latency in seconds",
		prometheus.ExponentialBuckets(0.001, 2, 12),
	)

	// worker metrics
	Normalizer_NormalizedMessagesTotal = NewCounterVec(
		"normalized_messages_total",
		"Number of proto normalized messages in worker",
		[]string{"exchange", "channel", "symbol"},
	)

	// worker metrics
	Normalizer_NormalizedMessageErrorsTotal = NewCounterVec(
		"normalized_message_errors_total",
		"Number of proto normalizer errors in worker",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_OrdererErrorsTotal = NewCounterVec(
		"orderer_errors_total",
		"Number of orderer errors in the worker",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_WorkerQueueSize = NewGaugeVec(
		"worker_queue_size",
		"Size of the dispatcher to worker channel",
		[]string{"worker_id"},
	)

	Normalizer_PausedPartitions = NewGaugeVec(
		"worker_paused_partitions",
		"Metric to track partitions paused",
		[]string{"worker_id"},
	)

	Normalizer_WorkerQueueUsage = NewGaugeVec(
		"worker_queue_usage",
		"Size/capacity of the dispatcher to worker channel",
		[]string{"worker_id"},
	)

	Normalizer_WorkerLatencySeconds = NewHistogramVec(
		"worker_latency_seconds",
		"Latency of the worker ProcessRecord()",
		prometheus.ExponentialBuckets(0.001, 2, 12),
		[]string{"worker_id"},
	)

	Normalizer_WorkerCrashesTotal = NewCounterVec(
		"worker_crashes_total",
		"Number of worker crashes",
		[]string{"worker_id"},
	)

	Normalizer_WorkerProcessedMessagesTotal = NewCounterVec(
		"worker_processed_messages_total",
		"Number of worker processed messages per worker id",
		[]string{"worker_id"},
	)

	// if kafka saturated or redis overloaded - for circuit breaker
	Normalizer_BackpressureTriggeredTotal = NewCounterVec(
		"backpressure_triggered_total",
		"Number of backpressure triggers in Kafka/Redis",
		[]string{"service", "exchange", "channel", "symbol"},
	)

	// circuit breaker state change metrics
	Normalizer_RedisCB_StateChanges = NewCounterVec(
		"redis_cb_state_changes",
		"Number of redis circuit breaker state changes",
		[]string{"to"},
	)

	Normalizer_KafkaCB_StateChanges = NewCounterVec(
		"kafka_cb_state_changes",
		"Number of kafka circuit breaker state changes",
		[]string{"to"},
	)

	Normalizer_KafkaCB_State = NewGauge(
		"kafka_cb_state",
		"Current kafka circuit breaker state",
	)

	Normalizer_KafkaCB_FallbacksTotal = NewCounter(
		"kafka_cb_fallbacks_total",
		"Number of kafka produce fallbacks when circuit in open state",
	)
}
