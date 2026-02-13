package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

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

	NormalizerOnce sync.Once
)

func InitNormalizerMetrics() {
	// consumer metrics
	NormalizerOnce.Do(func() {
		Normalizer_ConsumerMessagesTotal = NewGaugeVec(
			"normalizer_consumer_messages_total",
			"Number of messages read from the consumer",
			[]string{"topic", "partition"},
		)

		Normalizer_ConsumerLag = NewGaugeVec(
			"normalizer_consumer_lag",
			"Lag faced by the consumer",
			[]string{"topic", "partition"},
		)

		Normalizer_ConsumerErrorsTotal = NewCounterVec(
			"normalizer_consumer_errors_total",
			"Number of errors occurred during consumer processing",
			[]string{"topic", "partition"},
		)

		// dedupe metrics
		Normalizer_DedupeChecksTotal = NewCounterVec(
			"normalizer_dedupe_checks_total",
			"Number of dedupe checks in worker",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_DedupeHitsTotal = NewCounterVec(
			"normalizer_dedupe_hits_total",
			"Number of duplicate messages detected",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_DedupeErrorsTotal = NewCounterVec(
			"normalizer_dedupe_errors_total",
			"Number of errors when checking redis dedupe",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_RedisCB_FallbacksTotal = NewCounter(
			"normalizer_dedupe_cb_fallbacks_total",
			"Number of dedupe skips when circuit in open state",
		)

		Normalizer_RedisCB_State = NewGauge(
			"normalizer_dedupe_cb_state",
			"Current redis circuit breaker state",
		)

		Normalizer_DedupeStoreErrorsTotal = NewCounterVec(
			"normalizer_dedupe_store_errors_total",
			"Number of errors when writing dedupe key to redis",
			[]string{"exchange", "channel", "symbol"},
		)

		// histogram
		Normalizer_DedupeLatencySeconds = NewHistogramVec(
			"normalizer_dedupe_latency_seconds",
			"Latency taken by dedupe check in seconds",
			prometheus.DefBuckets,
			[]string{"exchange", "channel", "symbol"},
		)

		// orderer metrics
		Normalizer_BufferSize = NewGaugeVec(
			"normalizer_buffer_size",
			"Size of orderer buffer",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_BufferFlushesTotal = NewCounterVec(
			"normalizer_buffer_flushes_total",
			"Number of buffer flushes",
			[]string{"id"},
		)

		// histogram
		Normalizer_BufferFlushLatency = NewHistogramVec(
			"normalizer_buffer_flush_latency_seconds",
			"Buffer flush latency in seconds",
			prometheus.ExponentialBuckets(0.001, 2, 12),
			[]string{"id"},
		)

		Normalizer_DroppedTimerTotal = NewCounterVec(
			"normalizer_dropped_timer_total",
			"Number of dropped timer",
			[]string{"exchange", "channel", "symbol"},
		)

		// publisher metrics
		Normalizer_ProducerPublishesTotal = NewCounterVec(
			"normalizer_producer_publishes_total",
			"Number of producer normalized publishes to downstream",
			[]string{"topic"},
		)

		Normalizer_ProducerPublishErrorsTotal = NewCounterVec(
			"normalizer_producer_publish_errors_total",
			"Number of producer normalized publish errors to downstream",
			[]string{"topic"},
		)

		Normalizer_ProducerQueueSize = NewGaugeVec(
			"normalizer_producer_queue_size",
			"Queue size of the normalizer producer",
			[]string{"topic"},
		)

		// producer lag
		Normalizer_ProducerLatencySeconds = NewHistogramVec(
			"normalizer_producer_latency_seconds",
			"Producer latency in seconds",
			prometheus.ExponentialBuckets(0.001, 2, 12),
			[]string{"topic"},
		)

		Normalizer_CommitOffsetsTotal = NewGaugeVec(
			"normalizer_commit_offsets_total",
			"Latest offset committed by offset committer",
			[]string{"topic", "partition"},
		)

		Normalizer_CommitOffsetErrorsTotal = NewCounter(
			"normalizer_commit_offsets_errors_total",
			"Number of errors while committing kafka offsets",
		)

		// commit lag
		Normalizer_CommitLatencySeconds = NewHistogram(
			"normalizer_commit_latency_seconds",
			"Commit latency in seconds",
			prometheus.ExponentialBuckets(0.001, 2, 12),
		)

		// worker metrics
		Normalizer_NormalizedMessagesTotal = NewCounterVec(
			"normalizer_normalized_messages_total",
			"Number of proto normalized messages in worker",
			[]string{"exchange", "channel", "symbol"},
		)

		// worker metrics
		Normalizer_NormalizedMessageErrorsTotal = NewCounterVec(
			"normalizer_normalized_message_errors_total",
			"Number of proto normalizer errors in worker",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_OrdererErrorsTotal = NewCounterVec(
			"normalizer_orderer_errors_total",
			"Number of orderer errors in the worker",
			[]string{"exchange", "channel", "symbol"},
		)

		Normalizer_WorkerQueueSize = NewGaugeVec(
			"normalizer_worker_queue_size",
			"Size of the dispatcher to worker channel",
			[]string{"worker_id"},
		)

		Normalizer_PausedPartitions = NewGaugeVec(
			"normalizer_worker_paused_partitions",
			"Metric to track partitions paused",
			[]string{"worker_id"},
		)

		Normalizer_WorkerQueueUsage = NewGaugeVec(
			"normalizer_worker_queue_usage",
			"Size/capacity of the dispatcher to worker channel",
			[]string{"worker_id"},
		)

		Normalizer_WorkerLatencySeconds = NewHistogramVec(
			"normalizer_worker_latency_seconds",
			"Latency of the worker ProcessRecord()",
			prometheus.ExponentialBuckets(0.001, 2, 12),
			[]string{"worker_id"},
		)

		Normalizer_WorkerCrashesTotal = NewCounterVec(
			"normalizer_worker_crashes_total",
			"Number of worker crashes",
			[]string{"worker_id"},
		)

		Normalizer_WorkerProcessedMessagesTotal = NewCounterVec(
			"normalizer_worker_processed_messages_total",
			"Number of worker processed messages per worker id",
			[]string{"worker_id"},
		)

		// if kafka saturated or redis overloaded - for circuit breaker
		Normalizer_BackpressureTriggeredTotal = NewCounterVec(
			"normalizer_backpressure_triggered_total",
			"Number of backpressure triggers in Kafka/Redis",
			[]string{"service", "exchange", "channel", "symbol"},
		)

		// circuit breaker state change metrics
		Normalizer_RedisCB_StateChanges = NewCounterVec(
			"normalizer_redis_cb_state_changes",
			"Number of redis circuit breaker state changes",
			[]string{"to"},
		)

		Normalizer_KafkaCB_StateChanges = NewCounterVec(
			"normalizer_kafka_cb_state_changes",
			"Number of kafka circuit breaker state changes",
			[]string{"to"},
		)

		Normalizer_KafkaCB_State = NewGauge(
			"normalizer_kafka_cb_state",
			"Current kafka circuit breaker state",
		)

		Normalizer_KafkaCB_FallbacksTotal = NewCounter(
			"normalizer_kafka_cb_fallbacks_total",
			"Number of kafka produce fallbacks when circuit in open state",
		)
	})
}
