package metrics

import "github.com/prometheus/client_golang/prometheus"

var (

	// consumer metrics
	Normalizer_ConsumerMessagesTotal = NewGaugeVec(
		"consumer_messages_total",
		"Number of unprocessed messages read from the consumer",
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_ConsumerLag = NewGauge(
		"consumer_lag",
		"Lag faced by the consumer",
	)

	Normalizer_ConsumerErrorsTotal = NewCounterVec(
		"consumer_errors_total",
		"Number of errors occurred during consumer processing",
		[]string{"exchange", "channel", "symbol"},
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
		[]string{"exchange", "channel", "symbol"},
	)

	// histogram
	Normalizer_BufferFlushLatency = NewHistogramVec(
		"buffer_flush_latency_seconds",
		"Buffer flush latency in seconds",
		prometheus.DefBuckets,
		[]string{"exchange", "channel", "symbol"},
	)

	Normalizer_DroppedGapTotal = NewCounterVec(
		"dropped_gap_total",
		"Number of dropped gaps",
		[]string{"exchange", "channel", "symbol"},
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
		prometheus.DefBuckets,
		[]string{"topic"},
	)

	Normalizer_CommitOffsetsTotal = NewGauge(
		"commit_offsets_total",
		"Latest offset committed by offset committer",
	)

	Normalizer_CommitOffsetErrorsTotal = NewCounter(
		"commit_offsets_errors_total",
		"Number of errors while committing kafka offsets",
	)

	// commit lag
	Normalizer_CommitLatencySeconds = NewHistogram(
		"commit_latency_seconds",
		"Commit latency in seconds",
		prometheus.DefBuckets,
	)

	// worker metrics
	Normalizer_NormalizedMessagesTotal = NewCounterVec(
		"normalized_messages_total",
		"Number of proto normalized messages in worker",
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

	Normalizer_WorkerLatencySeconds = NewHistogramVec(
		"worker_latency_seconds",
		"Latency of the worker ProcessRecord()",
		prometheus.DefBuckets,
		[]string{"worker_id"},
	)

	Normalizer_WorkerCrashesTotal = NewCounterVec(
		"worker_crashes_total",
		"Number of worker crashes",
		[]string{"worker_id"},
	)

	// if kafka saturated or redis overloaded - for circuit breaker
	Normalizer_BackpressureTriggeredTotal = NewCounterVec(
		"backpressure_triggered_total",
		"Number of backpressure triggers in Kafka/Redis",
		[]string{"service", "exchange", "channel", "symbol"},
	)
)
