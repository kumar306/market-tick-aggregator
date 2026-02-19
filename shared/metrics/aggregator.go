package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	Aggregator_ConsumerErrorsTotal      *prometheus.CounterVec
	Aggregator_ConsumerSuccessesTotal   *prometheus.CounterVec
	Aggregator_TicksIngestedTotal       *prometheus.CounterVec
	Aggregator_TicksDroppedTotal        *prometheus.CounterVec
	Aggregator_WorkerQueueDepth         *prometheus.GaugeVec
	Aggregator_WindowFlushDurationMs    *prometheus.HistogramVec
	Aggregator_AggregatesProducedTotal  *prometheus.CounterVec
	Aggregator_TickProcessingDurationMs *prometheus.HistogramVec
	Aggregator_AggregatesDroppedTotal   *prometheus.CounterVec
	Aggregator_CircuitBreakerActive     prometheus.Gauge
	Aggregator_ProduceFailuresTotal     *prometheus.CounterVec
	Aggregator_ProduceSuccessesTotal    *prometheus.CounterVec
	Aggregator_SymbolsPerWorker         *prometheus.GaugeVec
	Aggregator_WindowsPerSymbol         *prometheus.GaugeVec
	Aggregator_RedisCB_StateChanges     *prometheus.CounterVec
	Aggregator_DedupeChecksTotal        *prometheus.CounterVec
	Aggregator_DedupeHitsTotal          *prometheus.CounterVec
	Aggregator_DedupeErrorsTotal        *prometheus.CounterVec
	Aggregator_RedisCB_FallbacksTotal   prometheus.Counter
	Aggregator_RedisCB_State            prometheus.Gauge
	Aggregator_DedupeStoreErrorsTotal   *prometheus.CounterVec
	Aggregator_DedupeLatencySeconds     *prometheus.HistogramVec
	Aggregator_CommitOffsetsTotal       *prometheus.GaugeVec
	Aggregator_CommitOffsetErrorsTotal  prometheus.Counter
	Aggregator_CommitLatencySeconds     prometheus.Histogram
	aggregatorMetricsOnce               sync.Once
)

func InitAggregatorMetrics() {
	aggregatorMetricsOnce.Do(func() {
		Aggregator_ConsumerErrorsTotal = NewCounterVec(
			"aggregator_consumer_errors_total",
			"Number of errors when consuming messages from upstream",
			[]string{"partition"},
		)

		Aggregator_ConsumerSuccessesTotal = NewCounterVec(
			"aggregator_consumer_successes_total",
			"Number of successful messages read from upstream",
			[]string{"partition"},
		)

		Aggregator_TicksIngestedTotal = NewCounterVec(
			"aggregator_ticks_ingested_total",
			"Number of ticks ingested by worker",
			[]string{"worker_id"},
		)

		Aggregator_TicksDroppedTotal = NewCounterVec(
			"aggregator_ticks_dropped_total",
			"Number of ticks dropped by worker",
			[]string{"worker_id"},
		)

		Aggregator_WorkerQueueDepth = NewGaugeVec(
			"aggregator_worker_queue_depth",
			"Worker queue depth for storing ticks for processing",
			[]string{"worker_id"},
		)

		Aggregator_WindowFlushDurationMs = NewHistogramVec(
			"aggregator_window_flush_duration_ms",
			"Window flush duration per worker window",
			prometheus.DefBuckets,
			[]string{"worker_id", "window_id"},
		)

		Aggregator_AggregatesProducedTotal = NewCounterVec(
			"aggregator_aggregates_produced_total",
			"Total number of aggregates produced by worker",
			[]string{"worker_id"},
		)

		Aggregator_TickProcessingDurationMs = NewHistogramVec(
			"aggregator_tick_processing_duration_ms",
			"Tick processing per worker for all its windows",
			prometheus.DefBuckets,
			[]string{"worker_id"},
		)

		Aggregator_CircuitBreakerActive = NewGauge(
			"aggregator_backpressure_active",
			"Flag which is set when backpressure is active",
		)

		Aggregator_AggregatesDroppedTotal = NewCounterVec(
			"aggregator_kafka_partitions_paused",
			"Kafka partitions which are paused when backpressure is active",
			[]string{"partition"},
		)

		Aggregator_ProduceFailuresTotal = NewCounterVec(
			"aggregator_produce_failures_total",
			"Number of failures when doing kafka produce",
			[]string{"exchange", "channel", "symbol", "partition"},
		)

		Aggregator_ProduceSuccessesTotal = NewCounterVec(
			"aggregator_produce_successes_total",
			"Number of successes when doing kafka produce",
			[]string{"exchange", "channel", "symbol", "partition"},
		)

		Aggregator_SymbolsPerWorker = NewGaugeVec(
			"aggregator_symbols_per_worker",
			"Number of symbols managed by worker",
			[]string{"worker_id"},
		)

		Aggregator_WindowsPerSymbol = NewGaugeVec(
			"aggregator_windows_per_symbol",
			"Number of windows managed per symbol",
			[]string{"worker_id", "exchange", "channel", "symbol"},
		)

		Aggregator_DedupeChecksTotal = NewCounterVec(
			"aggregator_dedupe_checks_total",
			"Number of dedupe checks in worker",
			[]string{"exchange", "symbol"},
		)

		Aggregator_DedupeHitsTotal = NewCounterVec(
			"aggregator_dedupe_hits_total",
			"Number of duplicate messages detected",
			[]string{"exchange", "symbol"},
		)

		Aggregator_DedupeErrorsTotal = NewCounterVec(
			"aggregator_dedupe_errors_total",
			"Number of errors when checking redis dedupe",
			[]string{"exchange", "symbol"},
		)

		Aggregator_RedisCB_FallbacksTotal = NewCounter(
			"aggregator_dedupe_cb_fallbacks_total",
			"Number of dedupe skips when circuit in open state",
		)

		Aggregator_RedisCB_State = NewGauge(
			"aggregator_dedupe_cb_state",
			"Current redis circuit breaker state",
		)

		Aggregator_DedupeStoreErrorsTotal = NewCounterVec(
			"aggregator_dedupe_store_errors_total",
			"Number of errors when writing dedupe key to redis",
			[]string{"exchange", "symbol"},
		)

		Aggregator_DedupeLatencySeconds = NewHistogramVec(
			"aggregator_dedupe_latency_seconds",
			"Latency taken by dedupe check in seconds",
			prometheus.DefBuckets,
			[]string{"exchange", "symbol"},
		)

		Aggregator_CommitOffsetsTotal = NewGaugeVec(
			"aggregator_commit_offsets_total",
			"Latest offset committed by offset committer",
			[]string{"topic", "partition"},
		)

		Aggregator_CommitOffsetErrorsTotal = NewCounter(
			"aggregator_commit_offsets_errors_total",
			"Number of errors while committing kafka offsets",
		)

		Aggregator_CommitLatencySeconds = NewHistogram(
			"normalizer_commit_latency_seconds",
			"Commit latency in seconds",
			prometheus.ExponentialBuckets(0.001, 2, 12),
		)
	})
}
