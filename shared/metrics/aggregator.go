package metrics

import "github.com/prometheus/client_golang/prometheus"

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
)

func InitAggregatorMetrics() {
	Aggregator_ConsumerErrorsTotal = NewCounterVec(
		"consumer_errors_total",
		"Number of errors when consuming messages from upstream",
		[]string{"partition"},
	)

	Aggregator_ConsumerSuccessesTotal = NewCounterVec(
		"consumer_successes_total",
		"Number of successful messages read from upstream",
		[]string{"partition"},
	)

	Aggregator_TicksIngestedTotal = NewCounterVec(
		"ticks_ingested_total",
		"Number of ticks ingested by worker",
		[]string{"worker_id"},
	)

	Aggregator_TicksDroppedTotal = NewCounterVec(
		"ticks_dropped_total",
		"Number of ticks dropped by worker",
		[]string{"worker_id"},
	)

	Aggregator_WorkerQueueDepth = NewGaugeVec(
		"worker_queue_depth",
		"Worker queue depth for storing ticks for processing",
		[]string{"worker_id"},
	)

	Aggregator_WindowFlushDurationMs = NewHistogramVec(
		"window_flush_duration_ms",
		"Window flush duration per worker window",
		prometheus.DefBuckets,
		[]string{"worker_id", "window_id"},
	)

	Aggregator_AggregatesProducedTotal = NewCounterVec(
		"aggregates_produced_total",
		"Total number of aggregates produced by worker",
		[]string{"worker_id"},
	)

	Aggregator_TickProcessingDurationMs = NewHistogramVec(
		"tick_processing_duration_ms",
		"Tick processing per worker for all its windows",
		prometheus.DefBuckets,
		[]string{"worker_id"},
	)

	Aggregator_CircuitBreakerActive = NewGauge(
		"backpressure_active",
		"Flag which is set when backpressure is active",
	)

	Aggregator_AggregatesDroppedTotal = NewCounterVec(
		"kafka_partitions_paused",
		"Kafka partitions which are paused when backpressure is active",
		[]string{"partition"},
	)

	Aggregator_ProduceFailuresTotal = NewCounterVec(
		"produce_failures_total",
		"Number of failures when doing kafka produce",
		[]string{"exchange", "channel", "symbol", "partition"},
	)

	Aggregator_ProduceSuccessesTotal = NewCounterVec(
		"produce_successes_total",
		"Number of successes when doing kafka produce",
		[]string{"exchange", "channel", "symbol", "partition"},
	)

	Aggregator_SymbolsPerWorker = NewGaugeVec(
		"symbols_per_worker",
		"Number of symbols managed by worker",
		[]string{"worker_id"},
	)

	Aggregator_WindowsPerSymbol = NewGaugeVec(
		"windows_per_symbol",
		"Number of windows managed per symbol",
		[]string{"worker_id", "exchange", "channel", "symbol"},
	)
}
