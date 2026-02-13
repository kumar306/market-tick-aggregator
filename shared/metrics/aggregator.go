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
	})
}
