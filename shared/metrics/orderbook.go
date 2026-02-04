package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	Orderbook_UpdatesTotal                   *prometheus.CounterVec
	Orderbook_WorkerQueueDepth               *prometheus.GaugeVec
	Orderbook_FlushEpochsTotal               *prometheus.CounterVec
	Orderbook_FlushLatencyMs                 *prometheus.HistogramVec
	Orderbook_FlushSuccessTotal              *prometheus.CounterVec
	Orderbook_FlushKafkaErrorsTotal          *prometheus.CounterVec
	Orderbook_CommitPartitionOffsets         *prometheus.GaugeVec
	Orderbook_CommitLatencyMs                prometheus.Histogram
	Orderbook_ActiveEpochPendingParticipants prometheus.Gauge
	Orderbook_CommitActiveEpochs             prometheus.Gauge
	Orderbook_SnapshotRequestsTotal          *prometheus.CounterVec
	Orderbook_SnapshotFailuresTotal          *prometheus.CounterVec
	Orderbook_SnapshotSuccessesTotal         *prometheus.CounterVec
	Orderbook_EmptySnapshotsTotal            *prometheus.CounterVec
	Orderbook_SnapshotSizeBytes              *prometheus.HistogramVec
	Orderbook_BackpressureState              prometheus.Gauge
	Orderbook_BackpressureTransitionsTotal   prometheus.Counter
	Orderbook_KafkaFetchPaused               prometheus.Gauge
	Orderbook_MaxWorkerQueueUsage            prometheus.Histogram
	Orderbook_ConsumerSuccessesTotal         *prometheus.CounterVec
	Orderbook_ConsumerErrorsTotal            *prometheus.CounterVec
)

func InitOrderbookMetrics() {

	Orderbook_UpdatesTotal = NewCounterVec("orderbook_updates_total",
		"Number of orderbook update events per worker", []string{"worker", "exchange", "symbol"})
	Orderbook_WorkerQueueDepth = NewGaugeVec("worker_queue_depth", "Worker queue depth used to calculate backpressure", []string{"worker"})
	Orderbook_FlushEpochsTotal = NewCounterVec("flush_epochs_total", "Total number of flushed epochs by coordinator", []string{"status"})
	Orderbook_FlushLatencyMs = NewHistogramVec("flush_latency_ms", "Latency in ms for worker flush", prometheus.DefBuckets, []string{"worker"})
	Orderbook_FlushSuccessTotal = NewCounterVec("flush_success_total", "Total number of successful flushes per worker", []string{"worker"})
	Orderbook_FlushKafkaErrorsTotal = NewCounterVec("flush_kafka_errors_total", "Total number of errors occurred during downstream kafka publish", []string{"worker"})
	Orderbook_CommitPartitionOffsets = NewGaugeVec("commit_partition_offsets", "Offset committed per partittion", []string{"partition"})
	Orderbook_CommitLatencyMs = NewHistogram("commit_latency_ms", "Latency in ms for coordinator offset commit", prometheus.DefBuckets)
	Orderbook_ActiveEpochPendingParticipants = NewGauge("active_epoch_pending_participants", "Workers pending participating per epoch for ack and offset commit")
	Orderbook_CommitActiveEpochs = NewGauge("active_epochs", "Total number of active epochs in coordinator")
	Orderbook_SnapshotRequestsTotal = NewCounterVec("snapshot_req_total", "Total number of snapshot persistence requests per worker", []string{"worker"})
	Orderbook_SnapshotFailuresTotal = NewCounterVec("snapshot_err_total", "Total number of errors occurred during snapshot persistence", []string{"worker"})
	Orderbook_SnapshotSuccessesTotal = NewCounterVec("snapshot_success_total", "Total number of successful persisted snapshots", []string{"worker"})
	Orderbook_EmptySnapshotsTotal = NewCounterVec("empty_snapshots_total", "Total number of empty snapshots", []string{"worker"})
	Orderbook_SnapshotSizeBytes = NewHistogramVec("snapshot_size_bytes", "Varying snapshot byte size per persistence", prometheus.DefBuckets, []string{"worker"})
	Orderbook_BackpressureState = NewGauge("backpressure_state", "Flag which is set when backpressure healthy/suspect/throttling")
	Orderbook_BackpressureTransitionsTotal = NewCounter("backpressure_transitions_total", "Total number of backpressure events")
	Orderbook_ConsumerSuccessesTotal = NewCounterVec("consumer_successes_total", "Total number of successful consumptions", []string{"partition"})
	Orderbook_ConsumerErrorsTotal = NewCounterVec("consumer_errors_total", "Total number of consumption errors", []string{"partition"})
	Orderbook_KafkaFetchPaused = NewGauge("kafka_fetch_paused", "0 -> kafka not paused, 1 -> kafka paused")
	Orderbook_MaxWorkerQueueUsage = NewHistogram("max_worker_queue_usage", "Track max worker queue usage over time", prometheus.DefBuckets)
}
