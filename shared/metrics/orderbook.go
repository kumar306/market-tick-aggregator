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
	Orderbook_BackpressureWorkerQueueUsage   *prometheus.GaugeVec
	Orderbook_BackpressureWorkerPaused       *prometheus.GaugeVec
	Orderbook_KafkaFetchPaused               prometheus.Gauge
	Orderbook_MaxWorkerQueueUsage            *prometheus.HistogramVec
	Orderbook_ConsumerSuccessesTotal         *prometheus.CounterVec
	Orderbook_ConsumerErrorsTotal            *prometheus.CounterVec
)

func InitOrderbookMetrics() {

	Orderbook_UpdatesTotal = NewCounterVec("orderbook_updates_total",
		"Number of orderbook update events per worker", []string{"worker", "exchange", "symbol"})
	Orderbook_WorkerQueueDepth = NewGaugeVec("orderbook_worker_queue_depth", "Worker queue depth used to calculate backpressure", []string{"worker"})
	Orderbook_FlushEpochsTotal = NewCounterVec("orderbook_flush_epochs_total", "Total number of flushed epochs by coordinator", []string{"status"})
	Orderbook_FlushLatencyMs = NewHistogramVec("orderbook_flush_latency_ms", "Latency in ms for worker flush", prometheus.DefBuckets, []string{"worker"})
	Orderbook_FlushSuccessTotal = NewCounterVec("orderbook_flush_success_total", "Total number of successful flushes per worker", []string{"worker"})
	Orderbook_FlushKafkaErrorsTotal = NewCounterVec("orderbook_flush_kafka_errors_total", "Total number of errors occurred during downstream kafka publish", []string{"worker"})
	Orderbook_CommitPartitionOffsets = NewGaugeVec("orderbook_commit_partition_offsets", "Offset committed per partittion", []string{"partition"})
	Orderbook_CommitLatencyMs = NewHistogram("orderbook_commit_latency_ms", "Latency in ms for coordinator offset commit", prometheus.DefBuckets)
	Orderbook_ActiveEpochPendingParticipants = NewGauge("orderbook_active_epoch_pending_participants", "Workers pending participating per epoch for ack and offset commit")
	Orderbook_CommitActiveEpochs = NewGauge("orderbook_active_epochs", "Total number of active epochs in coordinator")
	Orderbook_SnapshotRequestsTotal = NewCounterVec("orderbook_snapshot_req_total", "Total number of snapshot persistence requests per worker", []string{"worker"})
	Orderbook_SnapshotFailuresTotal = NewCounterVec("orderbook_snapshot_err_total", "Total number of errors occurred during snapshot persistence", []string{"worker"})
	Orderbook_SnapshotSuccessesTotal = NewCounterVec("orderbook_snapshot_success_total", "Total number of successful persisted snapshots", []string{"worker"})
	Orderbook_EmptySnapshotsTotal = NewCounterVec("orderbook_empty_snapshots_total", "Total number of empty snapshots", []string{"worker"})
	Orderbook_SnapshotSizeBytes = NewHistogramVec("orderbook_snapshot_size_bytes", "Varying snapshot byte size per persistence", prometheus.DefBuckets, []string{"worker"})
	Orderbook_BackpressureState = NewGauge("orderbook_backpressure_state", "Flag which is set when backpressure healthy/suspect/throttling")
	Orderbook_BackpressureTransitionsTotal = NewCounter("orderbook_backpressure_transitions_total", "Total number of backpressure events")
	Orderbook_BackpressureWorkerQueueUsage = NewGaugeVec("orderbook_backpressure_worker_queue_usage", "Per-worker queue usage used by backpressure", []string{"worker"})
	Orderbook_BackpressureWorkerPaused = NewGaugeVec("orderbook_backpressure_worker_paused_partitions", "Per-worker backpressure pause state, 1 when worker is hot", []string{"worker"})
	Orderbook_ConsumerSuccessesTotal = NewCounterVec("orderbook_consumer_successes_total", "Total number of successful consumptions", []string{"partition"})
	Orderbook_ConsumerErrorsTotal = NewCounterVec("orderbook_consumer_errors_total", "Total number of consumption errors", []string{"partition"})
	Orderbook_KafkaFetchPaused = NewGauge("orderbook_kafka_fetch_paused", "0 -> kafka not paused, 1 -> kafka paused")
	Orderbook_MaxWorkerQueueUsage = NewHistogramVec("orderbook_max_worker_queue_usage", "Track max worker queue usage over time", prometheus.DefBuckets, []string{"worker"})
}
