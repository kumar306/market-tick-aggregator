package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// kafka consume metrics
	Persistence_KafkaRecordsConsumed *prometheus.CounterVec
	Persistence_KafkaErrorsTotal     *prometheus.CounterVec
	Persistence_ConsumerLag          *prometheus.GaugeVec

	// pipeline/batching metrics - to find out whether normal or timer flush dominates - accordingly tune batch size
	Persistence_BatchSize          *prometheus.HistogramVec
	Persistence_FlushCount         *prometheus.CounterVec
	Persistence_BatchFlushDuration *prometheus.HistogramVec

	// database metrics
	Persistence_DbRowsWritten *prometheus.CounterVec
	Persistence_TxnFailures   *prometheus.CounterVec
	Persistence_TxnDuration   *prometheus.HistogramVec

	// offset metrics
	Persistence_OffsetCommitAttempts *prometheus.CounterVec
	Persistence_OffsetCommitSuccess  *prometheus.CounterVec
	Persistence_OffsetCommitFailures *prometheus.CounterVec
	Persistence_OffsetCommitted      *prometheus.GaugeVec

	// backpressure metrics
	Persistence_BatchQueueDepth    *prometheus.GaugeVec
	Persistence_BatchDroppedItems  *prometheus.CounterVec
	Persistence_BatchDroppedTimers *prometheus.CounterVec
)

func InitPersistenceMetrics() {
	Persistence_KafkaRecordsConsumed = NewCounterVec("kafka_records_consumed", "Number of kafka records consumed", []string{"topic", "partition"})
	Persistence_KafkaErrorsTotal = NewCounterVec("kafka_errors", "Total number of kafka errors occurred during consumption", []string{"topic", "partition"})
	Persistence_ConsumerLag = NewGaugeVec("kafka_consumer_lag", "Kafka consumer lag per partition", []string{"topic", "partition"})
	Persistence_BatchSize = NewHistogramVec("pipeline_batch_size", "Batch size plot for each pipeline", prometheus.LinearBuckets(0, 1, 100), []string{"pipeline"})
	Persistence_FlushCount = NewCounterVec("pipeline_flush_count", "Number of flushes per pipeline", []string{"pipeline"})
	Persistence_BatchFlushDuration = NewHistogramVec("pipeline_batch_flush_duration", "Duration of batch flush per pipeline", prometheus.DefBuckets, []string{"pipeline"})
	Persistence_DbRowsWritten = NewCounterVec("db_rows_written", "Number of rows written per pipeline", []string{"pipeline"})
	Persistence_TxnFailures = NewCounterVec("txn_failures", "Number of txn failures per pipeline", []string{"pipeline"})
	Persistence_TxnDuration = NewHistogramVec("txn_duration", "Duration of txn per pipeline", prometheus.DefBuckets, []string{"pipeline"})
	Persistence_OffsetCommitAttempts = NewCounterVec("offset_commit_attempts", "Number of offset commit attempts per pipeline", []string{"pipeline"})
	Persistence_OffsetCommitFailures = NewCounterVec("offset_commit_failures", "Number of offset commit attempt failures per pipeline", []string{"pipeline"})
	Persistence_OffsetCommitSuccess = NewCounterVec("offset_commit_successes", "Number of offset commit attempt successes", []string{"pipeline"})
	Persistence_OffsetCommitted = NewGaugeVec("offset_committed_per_topic_partition", "Offset committed per topic and partition", []string{"topic", "partition"})
	Persistence_BatchQueueDepth = NewGaugeVec("batch_queue_depth", "Queue depth per pipeline", []string{"pipeline"})
	Persistence_BatchDroppedItems = NewCounterVec("batch_dropped_items", "Number of dropped items for batcher add", []string{"pipeline"})
	Persistence_BatchDroppedTimers = NewCounterVec("batch_dropped_timer_events", "Number of dropped timer events for batcher", []string{"pipeline"})
}
