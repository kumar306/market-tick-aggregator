package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	Adapter_BuildInfo            *prometheus.GaugeVec
	Adapter_AppShutdowns         prometheus.Counter
	Adapter_AppStarts            prometheus.Counter
	Adapter_FeedConnections      *prometheus.CounterVec
	Adapter_FeedErrors           *prometheus.CounterVec
	Adapter_SupervisorCount      prometheus.Gauge
	Adapter_SupervisorGoroutines *prometheus.GaugeVec
	Adapter_LastPongTimes        *prometheus.GaugeVec

	// ring buffer metrics
	Adapter_BufferCapacity   *prometheus.GaugeVec
	Adapter_BufferLen        *prometheus.GaugeVec
	Adapter_BufferDrops      *prometheus.CounterVec
	Adapter_KafkaPublishes   *prometheus.CounterVec
	Adapter_NormalizerErrors *prometheus.CounterVec
)

func InitAdapterMetrics() {
	Adapter_BuildInfo = NewGaugeVec("build_info",
		"Build Info with Version and Commit SHA",
		[]string{"version", "commit"})

	Adapter_AppShutdowns = NewCounter("app_shutdowns",
		"Number of application shutdowns")

	Adapter_AppStarts = NewCounter("app_starts",
		"Number of application starts")

	Adapter_FeedConnections = NewCounterVec("feed_connections_total",
		"Total number of successful connections",
		[]string{"feed_name"})

	Adapter_FeedErrors = NewCounterVec("feed_errors_total",
		"Total number of connection errors",
		[]string{"feed_name"})

	Adapter_SupervisorCount = NewGauge("supervisor_count",
		"Total number of active supervisors")

	Adapter_SupervisorGoroutines = NewGaugeVec("supervisor_goroutines",
		"Total number of goroutines per supervisor",
		[]string{"feed_name"})

	Adapter_LastPongTimes = NewGaugeVec("last_pong_time",
		"Last pong time per feed",
		[]string{"feed_name"})

	// ring buffer metrics
	Adapter_BufferCapacity = NewGaugeVec("buffer_capacity",
		"Buffer capacity per feed",
		[]string{"feed_name"})

	Adapter_BufferLen = NewGaugeVec("buffer_len",
		"Buffer length per feed",
		[]string{"feed_name"})

	Adapter_BufferDrops = NewCounterVec("buffer_drops",
		"Buffer drops per feed",
		[]string{"feed_name"})

	Adapter_KafkaPublishes = NewCounterVec("kafka_publishes",
		"Kafka Publishes per stream",
		[]string{"stream"})

	Adapter_NormalizerErrors = NewCounterVec("normalizer_errors",
		"Normalizer errors per stream",
		[]string{"stream"})
}
