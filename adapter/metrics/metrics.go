package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	BuildInfo = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "build_info",
			Help: "Build Info with Version and Commit SHA",
		},
		[]string{"version", "commit"},
	)

	AppStarts = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "app_starts",
			Help: "Number of application startups",
		},
	)

	AppShutdowns = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "app_shutdowns",
			Help: "Number of application shutdowns",
		},
	)

	FeedConnections = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feed_connections_total",
			Help: "Total number of successful connections",
		},
		[]string{"feed_name"},
	)

	FeedErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "feed_errors_total",
			Help: "Total number of connection errors",
		},
		[]string{"feed_name"},
	)

	Supervisors = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "supervisor_count",
			Help: "Total number of active supervisors",
		},
	)

	SupervisorGoroutines = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "supervisor_goroutines",
			Help: "Total number of goroutines per supervisor",
		},
		[]string{"feed_name"},
	)

	LastPongTimes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "last_pong_time",
			Help: "Last pong time per feed",
		},
		[]string{"feed_name"},
	)

	// adapter ring buffer metrics
	BufferCapacity = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "buffer_capacity",
			Help: "Buffer capacity per feeed",
		},
		[]string{"feed_name"},
	)

	BufferLen = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "buffer_len",
			Help: "Buffer length per feed",
		},
		[]string{"feed_name"},
	)

	BufferDrops = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "buffer_drops",
			Help: "Buffer drops per feed",
		},
		[]string{"feed_name"},
	)

	KafkaPublishes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "kafka_publishes",
			Help: "Kafka Publishes per stream",
		},
		[]string{"stream"},
	)

	NormalizerErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "normalizer_errors",
			Help: "Normalizer errors per stream",
		},
		[]string{"stream"},
	)
)

func Init() {
	prometheus.MustRegister(
		BuildInfo,
		AppStarts,
		AppShutdowns,
		FeedConnections,
		FeedErrors,
		Supervisors,
		SupervisorGoroutines,
		LastPongTimes,
		BufferCapacity,
		BufferLen,
		BufferDrops,
		KafkaPublishes,
		NormalizerErrors)

	// metric to track current version, commit SHA
	BuildInfo.WithLabelValues("v1.0.0", "abc123").Set(1)
}
