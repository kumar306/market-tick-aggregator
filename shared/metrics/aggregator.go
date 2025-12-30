package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	Aggregator_ConsumerErrorsTotal   *prometheus.CounterVec
	Aggregator_ConsumerMessagesTotal *prometheus.CounterVec
)
