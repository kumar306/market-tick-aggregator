package metrics

import "github.com/prometheus/client_golang/prometheus"

var Registry prometheus.Registry = *prometheus.NewRegistry()

func NewCounter(name, help string) prometheus.Counter {
	counter := prometheus.NewCounter(prometheus.CounterOpts{
		Name: name,
		Help: help,
	},
	)
	Registry.MustRegister(counter)
	return counter
}

func NewCounterVec(name, help string, labels []string) *prometheus.CounterVec {
	counterVec := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: name,
		Help: help,
	}, labels)

	Registry.MustRegister(counterVec)
	return counterVec
}

func NewGauge(name, help string) prometheus.Gauge {
	gauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
		})

	Registry.MustRegister(gauge)
	return gauge
}

func NewGaugeVec(name, help string, labels []string) *prometheus.GaugeVec {
	gaugeVec := prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: name,
			Help: help,
		}, labels)

	Registry.MustRegister(gaugeVec)
	return gaugeVec
}

func NewHistogram(name, help string, buckets []float64) prometheus.Histogram {
	histogram := prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	})

	Registry.MustRegister(histogram)
	return histogram
}

func NewHistogramVec(name, help string, buckets []float64, labels []string) *prometheus.HistogramVec {
	histogramVec := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    name,
		Help:    help,
		Buckets: buckets,
	}, labels)

	Registry.MustRegister(histogramVec)
	return histogramVec
}
