package internal

import (
	"market-aggregator/constants"
	"market-aggregator/internal/metrics"
	"shared/logger"
	"sync"
)

type MetricCtor func(*constants.WindowConfig) constants.Metric

var MetricCtorRegistry = make(map[constants.MetricName]MetricCtor)
var MetricRegisterer sync.Once

const (
	OhlcKey        constants.MetricName = "ohlc"
	RollingVWAPKey constants.MetricName = "rollingVWAP"
)

// add the things into map - struct of name, function
func InitMetricRegistry() {
	MetricRegisterer.Do(func() {
		pairs := []struct {
			id   constants.MetricName
			ctor MetricCtor
		}{
			{OhlcKey, func(cfg *constants.WindowConfig) constants.Metric {
				return &metrics.OHLC{}
			}},
			{RollingVWAPKey, func(cfg *constants.WindowConfig) constants.Metric {
				return metrics.NewRollingVWAP(cfg)
			}},
		}

		for _, pair := range pairs {
			MetricCtorRegistry[pair.id] = pair.ctor
			logger.Log.Info("Registered metric constructor for metric", "metric", pair.id)
		}
	})
}
