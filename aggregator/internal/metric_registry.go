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
	OhlcKey          constants.MetricName = "ohlc"
	RollingVWAPKey   constants.MetricName = "rollingVWAP"
	RollingVolumeKey constants.MetricName = "rollingVolume"
	AtrKey           constants.MetricName = "atr"
	TwapSmaKey       constants.MetricName = "twapSma"
	VolatilityKey    constants.MetricName = "volatility"
	VolumeKey        constants.MetricName = "volume"
	VwapKey          constants.MetricName = "vwap"
	ReturnsKey       constants.MetricName = "returns"
	EmaKey           constants.MetricName = "ema"
)

// add the things into map - struct of name, function
func InitMetricRegistry() {
	MetricRegisterer.Do(func() {
		pairs := []struct {
			id   constants.MetricName
			ctor MetricCtor
		}{
			{OhlcKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.OHLC{}
			}},
			{VwapKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.VWAP{}
			}},
			{TwapSmaKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.Returns{}
			}},
			{RollingVWAPKey, func(wc *constants.WindowConfig) constants.Metric {
				return metrics.NewRollingVWAP(wc)
			}},
			{VolumeKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.Volume{}
			}},
			{RollingVolumeKey, func(wc *constants.WindowConfig) constants.Metric {
				return metrics.NewRollingVolume(wc)
			}},
			{VolatilityKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.Volatility{}
			}},
			{AtrKey, func(wc *constants.WindowConfig) constants.Metric {
				return metrics.NewATR(wc)
			}},
			{EmaKey, func(wc *constants.WindowConfig) constants.Metric {
				return metrics.NewEMA(wc)
			}},
			{ReturnsKey, func(wc *constants.WindowConfig) constants.Metric {
				return &metrics.Returns{}
			}},
		}

		for _, pair := range pairs {
			MetricCtorRegistry[pair.id] = pair.ctor
			logger.Log.Info("Registered metric constructor for metric", "metric", pair.id)
		}
	})
}
