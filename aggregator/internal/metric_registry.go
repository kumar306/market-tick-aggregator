package internal

import (
	"market-aggregator/constants"
	"market-aggregator/internal/aggmetrics"
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
				return &aggmetrics.OHLC{}
			}},
			{VwapKey, func(wc *constants.WindowConfig) constants.Metric {
				return &aggmetrics.VWAP{}
			}},
			{TwapSmaKey, func(wc *constants.WindowConfig) constants.Metric {
				return &aggmetrics.Returns{}
			}},
			{RollingVWAPKey, func(wc *constants.WindowConfig) constants.Metric {
				return aggmetrics.NewRollingVWAP(wc)
			}},
			{VolumeKey, func(wc *constants.WindowConfig) constants.Metric {
				return &aggmetrics.Volume{}
			}},
			{RollingVolumeKey, func(wc *constants.WindowConfig) constants.Metric {
				return aggmetrics.NewRollingVolume(wc)
			}},
			{VolatilityKey, func(wc *constants.WindowConfig) constants.Metric {
				return &aggmetrics.Volatility{}
			}},
			{AtrKey, func(wc *constants.WindowConfig) constants.Metric {
				return aggmetrics.NewATR(wc)
			}},
			{EmaKey, func(wc *constants.WindowConfig) constants.Metric {
				return aggmetrics.NewEMA(wc)
			}},
			{ReturnsKey, func(wc *constants.WindowConfig) constants.Metric {
				return &aggmetrics.Returns{}
			}},
		}

		for _, pair := range pairs {
			MetricCtorRegistry[pair.id] = pair.ctor
		}
	})
}
