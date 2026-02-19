package aggmetrics

import (
	"market-aggregator/proto/generated"
	"shared/logger"
)

type TWAP struct {
	SumPrice float64
	Count    float64
}

func (t *TWAP) Update(tick *generated.NormalizedTick) {
	t.SumPrice += tick.Price
	t.Count++
	logger.Log.Info("Updating TWAP SMA", "sum_price", t.SumPrice, "count", t.Count, "exchange", tick.Exchange, "symbol", tick.Symbol, "event_time", tick.EventTsMillis)
}

func (t *TWAP) Apply(a *generated.AggregatedTick) {
	if a.PriceMetrics == nil {
		a.PriceMetrics = &generated.PriceMetrics{}
	}

	if a.TrendMetrics == nil {
		a.TrendMetrics = &generated.TrendMetrics{}
	}

	if t.Count == 0 {
		a.PriceMetrics.Twap = 0
	} else {
		a.PriceMetrics.Twap = t.SumPrice / t.Count
		// sma and twap are the same thing used for different downstreams
		a.TrendMetrics.Sma = a.PriceMetrics.Twap
	}
}

func (t *TWAP) Reset() {
	t.SumPrice = 0
	t.Count = 0
}

func (t *TWAP) GetValue() float64 {
	return t.SumPrice / t.Count
}
