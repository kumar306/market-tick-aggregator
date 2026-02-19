package aggmetrics

import (
	"market-aggregator/proto/generated"
	"shared/logger"
)

type VWAP struct {
	SumPV float64
	SumV  float64
}

func (v *VWAP) Update(t *generated.NormalizedTick) {
	v.SumPV += t.Price * t.Volume
	v.SumV += t.Volume
	logger.Log.Info("Updating VWAP", "sum_pv", v.SumPV, "sum_v", v.SumV, "exchange", t.Exchange, "symbol", t.Symbol, "event_time", t.EventTsMillis)
}

func (v *VWAP) Apply(a *generated.AggregatedTick) {
	if a.PriceMetrics == nil {
		a.PriceMetrics = &generated.PriceMetrics{}
	}

	a.PriceMetrics.Vwap = v.SumPV / v.SumV
}

func (v *VWAP) Reset() {
	v.SumPV = 0.0
	v.SumV = 0.0
}

func (v *VWAP) GetValue() float64 {
	return v.SumPV / v.SumV
}
