package aggmetrics

import (
	"market-aggregator/proto/generated"
	"math"
	"shared/logger"
)

type VWAP struct {
	SumPV float64
	SumV  float64
}

func (v *VWAP) Update(t *generated.NormalizedTick) {
	if math.IsNaN(t.Price) || math.IsInf(t.Price, 0) || math.IsNaN(t.Volume) || math.IsInf(t.Volume, 0) || t.Volume <= 0 {
		return
	}

	v.SumPV += t.Price * t.Volume
	v.SumV += t.Volume
	logger.Log.Info("Updating VWAP", "sum_pv", v.SumPV, "sum_v", v.SumV, "exchange", t.Exchange, "symbol", t.Symbol, "event_time", t.EventTsMillis)
}

func (v *VWAP) Apply(a *generated.AggregatedTick) {
	if v.SumV <= 0 || math.IsNaN(v.SumPV) || math.IsInf(v.SumPV, 0) || math.IsNaN(v.SumV) || math.IsInf(v.SumV, 0) {
		return
	}

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
