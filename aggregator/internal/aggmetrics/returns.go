package aggmetrics

import (
	"market-aggregator/proto/generated"
	"math"
)

// calculate how much the price moved in the current window
// simple returns and log returns give accurate depiction of price shift
// assume Pt is price at time t and Pt-1 is price at time t-1 i.e price at last tick
// simple return = (Pt - Pt-1)/Pt-1
// log return = ln(Pt/Pt-1)

type Returns struct {
	PrevPrice    float64
	LogReturn    float64
	SimpleReturn float64
}

func (r *Returns) Update(t *generated.NormalizedTick) {
	if math.IsNaN(t.Price) || math.IsInf(t.Price, 0) || t.Price <= 0 {
		return
	}

	if r.PrevPrice == 0 {
		r.PrevPrice = t.Price
		return
	}

	if math.IsNaN(r.PrevPrice) || math.IsInf(r.PrevPrice, 0) || r.PrevPrice <= 0 {
		r.PrevPrice = t.Price
		return
	}

	r.SimpleReturn += (t.Price - r.PrevPrice) / r.PrevPrice
	r.LogReturn += math.Log(t.Price / r.PrevPrice)
	r.PrevPrice = t.Price
}

func (r *Returns) Apply(a *generated.AggregatedTick) {
	if math.IsNaN(r.SimpleReturn) || math.IsInf(r.SimpleReturn, 0) || math.IsNaN(r.LogReturn) || math.IsInf(r.LogReturn, 0) {
		return
	}

	if a.TrendMetrics == nil {
		a.TrendMetrics = &generated.TrendMetrics{}
	}

	a.TrendMetrics.SimpleReturn = r.SimpleReturn
	a.TrendMetrics.LogReturn = r.LogReturn
}

func (r *Returns) Reset() {
	r.PrevPrice = 0.0
	r.LogReturn = 0.0
	r.SimpleReturn = 0.0
}

func (r *Returns) GetValue() float64 {
	return r.LogReturn
}
