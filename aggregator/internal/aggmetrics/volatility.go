package aggmetrics

import (
	"market-aggregator/proto/generated"
	"math"
)

// it is a depiction of price fluctuation, not direction
// its the sqrt of summed squared deviations from mean
// its a tumbling metric

// use log returns as the value taken in as its easier to compare across symbols instead of taking raw price value

type Volatility struct {
	n         int64
	m2        float64
	mean      float64
	init      bool
	prevPrice float64
}

func (v *Volatility) Update(t *generated.NormalizedTick) {
	price := t.Price
	if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return
	}

	if !v.init {
		v.prevPrice = price
		v.init = true
		return
	}

	if math.IsNaN(v.prevPrice) || math.IsInf(v.prevPrice, 0) || v.prevPrice <= 0 {
		v.prevPrice = price
		return
	}

	ret := math.Log(t.Price / v.prevPrice)
	v.prevPrice = price

	v.n++
	delta := ret - v.mean
	v.mean += delta / float64(v.n)
	delta2 := ret - v.mean
	v.m2 += delta * delta2
}

func (v *Volatility) Apply(a *generated.AggregatedTick) {
	// return if n < 2 as doing  /n-1
	if v.n < 2 {
		return
	}

	volatility := math.Sqrt(v.m2 / float64(v.n-1))
	if math.IsNaN(volatility) || math.IsInf(volatility, 0) {
		return
	}

	if a.VolatilityMetrics == nil {
		a.VolatilityMetrics = &generated.VolatilityMetrics{}
	}

	a.VolatilityMetrics.Volatility = volatility
}

func (v *Volatility) Reset() {
	v.n = 0
	v.mean = 0
	v.init = false
	v.m2 = 0
	v.prevPrice = 0
}

func (v *Volatility) GetValue() float64 {
	if v.n < 2 {
		// for testing purpose. this is not invoked in normal flow
		v.n = 2
	}

	return math.Sqrt(v.m2 / float64(v.n-1))
}
