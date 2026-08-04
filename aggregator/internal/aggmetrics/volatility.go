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
	N         int64
	M2        float64
	Mean      float64
	Init      bool
	PrevPrice float64
}

func (v *Volatility) Update(t *generated.NormalizedTick) {
	price := t.Price
	if math.IsNaN(price) || math.IsInf(price, 0) || price <= 0 {
		return
	}

	if !v.Init {
		v.PrevPrice = price
		v.Init = true
		return
	}

	if math.IsNaN(v.PrevPrice) || math.IsInf(v.PrevPrice, 0) || v.PrevPrice <= 0 {
		v.PrevPrice = price
		return
	}

	ret := math.Log(t.Price / v.PrevPrice)
	v.PrevPrice = price

	v.N++
	delta := ret - v.Mean
	v.Mean += delta / float64(v.N)
	delta2 := ret - v.Mean
	v.M2 += delta * delta2
}

func (v *Volatility) Apply(a *generated.AggregatedTick) {
	// return if n < 2 as doing  /n-1
	if v.N < 2 {
		return
	}

	volatility := math.Sqrt(v.M2 / float64(v.N-1))
	if math.IsNaN(volatility) || math.IsInf(volatility, 0) {
		return
	}

	if a.VolatilityMetrics == nil {
		a.VolatilityMetrics = &generated.VolatilityMetrics{}
	}

	a.VolatilityMetrics.Volatility = volatility
}

func (v *Volatility) Reset() {
	v.N = 0
	v.Mean = 0
	v.Init = false
	v.M2 = 0
	v.PrevPrice = 0
}

func (v *Volatility) GetValue() float64 {
	if v.N < 2 {
		// for testing purpose. this is not invoked in normal flow
		v.N = 2
	}

	return math.Sqrt(v.M2 / float64(v.N-1))
}
