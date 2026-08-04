package aggmetrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
	"math"
)

// this is also a depiction of price fluctuation
// calc true range (TR) which is max(today high - today low, abs(today high - yesterday close), abs(today low - yesterday close))
// atr = ((N-1)th ATR + TR)/N - where N time periods occurred

// which (1 - 1/N)ATR + (1/N)TR
// we can approximate ATR to TR as they will smoothen out
// if 1/N is our alpha its like
// alpha*tr + (1-alpha)*ATR - which is decayed rolling

type ATR struct {
	Value     float64
	Alpha     float64
	PrevClose float64
	Init      bool
}

// use between 10-20. 10 is more reactive, 20 is more smooth
func NewATR(cfg *constants.WindowConfig) *ATR {
	alpha := 1.0 / float64(14)
	return &ATR{
		Alpha: alpha,
	}
}

func (atr *ATR) Update(t *generated.NormalizedTick) {
	if !atr.Init {
		atr.Value = t.High - t.Low
		atr.PrevClose = t.Close
		atr.Init = true
		return
	}

	// calc true range
	tr := math.Max(t.High-t.Low, math.Max(
		math.Abs(t.High-atr.PrevClose),
		math.Abs(t.Low-atr.PrevClose),
	))

	atr.PrevClose = t.Close
	atr.Value = atr.Alpha*tr + (1-atr.Alpha)*atr.Value
}

func (atr *ATR) Apply(a *generated.AggregatedTick) {
	if a.VolatilityMetrics == nil {
		a.VolatilityMetrics = &generated.VolatilityMetrics{}
	}

	a.VolatilityMetrics.Atr = atr.Value
}

// no-op as decayed rolling
func (atr *ATR) Reset() {

}

func (atr *ATR) GetValue() float64 {
	return atr.Value
}
