package aggmetrics

import (
	"market-aggregator/proto/generated"
)

type OHLC struct {
	OpenSet bool
	Open    float64
	High    float64
	Low     float64
	Close   float64
}

func (o *OHLC) Update(t *generated.NormalizedTick) {

	if !o.OpenSet {
		o.Open = t.Open
		o.Low = t.Low
		o.High = t.High
		o.OpenSet = true
	} else {
		o.Low = min(o.Low, t.Low)
		o.High = max(o.High, t.High)
	}

	o.Close = t.Close
}

func (o *OHLC) Apply(target *generated.AggregatedTick) {
	if target.PriceMetrics == nil {
		target.PriceMetrics = &generated.PriceMetrics{}
	}
	if target.PriceMetrics.Ohlc == nil {
		target.PriceMetrics.Ohlc = &generated.OHLC{}
	}
	target.PriceMetrics.Ohlc.Open = o.Open
	target.PriceMetrics.Ohlc.Close = o.Close
	target.PriceMetrics.Ohlc.Low = o.Low
	target.PriceMetrics.Ohlc.High = o.High
}

func (o *OHLC) Reset() {
	o.Low = 0.0
	o.High = 0.0
	o.Close = 0.0
	o.Open = 0.0
	o.OpenSet = false
}
