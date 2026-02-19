package aggmetrics

import (
	"market-aggregator/proto/generated"
	"shared/logger"
)

type Volume struct {
	Volume float64
}

func (v *Volume) Update(tick *generated.NormalizedTick) {
	v.Volume += tick.Volume
	logger.Log.Info("Updating volume", "volume", v.Volume, "exchange", tick.Exchange, "symbol", tick.Symbol, "event_time", tick.EventTsMillis)
}

func (v *Volume) Apply(a *generated.AggregatedTick) {
	if a.VolumeMetrics == nil {
		a.VolumeMetrics = &generated.VolumeMetrics{}
	}
	a.VolumeMetrics.Volume = v.Volume
}

func (v *Volume) Reset() {
	v.Volume = 0.0
}

func (v *Volume) GetValue() float64 {
	return v.Volume
}
