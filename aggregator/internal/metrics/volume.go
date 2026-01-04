package metrics

import "market-aggregator/proto/generated"

type Volume struct {
	Volume float64
}

func (v *Volume) Update(tick *generated.NormalizedTick) {
	v.Volume += tick.Volume
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
