package metrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
)

// for rolling volume and volume acceleration metrics

type RollingVolume struct {
	VolumeBuckets    []float64
	idx              int64
	RunningTotal     float64
	PrevRunningTotal float64
	BucketSizeMs     int64
	LastBucketTsMs   int64
	FlushCadencyMs   int64
}

func CreateRollingVolume(cfg *constants.WindowConfig) *RollingVolume {
	bucketsLength := cfg.DurationMs / cfg.BucketSizeMs
	return &RollingVolume{
		idx:            0,
		VolumeBuckets:  make([]float64, bucketsLength),
		BucketSizeMs:   cfg.BucketSizeMs,
		FlushCadencyMs: cfg.FlushCadencyMs,
	}
}

// create this rolling volume object with number of buckets - calculated based on
// duration ms / bucket size ms
func (r *RollingVolume) Update(t *generated.NormalizedTick) {
	now := t.EventTsMillis

	elapsed := now - r.LastBucketTsMs
	steps := elapsed / r.BucketSizeMs

	for i := int64(0); i < steps; i++ {
		// move to next bucket
		r.idx = (r.idx + 1) % int64(len(r.VolumeBuckets))
		r.RunningTotal -= r.VolumeBuckets[r.idx]
		r.VolumeBuckets[r.idx] = 0.0
	}

	r.LastBucketTsMs += steps * r.BucketSizeMs
	r.VolumeBuckets[r.idx] += t.Volume
	r.RunningTotal += t.Volume
}

func (r *RollingVolume) Apply(a *generated.AggregatedTick) {
	if a.VolumeMetrics == nil {
		a.VolumeMetrics = &generated.VolumeMetrics{}
	}

	a.VolumeMetrics.RollingVolume = r.RunningTotal
	if r.PrevRunningTotal > 0.0 {
		// volume acceleration = change in rolling volume per second in between flushes
		a.VolumeMetrics.VolumeAcceleration = (r.RunningTotal - r.PrevRunningTotal) / float64(r.FlushCadencyMs/1000.0)
	}

	r.PrevRunningTotal = r.RunningTotal
}

// no-op as its rolling
func (r *RollingVolume) Reset() {
}
