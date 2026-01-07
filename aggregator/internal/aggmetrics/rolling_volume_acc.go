package aggmetrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
)

// for rolling volume and volume acceleration metrics

type RollingVolume struct {
	VolumeBuckets    []float64
	Idx              int64
	RunningTotal     float64
	PrevRunningTotal float64
	BucketSizeMs     int64
	LastBucketTsMs   int64
	FlushCadencyMs   int64
}

func NewRollingVolume(cfg *constants.WindowConfig) *RollingVolume {
	bucketsLength := cfg.DurationMs / cfg.BucketSizeMs
	return &RollingVolume{
		Idx:            0,
		VolumeBuckets:  make([]float64, bucketsLength),
		BucketSizeMs:   cfg.BucketSizeMs,
		FlushCadencyMs: cfg.FlushCadencyMs,
	}
}

// create this rolling volume object with number of buckets - calculated based on
// duration ms / bucket size ms
func (r *RollingVolume) Update(t *generated.NormalizedTick) {
	now := t.EventTsMillis

	// first tick
	if r.LastBucketTsMs == 0 {
		r.LastBucketTsMs = now
	}

	elapsed := now - r.LastBucketTsMs
	steps := elapsed / r.BucketSizeMs

	for i := int64(0); i < steps; i++ {
		// move to next bucket
		r.Idx = (r.Idx + 1) % int64(len(r.VolumeBuckets))
		r.RunningTotal -= r.VolumeBuckets[r.Idx]
		r.VolumeBuckets[r.Idx] = 0.0
	}

	r.LastBucketTsMs += steps * r.BucketSizeMs
	r.VolumeBuckets[r.Idx] += t.Volume
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

func (r *RollingVolume) GetValue() float64 {
	return r.RunningTotal
}
