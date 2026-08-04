// rolling_vwap.go
package aggmetrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
)

type VWAPBucket struct {
	SumPV float64
	SumV  float64
}

type RollingVWAP struct {
	Buckets        []VWAPBucket
	Idx            int64
	BucketSizeMs   int64
	LastBucketTsMs int64
	TotalSumPV     float64
	TotalSumV      float64
}

func NewRollingVWAP(cfg *constants.WindowConfig) constants.Metric {
	if cfg.BucketSizeMs <= 0 {
		panic("Invalid bucket size configuration")
	}

	bucketsSize := cfg.DurationMs / cfg.BucketSizeMs
	if bucketsSize <= 0 {
		panic("Invalid bucket configuration")
	}

	return &RollingVWAP{
		Buckets:        make([]VWAPBucket, bucketsSize),
		Idx:            0,
		BucketSizeMs:   cfg.BucketSizeMs,
		LastBucketTsMs: 0,
	}
}

func (r *RollingVWAP) Update(t *generated.NormalizedTick) {
	now := t.EventTsMillis
	if r.LastBucketTsMs == 0 {
		r.LastBucketTsMs = now
	}

	elapsed := now - r.LastBucketTsMs

	if elapsed >= r.BucketSizeMs {
		steps := elapsed / r.BucketSizeMs
		for i := int64(0); i < steps; i++ {
			r.Idx = (r.Idx + 1) % int64(len(r.Buckets))
			r.TotalSumPV -= r.Buckets[r.Idx].SumPV
			r.TotalSumV -= r.Buckets[r.Idx].SumV
			r.Buckets[r.Idx] = VWAPBucket{}
		}
		r.LastBucketTsMs += steps * r.BucketSizeMs
	}

	b := &r.Buckets[r.Idx]
	b.SumPV += t.Price * t.Volume
	b.SumV += t.Volume

	r.TotalSumPV += t.Price * t.Volume
	r.TotalSumV += t.Volume
}

func (r *RollingVWAP) Apply(target *generated.AggregatedTick) {
	if target.PriceMetrics == nil {
		target.PriceMetrics = &generated.PriceMetrics{}
	}

	vwap := 0.0
	if r.TotalSumV > 0 {
		vwap = r.TotalSumPV / r.TotalSumV
	}

	target.PriceMetrics.RollingVwap = vwap
}

func (r *RollingVWAP) Reset() {
	// rolling metric so no-op
}

func (r *RollingVWAP) GetValue() float64 {
	return r.TotalSumPV / r.TotalSumV
}
