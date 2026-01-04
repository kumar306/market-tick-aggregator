package metrics

import (
	"market-aggregator/constants"
	"market-aggregator/proto/generated"
)

type VWAPBucket struct {
	SumPV float64
	SumV  float64
}

type RollingVWAP struct {
	buckets        []VWAPBucket
	idx            int64
	bucketSizeMs   int64
	lastBucketTsMs int64
	totalSumPV     float64
	totalSumV      float64
}

func NewRollingVWAP(cfg *constants.WindowConfig) constants.Metric {
	bucketSizeMs := cfg.DurationMs / 100
	if bucketSizeMs <= 0 {
		bucketSizeMs = 1000
	}

	bucketsSize := cfg.DurationMs / cfg.BucketSizeMs

	if bucketsSize <= 0 {
		panic("Invalid bucket configuration")
	}

	return &RollingVWAP{
		buckets:        make([]VWAPBucket, bucketsSize),
		idx:            0,
		bucketSizeMs:   bucketSizeMs,
		lastBucketTsMs: 0,
	}
}

func (r *RollingVWAP) Update(t *generated.NormalizedTick) {
	now := t.EventTsMillis
	if r.lastBucketTsMs == 0 {
		r.lastBucketTsMs = now
	}

	elapsed := now - r.lastBucketTsMs

	// advance buckets if bucket size crossed
	if elapsed >= r.bucketSizeMs {
		steps := elapsed / r.bucketSizeMs
		for i := int64(0); i < steps; i++ {
			r.idx = (r.idx + 1) % int64(len(r.buckets))

			// maintain a rolling total and subtract oldest buckets value
			// to prevent O(n) on every apply
			r.totalSumPV -= r.buckets[r.idx].SumPV
			r.totalSumV -= r.buckets[r.idx].SumV

			r.buckets[r.idx] = VWAPBucket{}
		}

		r.lastBucketTsMs += steps * r.bucketSizeMs
	}

	b := &r.buckets[r.idx]
	b.SumPV += t.Price * t.Volume
	b.SumV += t.Volume

	r.totalSumPV += t.Price * t.Volume
	r.totalSumV += t.Volume
}

func (r *RollingVWAP) Apply(target *generated.AggregatedTick) {
	if target.PriceMetrics == nil {
		target.PriceMetrics = &generated.PriceMetrics{}
	}

	vwap := 0.0
	if r.totalSumV > 0 {
		vwap = r.totalSumPV / r.totalSumV
	}

	target.PriceMetrics.RollingVwap = vwap
}

func (r *RollingVWAP) Reset() {
	// rolling metric so no-op
}
