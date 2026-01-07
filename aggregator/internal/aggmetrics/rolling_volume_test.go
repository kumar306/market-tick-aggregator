package aggmetrics_test

import (
	"market-aggregator/constants"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/proto/generated"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRollingVolume_BucketAdvance(t *testing.T) {
	cfg := &constants.WindowConfig{
		Id:             "5s",
		DurationMs:     5000,
		FlushCadencyMs: 1000,
		BucketSizeMs:   1000, // 1s buckets → 5 buckets
	}

	rv := aggmetrics.NewRollingVolume(cfg)

	t0 := int64(1_000_000)

	tick1 := &generated.NormalizedTick{
		Volume:        10,
		EventTsMillis: t0,
	}

	rv.Update(tick1)
	require.Equal(t, float64(10), rv.RunningTotal)

	tick2 := &generated.NormalizedTick{
		Volume:        15,
		EventTsMillis: t0 + cfg.BucketSizeMs,
	}

	rv.Update(tick2)

	// advance 2 buckets forward (evict first bucket)
	tick3 := &generated.NormalizedTick{
		Volume:        5,
		EventTsMillis: t0 + 5*cfg.BucketSizeMs,
	}

	rv.Update(tick3)

	// first bucket should be overwritten and second bucket is the oldest as of now
	// running total should reflect tick2 and tick3
	require.Equal(t, float64(20), rv.RunningTotal)
}
