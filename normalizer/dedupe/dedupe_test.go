package dedupe_test

import (
	"context"
	"market-normalizer/dedupe"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
)

func TestMarkForDedupe(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	m, err := miniredis.Run()
	require.NoError(t, err)
	defer m.Close()

	dedupe.Rdb = redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	dedupe.Ttl = 2 * time.Minute
	dedupe.RedisBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "redis-dedupe-breaker",
		ReadyToTrip: func(counts gobreaker.Counts) bool {

			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= 0.5
		},
		Timeout: time.Duration(1000) * time.Millisecond,
	})

	key := dedupe.ConstructDedupeKey("coinbase.ticks", 1, 3)
	dedupeErr := dedupe.MarkForDedupe(ctx, key)
	require.NoError(t, dedupeErr)

	// validate key exists
	exists := m.Exists(key)
	require.True(t, exists, "mark for dedupe should store the key")

	ttl := m.TTL(key)
	require.True(t, ttl > 0, "TTL should be set for key in redis")
}

// unit test for is duplicate check. first key not there so isDuplicate false. then mark for dedupe and call is duplicate again and verify its true
func TestIsDuplicateDetection(t *testing.T) {
	ctx := context.Background()

	m, err := miniredis.Run()
	require.NoError(t, err)
	defer m.Close()

	dedupe.Rdb = redis.NewClient(&redis.Options{
		Addr: m.Addr(),
	})
	dedupe.Ttl = time.Minute
	dedupe.RedisBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "redis-dedupe-breaker",
		ReadyToTrip: func(counts gobreaker.Counts) bool {

			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= 0.5
		},
		Timeout: time.Duration(1000) * time.Millisecond,
	})
	dedupe.TestingHook = nil

	key := dedupe.ConstructDedupeKey("kraken.book", 0, 30)

	dup, err := dedupe.IsDuplicate(ctx, key)
	require.NoError(t, err)
	require.False(t, dup)

	err = dedupe.MarkForDedupe(ctx, key)
	require.NoError(t, err)

	dup, err = dedupe.IsDuplicate(ctx, key)
	require.NoError(t, err)
	require.True(t, dup, "Duplicate must be detected")
}
