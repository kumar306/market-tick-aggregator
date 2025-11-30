package dedupe

import (
	"context"
	"errors"
	"market-normalizer/constants"
	"os"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
)

// use redis to store dedupe keys - set key if not exists - do it at the end of publish
// at the start of pipeline, check if key present. if its there, then skip

// init redis
var Rdb *redis.Client
var Ttl time.Duration
var RedisBreaker *gobreaker.CircuitBreaker
var TestingHook func() error

const (
	REDIS_ADDR     string = "REDIS_ADDR"
	REDIS_PASSWORD string = "REDIS_PASSWORD"
)

func InitRedis(redisConfig *constants.RedisConfig) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv(REDIS_ADDR),
		Password: os.Getenv(REDIS_PASSWORD),
		DB:       0,
	})
	Ttl = time.Duration(redisConfig.TtlMinutes) * time.Minute

	RedisBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "redis-dedupe-breaker",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.Requests < redisConfig.CBReqCount {
				return false
			}
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return failureRatio >= redisConfig.CBFailureRatio
		},
		Timeout: time.Duration(redisConfig.CBTimeoutMillis) * time.Millisecond,
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Log.Warn("Redis circuit breaker state change", "name", name, "from", from.String(), "to", to.String())
			metrics.Normalizer_RedisCB_StateChanges.WithLabelValues(to.String()).Inc()
			metrics.Normalizer_RedisCB_State.Set(float64(to))
		},
	})

	logger.Log.Info("Initialised Redis Client")
}

func ConstructDedupeKey(exchange, channel, symbol string, orderingID string) string {
	return exchange + ":" + channel + ":" + symbol + ":" + orderingID
}

// set the dedupe key in redis with TTL
func MarkForDedupe(ctx context.Context, key string) error {

	// only during test - for mock dedupe in worker pipeline
	if TestingHook != nil {
		return TestingHook()
	}

	ok, err := RedisBreaker.Execute(func() (interface{}, error) {
		return Rdb.SetNX(ctx, key, 1, Ttl).Result()
	})

	if err != nil {

		// don't stop pipeline processing if circuit is open
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			metrics.Normalizer_RedisCB_FallbacksTotal.Inc()
			logger.Log.Error("Dedupe MarkForDedupe skipped as circuit breaker - OPEN state", "err", err)
			return nil
		}

		return logger.LogAndWrap("Error when setting dedupe key in redis", err, "key", key)
	}

	if !ok.(bool) {
		// key exists
		logger.Log.Warn("Key already exists in redis", "key", key)
	}

	return nil
}

func IsDuplicate(ctx context.Context, key string) (bool, error) {

	// only during test - for mock dedupe
	if TestingHook != nil {
		return false, TestingHook()
	}

	ok, err := RedisBreaker.Execute(func() (interface{}, error) {
		return Rdb.Exists(ctx, key).Result()
	})

	if err != nil {

		// don't stop pipeline processing if circuit is open
		if errors.Is(err, gobreaker.ErrOpenState) || errors.Is(err, gobreaker.ErrTooManyRequests) {
			metrics.Normalizer_RedisCB_FallbacksTotal.Inc()
			logger.Log.Warn("Dedupe IsDuplicate skipped as circuit breaker - OPEN state", "err", err)
			return false, nil
		}

		// actual redis error
		return false, err
	}

	return ok.(int64) == 1, nil
}
