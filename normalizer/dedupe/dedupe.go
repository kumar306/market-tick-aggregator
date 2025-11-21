package dedupe

import (
	"context"
	"os"
	"shared/logger"
	"time"

	"github.com/redis/go-redis/v9"
)

// use redis to store dedupe keys - set key if not exists - do it at the end of publish
// at the start of pipeline, check if key present. if its there, then skip

// init redis
var Rdb *redis.Client
var Ttl time.Duration

const (
	REDIS_ADDR     string = "REDIS_ADDR"
	REDIS_PASSWORD string = "REDIS_PASSWORD"
)

func InitRedis(ttl time.Duration) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     os.Getenv(REDIS_ADDR),
		Password: os.Getenv(REDIS_PASSWORD),
		DB:       0,
	})
	Ttl = ttl

	logger.Log.Info("Initialised Redis Client")
}

func ConstructDedupeKey(exchange, channel, symbol string, orderingID string) string {
	return exchange + ":" + channel + ":" + symbol + ":" + orderingID
}

// set the dedupe key in redis with TTL
func MarkForDedupe(ctx context.Context, key string) error {
	ok, err := Rdb.SetNX(ctx, key, 1, Ttl).Result()
	if err != nil {
		return logger.LogAndWrap("Error when setting dedupe key in redis", err, "key", key)
	}

	if !ok {
		// key exists
		logger.Log.Warn("Key already exists in redis", "key", key)
	}

	return nil
}

func IsDuplicate(ctx context.Context, key string) (bool, error) {
	ok, err := Rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}

	return ok == 1, nil
}
