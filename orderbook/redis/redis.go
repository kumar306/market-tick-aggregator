package redis

import (
	"context"
	"market-orderbook/constants"
	"os"
	"shared/logger"
	"time"

	"github.com/redis/go-redis/v9"
)

var Rdb *redis.Client
var Ttl time.Duration

const (
	REDIS_ADDR     string = "REDIS_ADDR"
	REDIS_PASSWORD string = "REDIS_PASSWORD"
)

func InitRedis(cfg *constants.RedisConfig) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:         os.Getenv(REDIS_ADDR),
		Password:     os.Getenv(REDIS_PASSWORD),
		DB:           0,
		PoolSize:     cfg.PoolSize,
		MinIdleConns: cfg.MinIdleConns,
	})

	Ttl = time.Duration(cfg.TtlMinutes) * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := Rdb.Ping(ctx); err != nil {
		logger.Log.Info("Error in Redis Client ping")
	}

	logger.Log.Info("Initialised Redis Client")
}

func LoadSnapshot(ctx context.Context, key string, snapshot []byte) error {
	return Rdb.Set(ctx, key, snapshot, Ttl).Err()
}

func GetSnapshot(ctx context.Context, key string) ([]byte, error) {
	snapshotBytes, err := Rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		logger.Log.Info("Could not find a snapshot in redis for key", "key", key)
		return []byte{}, nil
	}

	return snapshotBytes, err
}
