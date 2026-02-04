package redis

import (
	"bytes"
	"context"
	"market-orderbook/constants"
	testcontainers "market-orderbook/internal/testcontainers"
	"os"
	"testing"

	tc "github.com/testcontainers/testcontainers-go"
)

func TestRedisLoadAndGetSnapshot(t *testing.T) {
	ctx := context.Background()

	container, addr := testcontainers.StartRedis(ctx, t)
	defer func() {
		if err := tc.TerminateContainer(container); err != nil {
			t.Fatalf("Error terminating redis container: %v", err)
		}
	}()

	if err := os.Setenv(REDIS_ADDR, addr); err != nil {
		t.Fatalf("Error setting redis addr env: %v", err)
	}
	if err := os.Setenv(REDIS_PASSWORD, ""); err != nil {
		t.Fatalf("Error setting redis password env: %v", err)
	}

	InitRedis(&constants.RedisConfig{
		TtlMinutes:   1,
		PoolSize:     2,
		MinIdleConns: 1,
	})

	key := "coinbase:ETH-USD"
	data := []byte("snapshot-bytes")

	if err := LoadSnapshot(ctx, key, data); err != nil {
		t.Fatalf("LoadSnapshot error: %v", err)
	}

	got, err := GetSnapshot(ctx, key)
	if err != nil {
		t.Fatalf("GetSnapshot error: %v", err)
	}

	if !bytes.Equal(got, data) {
		t.Fatalf("snapshot mismatch: got=%v want=%v", got, data)
	}
}
