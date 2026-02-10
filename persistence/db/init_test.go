package db

import (
	"context"
	"sync"
	"testing"
)

func TestInitDBFailsWhenRequiredEnvMissing(t *testing.T) {
	Pool = nil
	DbOnce = sync.Once{}

	t.Setenv("POSTGRES_USER", "")
	t.Setenv("POSTGRES_PASSWORD", "secret")
	t.Setenv("POSTGRES_DB", "market")

	err := InitDB(context.Background())
	if err == nil {
		t.Fatalf("error is nil, wanted non-nil: %v", err)
	}
}
