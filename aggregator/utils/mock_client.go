package utils

import (
	"context"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaClient interface {
	Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error))
}

type MockClient struct{}

func (m *MockClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	logger.Log.Info("Entered mock client produce")
	logger.Log.Info("Exitted mock client produce")
}
