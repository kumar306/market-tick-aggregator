package utils

import (
	"context"
	"errors"
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

type BreakerTestClient struct {
	Promise func(*kgo.Record, error)
}

func NewBreakerTestClient(promise func(*kgo.Record, error)) *BreakerTestClient {
	return &BreakerTestClient{Promise: promise}
}

func (b *BreakerTestClient) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	if b.Promise != nil {
		logger.Log.Info("Executing mock client breaker promise")
		b.Promise(nil, errors.New("produce failed"))
	}
}
