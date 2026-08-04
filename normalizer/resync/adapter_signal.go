package resync

import (
	"context"
	"encoding/json"
	"market-normalizer/constants"
	"os"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

const ResyncRequestsTopic = "resync.requests"

var client *kgo.Client

type ResyncRequest struct {
	Topic  string `json:"topic"`
	Symbol string `json:"symbol"`
}

// InitResyncProducer sets up a small, standalone, non-transactional producer
// used only for resync signals. These are fire-and-forget control-plane
// requests to adapter -- a duplicate or occasionally dropped one just means
// adapter reconnects once more than strictly necessary -- so they deliberately
// don't ride on any worker's transactional session.
func InitResyncProducer(cfg *constants.KafkaConfig) error {
	c, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		return err
	}
	client = c
	return nil
}

// signals adapter to force a resubscribe for the given topic. keyed by topic, symbol as by channel not accurate
func PublishResyncRequest(topic, symbol string) {
	req := ResyncRequest{Topic: topic, Symbol: symbol}
	val, err := json.Marshal(req)
	if err != nil {
		logger.Log.Error("Failed to marshal resync request", "err", err)
		return
	}

	rec := &kgo.Record{Key: []byte(topic), Value: val, Topic: ResyncRequestsTopic}
	client.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Failed to publish resync request", "err", err, "topic", topic)
		}
	})
}
