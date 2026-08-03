package internal

import (
	"context"
	"encoding/json"
	"shared/logger"

	"github.com/twmb/franz-go/pkg/kgo"
)

const ResyncRequestsTopic = "resync.requests"

type resyncRequest struct {
	Topic  string `json:"topic"`
	Symbol string `json:"symbol"`
}

// consumer listens for resync requests from normalizer and forces a reconnect for the requested stream
// cancel's that stream's attemptCtx which triggers reconnect and gives back fresh snapshot
func StartResyncConsumer(ctx context.Context, brokers []string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(ResyncRequestsTopic),
		kgo.ConsumerGroup("adapter-resync-group"),
		kgo.AutoCommitMarks(),
	)
	if err != nil {
		logger.Log.Error("Failed to start resync consumer, resync requests will be ignored", "err", err)
		return
	}
	defer client.Close()

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		fetches := client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			var req resyncRequest
			if err := json.Unmarshal(rec.Value, &req); err != nil {
				logger.Log.Error("Failed to parse resync request", "err", err)
				return
			}

			if TriggerResync(req.Topic) {
				logger.Log.Warn("Forced resubscribe due to resync request", "topic", req.Topic, "symbol", req.Symbol)
			} else {
				logger.Log.Warn("Received resync request for unknown/inactive topic", "topic", req.Topic)
			}
		})
	}
}
