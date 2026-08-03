package resync

import (
	"context"
	"encoding/json"
	"market-normalizer/kafka"
	"shared/logger"
)

const ResyncRequestsTopic = "resync.requests"

type ResyncRequest struct {
	Topic  string `json:"topic"`
	Symbol string `json:"symbol"`
}

// signals adapter to force a resubscribe for the given topic. keyed by topic, symbol as by channel not accurate
func PublishResyncRequest(topic, symbol string) {
	req := ResyncRequest{Topic: topic, Symbol: symbol}
	val, err := json.Marshal(req)
	if err != nil {
		logger.Log.Error("Failed to marshal resync request", "err", err)
		return
	}
	kafka.ProduceSnapshotAsync(context.Background(), ResyncRequestsTopic, []byte(topic), val)
}
