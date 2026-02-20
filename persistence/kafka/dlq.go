package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"market-persistence/config"
	"shared/logger"
	"shared/metrics"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func PublishToDlq(ctx context.Context, message *config.DLQMessage) error {

	value, err := json.Marshal(message)
	if err != nil {
		return err
	}

	dlqRecord := &kgo.Record{
		Topic: config.DlqTopic,
		Key:   []byte(fmt.Sprintf("%s:%d:%d", message.Topic, message.Partition, message.Offset)),
		Value: value,
	}

	if Client == nil {
		return errors.New("Client closed")
	}

	dlqCtx, dlqCancel := context.WithTimeout(ctx, 5*time.Second)
	defer dlqCancel()
	produceResults := Client.ProduceSync(dlqCtx, dlqRecord)
	rec, err := produceResults.First()
	if err != nil {
		logger.Log.Error("Error in DLQ publish", "error", err)
		return err
	}

	logger.Log.Warn("Produced record to DLQ topic", "topic", rec.Topic, "partition", rec.Partition, "offset", rec.Offset)
	metrics.Persistence_DLQCount.WithLabelValues(message.Topic).Inc()

	return nil
}
