package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"market-aggregator/constants"
	"market-aggregator/utils"
	"os"
	"shared/logger"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const CheckpointTopic = "aggregator.window.checkpoints"

// format for (symbol, window) checkpoint
type WindowCheckpoint struct {
	Metrics map[constants.MetricName]constants.Metric
}

// PublishCheckpoint produces a window's current metric state to the
// compacted checkpoint topic, inside whatever transaction is currently open
// on producer

// compact aggressively as we committing very frequently.. huge state accumulation for huge windows
func PublishCheckpoint(producer utils.KafkaClient, bufferKey, windowId string, metricsMap map[constants.MetricName]constants.Metric, onFailure func()) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(WindowCheckpoint{Metrics: metricsMap}); err != nil {
		logger.Log.Error("Failed to encode window checkpoint", "bufferKey", bufferKey, "windowId", windowId, "err", err)
		onFailure()
		return
	}

	key := bufferKey + ":" + windowId
	rec := &kgo.Record{Key: []byte(key), Value: buf.Bytes(), Topic: CheckpointTopic}

	producer.Produce(context.Background(), rec, func(r *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Failed to produce window checkpoint", "key", key, "err", err)
			onFailure()
		}
	})
}

// reads the checkpoint topic to its current end using a short-lived client, and returns
// the latest checkpoint per bufferKey:windowId key.
// Called once at startup before any worker begins consuming, so every worker sees the same fully-loaded, read-only map
// its safe to share since it's never mutated after this function returns.
func LoadCheckpoints(ctx context.Context, cfg *constants.KafkaConfig) map[string]map[constants.MetricName]constants.Metric {
	result := make(map[string]map[constants.MetricName]constants.Metric)

	loadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ConsumeTopics(CheckpointTopic),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	)
	if err != nil {
		logger.Log.Warn("Failed to create checkpoint loader client, starting with empty window state", "err", err)
		return result
	}
	defer client.Close()

	adm := kadm.NewClient(client)

	// first fetch the watermarks per partition from checkpoint topic
	targets, err := adm.ListEndOffsets(loadCtx, CheckpointTopic)
	if err != nil {
		logger.Log.Warn("Failed to list checkpoint topic end offsets, starting with empty window state", "err", err)
		return result
	}

	remaining := map[int32]int64{}
	targets.Each(func(lo kadm.ListedOffset) {
		if lo.Offset > 0 {
			remaining[lo.Partition] = lo.Offset
		}
	})

	// if watermarks/checkpoints exist -
	for len(remaining) > 0 {
		if loadCtx.Err() != nil {
			logger.Log.Warn("Timed out loading window checkpoints, proceeding with what was loaded so far", "remaining_partitions", len(remaining))
			break
		}

		pollCtx, pollCancel := context.WithTimeout(loadCtx, 5*time.Second)
		fetches := client.PollFetches(pollCtx)
		pollCancel()

		if fetches.IsClientClosed() {
			break
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			var ckpt WindowCheckpoint
			if err := gob.NewDecoder(bytes.NewReader(rec.Value)).Decode(&ckpt); err != nil {
				logger.Log.Error("Failed to decode window checkpoint, skipping", "key", string(rec.Key), "err", err)
			} else {
				result[string(rec.Key)] = ckpt.Metrics
			}

			if target, ok := remaining[rec.Partition]; ok && rec.Offset+1 >= target {
				delete(remaining, rec.Partition)
			}
		})
	}

	logger.Log.Info("Loaded window checkpoints", "count", len(result))
	return result
}
