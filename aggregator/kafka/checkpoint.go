package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"market-aggregator/constants"
	"market-aggregator/utils"
	"os"
	"shared/kafkaauth"
	"shared/logger"
	"shared/srcoffset"
	"strings"
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

// symbolKeyPrefix tells the two checkpoint formats apart on the same compacted topic.
const symbolKeyPrefix = "v2:"

// SymbolCheckpoint is everything one symbol needs to resume after a crash, in ONE record: all its windows' metric
// state AND the source offsets already applied to that state. Kafka can write a single record atomically but not
// several, so keeping state and seen-offsets together is what makes a crash leave them consistent with each other.
// A record per (symbol, window) like the transactional format could be half old and half new.
type SymbolCheckpoint struct {
	Windows map[string]map[constants.MetricName]constants.Metric
	Seen    map[srcoffset.SrcKey]*srcoffset.Seen
}

// EncodeSymbolCheckpoint serialises the checkpoint right away. The caller owns the live metric maps and keeps
// mutating them, so the bytes must be taken before returning to the tick loop.
func EncodeSymbolCheckpoint(c SymbolCheckpoint) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(c); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ProduceSymbolCheckpoint writes one symbol's checkpoint. done runs once the broker has acked (or failed) it.
func ProduceSymbolCheckpoint(producer utils.KafkaClient, bufferKey string, value []byte, done func(error)) {
	rec := &kgo.Record{Key: []byte(symbolKeyPrefix + bufferKey), Value: value, Topic: CheckpointTopic}
	producer.Produce(context.Background(), rec, func(_ *kgo.Record, err error) {
		if err != nil {
			logger.Log.Error("Failed to produce symbol checkpoint", "bufferKey", bufferKey, "err", err)
		}
		done(err)
	})
}

// reads the checkpoint topic to its current end using a short-lived client and hands every record to handle.
// Called once at startup before any worker begins consuming, so every worker sees the same fully-loaded, read-only map
// its safe to share since it's never mutated after this function returns.
func readCheckpointTopic(ctx context.Context, cfg *constants.KafkaConfig, handle func(*kgo.Record)) {
	loadCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	authOpts, authErr := kafkaauth.IAMOpts(ctx)
	if authErr != nil {
		logger.Log.Error("Error building MSK IAM auth", "error", authErr)
		os.Exit(1)
	}

	opts := append([]kgo.Opt{
		kgo.SeedBrokers(cfg.BootstrapServers...),
		kgo.ConsumeTopics(CheckpointTopic),
		kgo.WithLogger(kgo.BasicLogger(os.Stdout, kgo.LogLevelWarn, nil)),
	}, authOpts...)

	client, err := kgo.NewClient(opts...)

	if err != nil {
		logger.Log.Warn("Failed to create checkpoint loader client, starting with empty window state", "err", err)
		return
	}
	defer client.Close()

	adm := kadm.NewClient(client)

	// first fetch the watermarks per partition from checkpoint topic
	targets, err := adm.ListEndOffsets(loadCtx, CheckpointTopic)
	if err != nil {
		logger.Log.Warn("Failed to list checkpoint topic end offsets, starting with empty window state", "err", err)
		return
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
			handle(rec)

			if target, ok := remaining[rec.Partition]; ok && rec.Offset+1 >= target {
				delete(remaining, rec.Partition)
			}
		})
	}
}

// returns the latest per-(symbol, window) checkpoint per bufferKey:windowId key. Transactional mode.
func LoadCheckpoints(ctx context.Context, cfg *constants.KafkaConfig) map[string]map[constants.MetricName]constants.Metric {
	result := make(map[string]map[constants.MetricName]constants.Metric)

	readCheckpointTopic(ctx, cfg, func(rec *kgo.Record) {
		if strings.HasPrefix(string(rec.Key), symbolKeyPrefix) {
			return // at-least-once format, not ours
		}
		var ckpt WindowCheckpoint
		if err := gob.NewDecoder(bytes.NewReader(rec.Value)).Decode(&ckpt); err != nil {
			logger.Log.Error("Failed to decode window checkpoint, skipping", "key", string(rec.Key), "err", err)
			return
		}
		result[string(rec.Key)] = ckpt.Metrics
	})

	logger.Log.Info("Loaded window checkpoints", "count", len(result))
	return result
}

// returns the latest per-symbol checkpoint per bufferKey. At-least-once mode.
func LoadSymbolCheckpoints(ctx context.Context, cfg *constants.KafkaConfig) map[string]*SymbolCheckpoint {
	result := make(map[string]*SymbolCheckpoint)

	readCheckpointTopic(ctx, cfg, func(rec *kgo.Record) {
		bufferKey, ok := strings.CutPrefix(string(rec.Key), symbolKeyPrefix)
		if !ok {
			return // transactional-mode format, not ours
		}
		var ckpt SymbolCheckpoint
		if err := gob.NewDecoder(bytes.NewReader(rec.Value)).Decode(&ckpt); err != nil {
			logger.Log.Error("Failed to decode symbol checkpoint, skipping", "key", string(rec.Key), "err", err)
			return
		}
		result[bufferKey] = &ckpt
	})

	logger.Log.Info("Loaded symbol checkpoints", "count", len(result))
	return result
}
