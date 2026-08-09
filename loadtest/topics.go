package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var standardTopics = []string{
	"binance.raw.ticks", "binance.raw.level2",
	"coinbase.raw.ticks", "coinbase.raw.level2",
	"kraken.raw.ticks", "kraken.raw.book",
	"normalized.ticks", "normalized.book",
	"aggregated.ticks", "aggregated.book",
	"persistence.dlq", "resync.requests",
}

const (
	checkpointTopic = "aggregator.window.checkpoints"
	partitions      = 3
)

// input topics per pipeline consumer group - excludes checkpointTopic, which
// aggregator manages as its own recovery state rather than a plain backlog.
var groupTopics = map[string][]string{
	"normalizer-group-1": {
		"binance.raw.ticks", "binance.raw.level2",
		"coinbase.raw.ticks", "coinbase.raw.level2",
		"kraken.raw.ticks", "kraken.raw.book",
	},
	"aggregator-group-1":  {"normalized.ticks"},
	"orderbook-group-1":   {"normalized.book"},
	"persistence-group-1": {"aggregated.ticks", "aggregated.book"},
}

func bootstrapTopics(ctx context.Context, client *kgo.Client) error {
	adm := kadm.NewClient(client)
	defer adm.Close()

	// replication factor = -1 => let msk serverless manage it
	resp, err := adm.CreateTopics(ctx, partitions, -1, nil, standardTopics...)
	if err != nil {
		return err
	}
	for topic, r := range resp {
		status := "ok"
		if r.Err != nil {
			status = fmt.Sprintf("FAILED: %v", r.Err)
		}
		fmt.Printf("  %-32s %s\n", topic, status)
	}

	compactPolicy := "compact"
	ckResp, err := adm.CreateTopics(ctx, partitions, -1, map[string]*string{"cleanup.policy": &compactPolicy}, checkpointTopic)
	if err != nil {
		return err
	}
	for topic, r := range ckResp {
		status := "ok (compacted)"
		if r.Err != nil {
			status = fmt.Sprintf("FAILED: %v", r.Err)
		}
		fmt.Printf("  %-32s %s\n", topic, status)
	}
	return nil
}

// skips every pipeline consumer group past whatever
// backlog is currently sitting in its input topics, so a fresh load test
// starts from a verified-zero-lag baseline instead of inheriting stale
// messages from a prior run or a mid-test redeploy. Requires each group to
// have no active members - callers must scale the consumers to 0 first.
func resetOffsetsToLatest(ctx context.Context, client *kgo.Client) error {
	adm := kadm.NewClient(client)
	defer adm.Close()

	for group, topics := range groupTopics {
		ends, err := adm.ListEndOffsets(ctx, topics...)
		if err != nil {
			return fmt.Errorf("listing end offsets for %s: %w", group, err)
		}
		if err := ends.Error(); err != nil {
			return fmt.Errorf("listing end offsets for %s: %w", group, err)
		}

		resp, err := adm.CommitOffsets(ctx, group, ends.Offsets())
		if err != nil {
			return fmt.Errorf("committing reset offsets for %s: %w", group, err)
		}
		for _, t := range resp {
			for _, r := range t {
				status := "ok"
				if r.Err != nil {
					status = fmt.Sprintf("FAILED: %v", r.Err)
				}
				fmt.Printf("  %-20s %-24s partition=%-3d -> %-10d %s\n", group, r.Topic, r.Partition, r.At, status)
			}
		}
	}
	return nil
}
