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
