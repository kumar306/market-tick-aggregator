package worker_test

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	kf "github.com/testcontainers/testcontainers-go/modules/kafka"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	upstream   = "normalized.ticks"
	downstream = "aggregated.ticks"
)

type runningWorker struct {
	flush chan *constants.DispatchRecord
	crash func()
	stop  func()
}

func startWorker(t *testing.T, ctx context.Context, cfg *constants.KafkaConfig, ckpts map[string]*kafka.SymbolCheckpoint) *runningWorker {
	cl, err := kafka.NewWorkerClient(ctx, cfg, 0)
	require.NoError(t, err)
	flush := make(chan *constants.DispatchRecord, 10)
	w := worker.NewWorker(0, flush, oneWindow, nil)
	w.EnableAtLeastOnce(ckpts)
	wctx, cancel := context.WithCancel(ctx)
	go w.RunAtLeastOnce(wctx, cl, 300*time.Millisecond)
	return &runningWorker{flush: flush, crash: cl.Close, stop: cancel}
}

func (r *runningWorker) flushNow() {
	r.flush <- &constants.DispatchRecord{Event: constants.FlushEvent, WindowConfig: oneWindow[0], FlushTsMs: time.Now().UnixMilli()}
}

// Crash test for the aggregator without transactions. The question it answers: after a crash, restart from the
// checkpoint topic, and a normalizer replay on top, is every tick counted exactly once?
//
// Volume is a tumbling sum and every tick has volume 1, so the sum of volume over everything the worker flushed
// downstream must equal the number of distinct source records. Any double count or any loss shows up as a mismatch.
func TestAtLeastOnce_CrashRestartReplay_EveryTickCountedOnce(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers-backed test in short mode")
	}
	newALOWorker(t, nil) // initialises metrics and the metric registry

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := kf.Run(ctx, "confluentinc/confluent-local:7.4.4", kf.WithClusterID("123"))
	require.NoError(t, err)
	defer func() { _ = testcontainers.TerminateContainer(c) }()
	brokers, err := c.Brokers(ctx)
	require.NoError(t, err)

	admin, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer admin.Close()
	_, err = kadm.NewClient(admin).CreateTopics(ctx, 1, 1, nil, upstream, downstream, kafka.CheckpointTopic)
	require.NoError(t, err)

	kafka.DownstreamTopic = downstream
	cfg := &constants.KafkaConfig{
		BootstrapServers: brokers, ConsumerGroup: "agg-alo-test", MaxBufferRecords: 5000,
		TopicConfig: constants.TopicConfig{Upstream: upstream, Downstream: downstream},
	}

	// every tick is stamped with a source offset, as the normalizer stamps it
	produce := func(from, to int64) {
		for off := from; off <= to; off++ {
			rec := tickFrom(t, 0, off)
			rec.Topic = upstream
			require.NoError(t, admin.ProduceSync(ctx, rec).FirstErr())
		}
	}

	// ---- life 1: 300 ticks, a flush, then a checkpoint that covers both
	first := startWorker(t, ctx, cfg, nil)
	produce(1, 300)
	time.Sleep(1500 * time.Millisecond)
	first.flushNow() // emits volume 300 and resets the tumbling sum
	time.Sleep(1500 * time.Millisecond)

	// ---- 100 more ticks, then a crash before they can be checkpointed (no flush, no final checkpoint)
	produce(301, 400)
	first.crash()

	// ---- life 2: restore from what is on the checkpoint topic
	ckpts := kafka.LoadSymbolCheckpoints(ctx, cfg)
	require.Contains(t, ckpts, symbolKey, "the symbol's checkpoint must have been written and be readable")
	second := startWorker(t, ctx, cfg, ckpts)
	defer second.stop()

	// the normalizer also crashed and re-sent 251..350: some are inside the checkpoint, some are not yet applied
	// by this life, some will have been redelivered from Kafka already
	produce(251, 350)
	produce(401, 600)
	time.Sleep(3 * time.Second)
	second.flushNow()

	// ---- what reached the downstream topic
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(downstream))
	require.NoError(t, err)
	defer consumer.Close()

	var total float64
	var flushes int
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) && flushes < 2 {
		pc, pcancel := context.WithTimeout(ctx, time.Second)
		consumer.PollFetches(pc).EachRecord(func(r *kgo.Record) {
			agg := &generated.AggregatedTick{}
			require.NoError(t, proto.Unmarshal(r.Value, agg))
			flushes++
			total += agg.GetVolumeMetrics().GetVolume()
		})
		pcancel()
	}

	t.Logf("flushes seen downstream: %d, summed volume: %.0f (distinct ticks produced: 600, source records re-sent: 100)", flushes, total)
	require.Equal(t, 2, flushes)
	require.Equal(t, 600.0, total, "every distinct tick counted exactly once across the crash, the restart and the replay")

}
