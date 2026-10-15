package worker_test

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/utils/kafkatest"
	"market-normalizer/worker"
	"os"
	"shared/metrics"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Not a pass/fail correctness test: it runs the real worker against a real broker behind simulated network
// latency and logs where the commit time goes, using the same histograms Prometheus scrapes in production.
// RTT_MS sets the round trip latency (default 500), COMMIT_MS the commit interval (default 500).
//
//	go test ./worker -run TestCommitUnderRTT -v -count=1
func TestCommitUnderRTT(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers-backed test in short mode")
	}
	rtt := time.Duration(envInt("RTT_MS", 500)) * time.Millisecond
	commitEvery := time.Duration(envInt("COMMIT_MS", 500)) * time.Millisecond

	metrics.InitNormalizerMetrics()
	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()

	rawTopic, downstreamTopic := "kraken.ticker", "normalized.ticks"
	_, kafkaContainer := kafkatest.InitKafkaContainer(t, []string{rawTopic, downstreamTopic})
	defer func() { _ = testcontainers.TerminateContainer(kafkaContainer) }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	brokers, err := kafkaContainer.Brokers(ctx)
	require.NoError(t, err)

	cfg := &constants.KafkaConfig{
		Brokers:          brokers,
		Topics:           []string{rawTopic},
		ConsumerGroup:    "normalizer-group-rtt",
		MaxBufferRecords: 5000,
	}

	// only the worker's connections are delayed, so the e2e below isolates the worker's commit behaviour
	session, err := kafka.NewWorkerSession(ctx, cfg, 0, kafkatest.DelayDialer(rtt))
	require.NoError(t, err)
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), nil)
	go w.Run(ctx, session, commitEvery)

	producer, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	require.NoError(t, err)
	defer producer.Close()
	consumer, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.ConsumeTopics(downstreamTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	require.NoError(t, err)
	defer consumer.Close()

	val := []byte(`{"exchange":"kraken","channel":"ticker","type":"snapshot","data":[{"symbol":"ETH/USD","bid":0.10025,
	"bid_qty":740.0,"ask":0.10036,"ask_qty":1361.4,"last":0.10035,"volume":997038.98,"vwap":0.10148,"low":0.09979,
	"high":0.10285,"change":-0.00017,"change_pct":-0.17}]}`)

	// one record every 50ms for 25s. the first 10s are warm-up (group join and first transactions cost many RTTs).
	const total, warmup = 25 * time.Second, 10 * time.Second
	var mu sync.Mutex
	var sent []time.Time
	go func() {
		tick := time.NewTicker(50 * time.Millisecond)
		defer tick.Stop()
		end := time.After(total)
		for {
			select {
			case <-end:
				return
			case <-tick.C:
				mu.Lock()
				sent = append(sent, time.Now())
				mu.Unlock()
				producer.Produce(ctx, &kgo.Record{Key: []byte("ETH/USD"), Topic: rawTopic, Value: val}, nil)
			}
		}
	}()

	start := time.Now()
	var latencies []float64
	received := 0
	for time.Since(start) < total+15*time.Second {
		pc, pcancel := context.WithTimeout(ctx, time.Second)
		consumer.PollFetches(pc).EachRecord(func(*kgo.Record) {
			mu.Lock()
			if received < len(sent) && sent[received].After(start.Add(warmup)) {
				latencies = append(latencies, time.Since(sent[received]).Seconds())
			}
			received++
			mu.Unlock()
		})
		pcancel()
		mu.Lock()
		done := received >= len(sent) && time.Since(start) > total
		mu.Unlock()
		if done {
			break
		}
	}

	t.Logf("simulated RTT %v, commit interval %v: %d records measured after warm-up", rtt, commitEvery, len(latencies))
	require.NotEmpty(t, latencies, "no records reached the downstream topic")
	sort.Float64s(latencies)
	t.Logf("worker-to-consumer latency (excludes producer and consumer legs): p50 %.2fs  p95 %.2fs  max %.2fs",
		latencies[len(latencies)/2], latencies[int(float64(len(latencies)-1)*0.95)], latencies[len(latencies)-1])

	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, f := range families {
		switch f.GetName() {
		case "normalizer_commit_latency_seconds":
			for _, m := range f.GetMetric() {
				h := m.GetHistogram()
				t.Logf("End() duration: %d commits, mean %.0fms", h.GetSampleCount(), 1000*h.GetSampleSum()/float64(h.GetSampleCount()))
			}
		case "normalizer_broker_rpc_seconds":
			for _, m := range f.GetMetric() {
				h := m.GetHistogram()
				t.Logf("broker rpc %-22s %5d calls, mean %6.0fms", m.GetLabel()[0].GetValue(), h.GetSampleCount(),
					1000*h.GetSampleSum()/float64(h.GetSampleCount()))
			}
		}
	}
}

func envInt(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil {
		return v
	}
	return def
}

