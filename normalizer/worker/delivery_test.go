package worker_test

import (
	"context"
	"fmt"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/proto/generated"
	"market-normalizer/utils/kafkatest"
	"market-normalizer/worker"
	"shared/metrics"
	"shared/srcoffset"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const (
	rawTopic = "kraken.raw.ticks"
	outTopic = constants.NormalizedTickerTopic // fixed by the ticker publisher
)

type env struct {
	brokers []string
	ctx     context.Context
}

func setup(t *testing.T) *env {
	metrics.InitNormalizerMetrics()
	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()

	_, c := kafkatest.InitKafkaContainer(t, []string{rawTopic, outTopic})
	t.Cleanup(func() { _ = testcontainers.TerminateContainer(c) })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	brokers, err := c.Brokers(ctx)
	require.NoError(t, err)
	return &env{brokers: brokers, ctx: ctx}
}

// a raw kraken ticker message whose "last" price carries the test's message id
func rawTick(symbol string, id int) []byte {
	return []byte(fmt.Sprintf(`{"exchange":"kraken","channel":"ticker","type":"snapshot","data":[{"symbol":%q,"bid":1,"bid_qty":1,
	"ask":1,"ask_qty":1,"last":%d,"volume":1,"vwap":1,"low":1,"high":1,"change":0,"change_pct":0}]}`, symbol, id))
}

// stop() ends the worker gracefully. crash() drops the client with no final commit and no flush, which is what
// a killed process looks like to the broker: unacked outputs may or may not have landed, offsets stay where the
// last commit put them.
type running struct {
	stop  func()
	crash func()
}

func (e *env) startAtLeastOnce(t *testing.T, rtt, commitEvery time.Duration) *running {
	cfg := &constants.KafkaConfig{Brokers: e.brokers, Topics: []string{rawTopic}, ConsumerGroup: "norm-alo", MaxBufferRecords: 5000}
	cl, err := kafka.NewWorkerClient(e.ctx, cfg, 0, kafkatest.DelayDialer(rtt))
	require.NoError(t, err)
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), nil)
	wctx, cancel := context.WithCancel(e.ctx)
	go w.RunAtLeastOnce(wctx, cl, commitEvery)
	return &running{stop: cancel, crash: cl.Close}
}

func (e *env) startTransactional(t *testing.T, rtt, commitEvery time.Duration) *running {
	cfg := &constants.KafkaConfig{Brokers: e.brokers, Topics: []string{rawTopic}, ConsumerGroup: "norm-txn", MaxBufferRecords: 5000}
	session, err := kafka.NewWorkerSession(e.ctx, cfg, 0, kafkatest.DelayDialer(rtt))
	require.NoError(t, err)
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), nil)
	wctx, cancel := context.WithCancel(e.ctx)
	go w.Run(wctx, session, commitEvery)
	return &running{stop: cancel, crash: session.Close}
}

type received struct {
	id   int
	at   time.Time
	src  string // "topic/partition/offset" of the raw record this output came from
	hasS bool
}

// collects every record on the output topic, read_committed so both modes are measured the same way
func (e *env) collect(t *testing.T) (snapshot func() []received) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...), kgo.ConsumeTopics(outTopic), kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	require.NoError(t, err)
	t.Cleanup(cl.Close)

	var mu sync.Mutex
	var all []received
	go func() {
		for e.ctx.Err() == nil {
			cl.PollFetches(e.ctx).EachRecord(func(r *kgo.Record) {
				m := &generated.NormalizedTicker{}
				if proto.Unmarshal(r.Value, m) != nil {
					return
				}
				rec := received{id: int(m.Price), at: time.Now()}
				for _, h := range r.Headers {
					if h.Key == srcoffset.HeaderKey {
						if topic, p, off, ok := srcoffset.Decode(h.Value); ok {
							rec.src, rec.hasS = fmt.Sprintf("%s/%d/%d", topic, p, off), true
						}
					}
				}
				mu.Lock()
				all = append(all, rec)
				mu.Unlock()
			})
		}
	}()
	return func() []received {
		mu.Lock()
		defer mu.Unlock()
		return append([]received(nil), all...)
	}
}

func quantiles(v []float64) (p50, p95, max float64) {
	if len(v) == 0 {
		return
	}
	sort.Float64s(v)
	return v[len(v)/2], v[int(float64(len(v)-1)*0.95)], v[len(v)-1]
}

// Kill test. Produces a steady stream, kills the worker mid-stream, starts a replacement, then checks what the
// downstream topic actually contains:
//   - zero loss: every id arrived at least once
//   - duplicates are counted and bounded by the commit window
//   - every output carries its source coordinate, so deduping on it leaves exactly one copy per input
//
// RTT_MS (default 200) is the simulated broker round trip, COMMIT_MS (default 1000) the offset commit interval.
func TestAtLeastOnce_KillMidStream_NoLossBoundedDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers-backed test in short mode")
	}
	rtt := time.Duration(envInt("RTT_MS", 200)) * time.Millisecond
	commitEvery := time.Duration(envInt("COMMIT_MS", 1000)) * time.Millisecond
	e := setup(t)
	snapshot := e.collect(t)

	producer, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
	require.NoError(t, err)
	defer producer.Close()

	const total, crashAt = 4000, 1800
	const every = 5 * time.Millisecond // 200 msg/s
	symbols := []string{"S0", "S1", "S2", "S3", "S4", "S5"}

	first := e.startAtLeastOnce(t, rtt, commitEvery)
	var second *running
	sent := make([]time.Time, total+1)
	for id := 1; id <= total; id++ {
		sent[id] = time.Now()
		producer.Produce(e.ctx, &kgo.Record{Topic: rawTopic, Key: []byte(symbols[id%len(symbols)]), Value: rawTick(symbols[id%len(symbols)], id)}, nil)
		if id == crashAt {
			t.Logf("crashing worker 1 after producing %d messages", id)
			first.crash()
			second = e.startAtLeastOnce(t, rtt, commitEvery)
		}
		time.Sleep(every)
	}
	defer func() {
		if second != nil {
			second.stop()
		}
	}()

	require.Eventually(t, func() bool {
		seen := map[int]bool{}
		for _, r := range snapshot() {
			seen[r.id] = true
		}
		return len(seen) == total
	}, 120*time.Second, time.Second, "some messages never reached the output topic")

	// let any in-flight duplicates land before counting
	time.Sleep(3 * time.Second)
	all := snapshot()

	byID := map[int]int{}
	bySrc := map[string]int{}
	var lat []float64
	firstSeen := map[int]bool{}
	for _, r := range all {
		byID[r.id]++
		require.True(t, r.hasS, "output without a source header cannot be deduplicated downstream")
		bySrc[r.src]++
		if !firstSeen[r.id] {
			firstSeen[r.id] = true
			lat = append(lat, r.at.Sub(sent[r.id]).Seconds())
		}
	}
	missing := 0
	for id := 1; id <= total; id++ {
		if byID[id] == 0 {
			missing++
		}
	}
	dups := len(all) - len(byID)
	p50, p95, max := quantiles(lat)

	t.Logf("RTT %v, commit every %v, %d messages at 200/s, crash after %d", rtt, commitEvery, total, crashAt)
	t.Logf("outputs read: %d, unique ids: %d, missing: %d, duplicate copies: %d", len(all), len(byID), missing, dups)
	t.Logf("unique source coordinates: %d (dedup on the header leaves one copy per input)", len(bySrc))
	t.Logf("latency producer -> first copy visible: p50 %.2fs  p95 %.2fs  max %.2fs", p50, p95, max)

	require.Zero(t, missing, "message loss")
	require.Equal(t, total, len(bySrc), "dedup on the source header must leave exactly one copy per input")

	// what can be replayed: everything since the last commit that landed, i.e. one commit interval plus the time a commit
	// takes to reach the broker, at 200 msg/s. generous bound; the point is that it is a window, not unbounded.
	bound := int(200 * (commitEvery + 4*rtt + 2*time.Second).Seconds())
	require.LessOrEqual(t, dups, bound, "duplicates exceed the commit window")
}

// A/B: the same stream through each delivery mode, same simulated RTT, measured at a read_committed consumer.
// Only the worker's own connections are delayed, so this isolates what the worker adds: for transactional that is
// the commit protocol, for at-least-once it is one produce ack.
func TestDeliveryMode_Latency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testcontainers-backed test in short mode")
	}
	rtt := time.Duration(envInt("RTT_MS", 500)) * time.Millisecond

	for _, mode := range []struct {
		name        string
		commitEvery time.Duration
		start       func(*env, *testing.T, time.Duration, time.Duration) *running
	}{
		{"transactional", 500 * time.Millisecond, (*env).startTransactional},
		{"at_least_once", time.Second, (*env).startAtLeastOnce},
	} {
		t.Run(mode.name, func(t *testing.T) {
			e := setup(t)
			snapshot := e.collect(t)
			run := mode.start(e, t, rtt, mode.commitEvery)
			defer run.stop()

			producer, err := kgo.NewClient(kgo.SeedBrokers(e.brokers...))
			require.NoError(t, err)
			defer producer.Close()

			const total = 25 * time.Second
			const warmup = 10 * time.Second // group join and first transactions cost many RTTs
			start := time.Now()
			sent := map[int]time.Time{}
			for id := 1; time.Since(start) < total; id++ {
				sent[id] = time.Now()
				producer.Produce(e.ctx, &kgo.Record{Topic: rawTopic, Key: []byte("S0"), Value: rawTick("S0", id)}, nil)
				time.Sleep(50 * time.Millisecond)
			}
			time.Sleep(8 * time.Second)

			var lat []float64
			seen := map[int]bool{}
			for _, r := range snapshot() {
				if seen[r.id] {
					continue
				}
				seen[r.id] = true
				if s := sent[r.id]; s.After(start.Add(warmup)) {
					lat = append(lat, r.at.Sub(s).Seconds())
				}
			}
			p50, p95, max := quantiles(lat)
			t.Logf("%-13s RTT %v, commit %v: %d msgs  worker-to-consumer p50 %.2fs  p95 %.2fs  max %.2fs",
				mode.name, rtt, mode.commitEvery, len(lat), p50, p95, max)
			require.NotEmpty(t, lat)
		})
	}
}
