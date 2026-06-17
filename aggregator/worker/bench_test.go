package worker_test

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"shared/metrics"
	"testing"

	"github.com/sony/gobreaker"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// nopClient satisfies utils.KafkaClient with zero overhead: no logs, no I/O.
type nopClient struct{}

func (nopClient) Produce(_ context.Context, _ *kgo.Record, _ func(*kgo.Record, error)) {}

func init() {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()
	// FlushWindow checks kafka.KafkaBreaker.State(); keep the breaker always closed.
	kafka.KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "bench-cb",
		ReadyToTrip: func(counts gobreaker.Counts) bool { return false },
	})
	kafka.DownstreamTopic = "aggregated.ticks"
	kafka.ProducerErrors = make(chan error, 65536)
}

// benchWindowCfg mirrors the 13 time windows in production (aggregator/config/config.yaml).
var benchWindowCfg = []*constants.WindowConfig{
	{Id: "5s", DurationMs: 5_000, FlushCadencyMs: 1_000, BucketSizeMs: 500},
	{Id: "10s", DurationMs: 10_000, FlushCadencyMs: 2_000, BucketSizeMs: 1_000},
	{Id: "30s", DurationMs: 30_000, FlushCadencyMs: 5_000, BucketSizeMs: 2_500},
	{Id: "1m", DurationMs: 60_000, FlushCadencyMs: 10_000, BucketSizeMs: 5_000},
	{Id: "2m", DurationMs: 120_000, FlushCadencyMs: 20_000, BucketSizeMs: 10_000},
	{Id: "5m", DurationMs: 300_000, FlushCadencyMs: 60_000, BucketSizeMs: 10_000},
	{Id: "10m", DurationMs: 600_000, FlushCadencyMs: 120_000, BucketSizeMs: 30_000},
	{Id: "30m", DurationMs: 1_800_000, FlushCadencyMs: 300_000, BucketSizeMs: 60_000},
	{Id: "1h", DurationMs: 3_600_000, FlushCadencyMs: 600_000, BucketSizeMs: 120_000},
	{Id: "2h", DurationMs: 7_200_000, FlushCadencyMs: 1_200_000, BucketSizeMs: 240_000},
	{Id: "6h", DurationMs: 21_600_000, FlushCadencyMs: 6_000_000, BucketSizeMs: 360_000},
	{Id: "12h", DurationMs: 43_200_000, FlushCadencyMs: 1_200_000, BucketSizeMs: 600_000},
	{Id: "24h", DurationMs: 86_400_000, FlushCadencyMs: 3_600_000, BucketSizeMs: 900_000},
}

func newBenchWorker(cfg []*constants.WindowConfig) (*worker.Worker, *constants.DispatchRecord) {
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 1), cfg)
	tick := &generated.NormalizedTick{
		Exchange:      "binance",
		Channel:       "aggTrade",
		Symbol:        "BTCUSDT",
		Price:         65_000.0,
		Volume:        0.5,
		EventTsMillis: 1_700_000_000_000,
		Open:          64_500.0,
		Close:         65_000.0,
		Low:           64_400.0,
		High:          65_200.0,
		SeqId:         1,
	}
	val, _ := proto.Marshal(tick)
	rec := &kgo.Record{
		Key:   []byte("binance:aggTrade:BTCUSDT"),
		Topic: "normalized.ticks",
		Value: val,
	}
	dr := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Tick:      tick,
		Record:    rec,
		Exchange:  "binance",
		Symbol:    "BTCUSDT",
		BufferKey: "binance:aggTrade:BTCUSDT",
		WorkerIdx: 0,
	}
	return w, dr
}

// BenchmarkProcessTick_1Window measures updating all 10 financial metrics across one time window.
func BenchmarkProcessTick_1Window(b *testing.B) {
	w, dr := newBenchWorker(benchWindowCfg[:1])
	ctx := context.Background()
	w.ProcessTick(ctx, dr) // warm-up: initialise symbol state before timing
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.ProcessTick(ctx, dr)
	}
}

// BenchmarkProcessTick_13Windows is the production hot-path: 10 financial metrics
// (OHLC, VWAP, EMA, ATR, …) updated across all 13 time windows per tick.
func BenchmarkProcessTick_13Windows(b *testing.B) {
	w, dr := newBenchWorker(benchWindowCfg)
	ctx := context.Background()
	w.ProcessTick(ctx, dr) // warm-up
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.ProcessTick(ctx, dr)
	}
}

// BenchmarkFlushWindow measures computing, serialising, and producing one window's aggregated metrics.
func BenchmarkFlushWindow(b *testing.B) {
	cfg := benchWindowCfg[:1]
	w, dr := newBenchWorker(cfg)
	ctx := context.Background()
	// Pre-fill price history so metrics are non-trivial.
	for i := 0; i < 100; i++ {
		w.ProcessTick(ctx, dr)
	}
	flushRec := &constants.DispatchRecord{
		Event:        constants.FlushEvent,
		WindowConfig: cfg[0],
		WorkerIdx:    0,
		FlushTsMs:    1_700_000_005_000,
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.FlushWindow(ctx, flushRec, nopClient{})
	}
}
