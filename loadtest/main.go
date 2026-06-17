// Load test for the market-tick-aggregator pipeline.
//
// Measures Kafka producer ACK latency and sustained throughput by injecting
// synthetic NormalizedTick events into the normalized.ticks topic at
// escalating rates.  The running aggregator, normalizer, and persistence
// services pick up these events and process them normally.
//
// Usage (requires docker-compose stack to be running):
//
//	go run . [-kafka localhost:9092] [-step 30s]
//
// After the run, query Prometheus for per-service processing metrics:
//
//	http://localhost:9090
//	  aggregator_tick_processing_duration_ms  – per-tick metric-update latency
//	  aggregator_window_flush_duration_ms      – window flush + serialisation
//	  normalizer_commit_latency_seconds        – Kafka offset commit latency
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/encoding/protowire"
)

// encodeTick manually encodes a NormalizedTick protobuf message using the wire
// format defined in normalizer/proto/normalized_ticker.proto.
// Field numbers: exchange=1, channel=2, symbol=3, event_ts_millis=4,
//
//	price=5, volume=6, open=7, close=8, low=9, high=10, seq_id=11.
func encodeTick(
	exchange, channel, symbol string,
	eventTsMs int64,
	price, volume, open, closep, low, high float64,
	seqId int64,
) []byte {
	var b []byte
	b = protowire.AppendTag(b, 1, protowire.BytesType)
	b = protowire.AppendString(b, exchange)
	b = protowire.AppendTag(b, 2, protowire.BytesType)
	b = protowire.AppendString(b, channel)
	b = protowire.AppendTag(b, 3, protowire.BytesType)
	b = protowire.AppendString(b, symbol)
	b = protowire.AppendTag(b, 4, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(eventTsMs))
	b = protowire.AppendTag(b, 5, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(price))
	b = protowire.AppendTag(b, 6, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(volume))
	b = protowire.AppendTag(b, 7, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(open))
	b = protowire.AppendTag(b, 8, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(closep))
	b = protowire.AppendTag(b, 9, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(low))
	b = protowire.AppendTag(b, 10, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, math.Float64bits(high))
	b = protowire.AppendTag(b, 11, protowire.VarintType)
	b = protowire.AppendVarint(b, uint64(seqId))
	return b
}

// runStats accumulates per-level results in a goroutine-safe way.
type runStats struct {
	sent    atomic.Int64
	acked   atomic.Int64
	errored atomic.Int64
	mu      sync.Mutex
	lats    []time.Duration
}

func (s *runStats) observe(start time.Time, err error) {
	if err != nil {
		s.errored.Add(1)
		return
	}
	s.acked.Add(1)
	lat := time.Since(start)
	s.mu.Lock()
	s.lats = append(s.lats, lat)
	s.mu.Unlock()
}

type summary struct {
	rate    int
	sent    int64
	acked   int64
	errored int64
	avg     time.Duration
	p50     time.Duration
	p95     time.Duration
	p99     time.Duration
	maxLat  time.Duration
}

func pct(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	i := int(p / 100.0 * float64(len(sorted)))
	if i >= len(sorted) {
		i = len(sorted) - 1
	}
	return sorted[i]
}

func calcSummary(rate int, s *runStats) summary {
	s.mu.Lock()
	lats := make([]time.Duration, len(s.lats))
	copy(lats, s.lats)
	s.mu.Unlock()

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

	var total time.Duration
	var maxLat time.Duration
	for _, l := range lats {
		total += l
		if l > maxLat {
			maxLat = l
		}
	}
	var avg time.Duration
	if len(lats) > 0 {
		avg = total / time.Duration(len(lats))
	}
	return summary{
		rate:    rate,
		sent:    s.sent.Load(),
		acked:   s.acked.Load(),
		errored: s.errored.Load(),
		avg:     avg,
		p50:     pct(lats, 50),
		p95:     pct(lats, 95),
		p99:     pct(lats, 99),
		maxLat:  maxLat,
	}
}

// runLevel produces ticks at the requested rate for stepDur, returns stats.
func runLevel(ctx context.Context, client *kgo.Client, topic string, rate int, stepDur time.Duration) summary {
	s := &runStats{}
	deadline := time.Now().Add(stepDur)

	// On Windows, time.Sleep resolution is ~1 ms, so we batch messages per
	// millisecond tick for rates above 1000/s to maintain accuracy.
	batchPerMs := 1
	sleepInterval := time.Second / time.Duration(rate)
	if sleepInterval < time.Millisecond {
		batchPerMs = rate / 1000
		if batchPerMs < 1 {
			batchPerMs = 1
		}
		sleepInterval = time.Millisecond
	}

	seq := int64(0)
	const basePrice = 65_000.0

	for time.Now().Before(deadline) && ctx.Err() == nil {
		for i := 0; i < batchPerMs; i++ {
			seq++
			price := basePrice + rand.Float64()*200 - 100
			vol := 0.1 + rand.Float64()

			val := encodeTick(
				"loadtest", "benchmark", "BTC-USD",
				time.Now().UnixMilli(),
				price, vol,
				basePrice-50,
				price+rand.Float64()*50-25,
				basePrice-100,
				basePrice+100,
				seq,
			)

			rec := &kgo.Record{
				Topic: topic,
				Key:   []byte("loadtest:benchmark:BTC-USD"),
				Value: val,
			}

			sendTime := time.Now()
			s.sent.Add(1)
			client.Produce(ctx, rec, func(_ *kgo.Record, err error) {
				s.observe(sendTime, err)
			})
		}

		time.Sleep(sleepInterval)
	}

	// Drain in-flight records before returning.
	flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = client.Flush(flushCtx)

	return calcSummary(rate, s)
}

func fmtDur(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d < time.Millisecond {
		return fmt.Sprintf("%.0fµs", float64(d.Microseconds()))
	}
	return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func main() {
	kafkaAddr := flag.String("kafka", envOr("KAFKA_ADDR", "localhost:9092"), "Kafka bootstrap address")
	topic := flag.String("topic", "normalized.ticks", "Kafka topic to produce into")
	stepDur := flag.Duration("step", 30*time.Second, "Duration to sustain each rate level")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	fmt.Println("┌──────────────────────────────────────────────────┐")
	fmt.Println("│   Market Tick Aggregator — Pipeline Load Test    │")
	fmt.Println("└──────────────────────────────────────────────────┘")
	fmt.Printf("Kafka broker  : %s\n", *kafkaAddr)
	fmt.Printf("Topic         : %s\n", *topic)
	fmt.Printf("Step duration : %s\n\n", *stepDur)

	client, err := kgo.NewClient(
		kgo.SeedBrokers(*kafkaAddr),
		kgo.RecordDeliveryTimeout(15*time.Second),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed to create Kafka client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := client.Ping(pingCtx); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: Kafka ping failed: %v\n", err)
		fmt.Fprintln(os.Stderr, "Hint: is docker-compose up? Check KAFKA_ADDR env or -kafka flag.")
		os.Exit(1)
	}
	pingCancel()
	fmt.Println("Connected to Kafka.\n")

	rates := []int{500, 1_000, 2_500, 5_000, 10_000}
	var results []summary

	for _, rate := range rates {
		if ctx.Err() != nil {
			break
		}
		fmt.Printf("%6d ticks/sec ... ", rate)
		r := runLevel(ctx, client, *topic, rate, *stepDur)
		results = append(results, r)
		actualThroughput := float64(r.acked) / stepDur.Seconds()
		fmt.Printf("acked=%-8d throughput=%7.0f/s | avg=%-8s p50=%-8s p95=%-8s p99=%s\n",
			r.acked, actualThroughput,
			fmtDur(r.avg), fmtDur(r.p50), fmtDur(r.p95), fmtDur(r.p99))
	}

	if len(results) == 0 {
		return
	}

	fmt.Println()
	fmt.Println("╔════════════════════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║         LOAD TEST SUMMARY — Kafka Producer ACK Latency (broker on localhost)         ║")
	fmt.Println("╠═════════════╦════════════╦══════════╦══════════╦══════════╦══════════╦══════════╦═════╣")
	fmt.Printf("║ %-11s ║ %-10s ║ %-8s ║ %-8s ║ %-8s ║ %-8s ║ %-8s ║ %-3s ║\n",
		"Rate(msg/s)", "Throughput", "Avg", "p50", "p95", "p99", "Max", "Err")
	fmt.Println("╠═════════════╬════════════╬══════════╬══════════╬══════════╬══════════╬══════════╬═════╣")
	for _, r := range results {
		throughput := float64(r.acked) / stepDur.Seconds()
		fmt.Printf("║ %-11d ║ %9.0f/s ║ %-8s ║ %-8s ║ %-8s ║ %-8s ║ %-8s ║ %-3d ║\n",
			r.rate, throughput,
			fmtDur(r.avg), fmtDur(r.p50), fmtDur(r.p95), fmtDur(r.p99), fmtDur(r.maxLat),
			r.errored)
	}
	fmt.Println("╚═════════════╩════════════╩══════════╩══════════╩══════════╩══════════╩══════════╩═════╝")
	fmt.Println()
	fmt.Println("Latency = Kafka broker ACK round-trip (producer → broker → ack callback).")
	fmt.Println()
	fmt.Println("For per-service processing metrics, query Prometheus at http://localhost:9090")
	fmt.Println("  aggregator_tick_processing_duration_ms  – per-tick hot-path (130 metric updates)")
	fmt.Println("  aggregator_window_flush_duration_ms      – window flush + proto serialisation")
	fmt.Println("  aggregator_dedupe_latency_seconds        – Redis dedup round-trip")
	fmt.Println("  normalizer_commit_latency_seconds        – offset commit latency")
}
