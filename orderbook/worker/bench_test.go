package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/kafka"
	"market-orderbook/proto/generated"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func newBenchOrderbookWorker() (*Worker, *constants.DispatchRecord) {
	updateCh := make(chan *constants.DispatchRecord, 1)
	ackCh := make(chan *constants.Ack, 1)
	updateAckCh := make(chan *constants.Ack, 1)

	w := NewWorker(0, context.Background(), 10, 15, updateCh, ackCh, updateAckCh)

	rec := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Exchange:  "binance",
		Symbol:    "BTCUSDT",
		Partition: 0,
		Offset:    1,
		TsMs:      1_700_000_000_000,
		Update: &generated.NormalizedBook{
			Bids: []*generated.NormalizedBook_BookLevel{
				{Price: 65000.0, Volume: 1.5},
				{Price: 64999.5, Volume: 2.0},
			},
			Asks: []*generated.NormalizedBook_BookLevel{
				{Price: 65000.5, Volume: 1.2},
				{Price: 65001.0, Volume: 0.8},
			},
		},
	}

	// Pre-populate state directly, bypassing Redis-backed RestoreOrCreateState
	// entirely -- ProcessBookUpdate's steady-state (already-known symbol)
	// path is what's being measured, not cold-start restore.
	buf := make([]byte, 0, 64)
	buf = appendLowerASCII(buf, []byte(rec.Exchange))
	buf = append(buf, ':')
	buf = appendLowerASCII(buf, []byte(rec.Symbol))
	key := string(buf)

	w.OrderbookStateMap[key] = &SymbolState{
		Exchange:            rec.Exchange,
		Symbol:              rec.Symbol,
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}

	return w, rec
}

// BenchmarkProcessBookUpdate measures the orderbook hot path: bufferKey
// lookup plus applying bid/ask upserts for an already-known symbol.
func BenchmarkProcessBookUpdate(b *testing.B) {
	initMetrics()
	w, rec := newBenchOrderbookWorker()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.ProcessBookUpdate(rec)
	}
}

// measures building and marshaling one flush cycle's
// worth of top-N book levels per symbol. Uses a real (but unreachable) kgo
// client - Produce is fire-and-forget/non-blocking here, so this measures
// FlushBook's own allocation behavior (proto construction, slice building)
// without needing a live Kafka broker.
func BenchmarkFlushBook(b *testing.B) {
	initMetrics()
	w, rec := newBenchOrderbookWorker()
	for i := 0; i < 200; i++ {
		w.ProcessBookUpdate(rec)
	}

	client, err := kgo.NewClient(kgo.SeedBrokers("127.0.0.1:1"), kgo.MaxBufferedRecords(1_000_000))
	if err != nil {
		b.Fatalf("failed to create kafka client: %v", err)
	}
	defer client.Close()
	kafka.Client = client
	kafka.DownstreamTopic = "orderbook.flush.bench"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		w.FlushBook(int32(i))
	}
}
