package worker_test

import (
	"context"
	"encoding/json"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/worker"
	"shared/metrics"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type nopProducer struct{}

func (nopProducer) Produce(_ context.Context, _ *kgo.Record, promise func(*kgo.Record, error)) {
	if promise != nil {
		promise(nil, nil)
	}
}

func init() {
	metrics.InitNormalizerMetrics()
	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()
}

// mirrors constants.BinanceAggTradeMsg's wire shape, plus the exchange/channel
// header fields ProcessRecord reads before routing to the registered pipeline.
type benchAggTradeMsg struct {
	Exchange     string `json:"exchange"`
	Channel      string `json:"channel"`
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	AggTradeID   int64  `json:"a"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	FirstTradeID int64  `json:"f"`
	LastTradeID  int64  `json:"l"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

// newBenchRecord builds a record with a strictly-increasing seq - the
// sequence orderer only takes the immediate-passthrough fast path (convert +
// order + publish, no buffering) when each record is exactly the next
// expected one; a repeated or out-of-order seq gets buffered instead, which
// would benchmark a different code path than the steady-state hot path.
func newBenchRecord(seq int64) *kgo.Record {
	msg := benchAggTradeMsg{
		Exchange:     "binance",
		Channel:      "aggTrade",
		EventType:    "aggTrade",
		EventTime:    1_700_000_000_000 + seq,
		Symbol:       "BTCUSDT",
		AggTradeID:   seq,
		Price:        "65000.50",
		Quantity:     "0.5",
		FirstTradeID: seq,
		LastTradeID:  seq,
		TradeTime:    1_700_000_000_000 + seq,
		IsBuyerMaker: seq%2 == 0,
	}
	val, _ := json.Marshal(msg)
	return &kgo.Record{
		Key:   []byte("BTCUSDT"),
		Topic: "binance.raw.ticks",
		Value: val,
	}
}

// measures the normalizer hot path for an
// already known symbol: header parse, bufferKey construction/lookup,
// convert, order, and publish
func BenchmarkProcessRecord(b *testing.B) {
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 1), nil)
	producer := nopProducer{}

	if err := w.ProcessRecord(context.Background(), newBenchRecord(0), producer); err != nil {
		b.Fatalf("warm-up ProcessRecord failed: %v", err)
	}

	// Pre-build records outside the timed section so JSON marshaling of the
	// test fixture itself doesn't get counted as part of ProcessRecord's cost.
	records := make([]*kgo.Record, b.N)
	for i := range records {
		records[i] = newBenchRecord(int64(i) + 1)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := w.ProcessRecord(context.Background(), records[i], producer); err != nil {
			b.Fatalf("ProcessRecord failed: %v", err)
		}
	}
}
