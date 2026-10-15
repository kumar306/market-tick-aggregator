package worker

import (
	"context"
	"market-orderbook/book"
	"market-orderbook/constants"
	"market-orderbook/proto/generated"
	"shared/ingestts"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// the flushed book reports the ingest time of the newest update since the last flush, whatever order they arrive in
func TestProcessBookUpdate_TracksNewestIngestTime(t *testing.T) {
	initMetrics()
	w := newTestWorker(context.Background())

	state := &SymbolState{
		Exchange:            "binance",
		Symbol:              "BTC-USD",
		Orderbook:           book.NewOrderBook(),
		LastCommittedOffset: map[int32]int64{},
		LastProcessedOffset: map[int32]int64{},
	}
	w.OrderbookStateMap["binance:btc-usd"] = state

	update := func(offset int64, rec *kgo.Record) *constants.DispatchRecord {
		return &constants.DispatchRecord{
			Partition: 0, Offset: offset, TsMs: 1, Exchange: "binance", Symbol: "BTC-USD", Record: rec,
			Update: &generated.NormalizedBook{Bids: []*generated.NormalizedBook_BookLevel{{Price: 100, Volume: 1}}},
		}
	}
	withIngest := func(ms int64) *kgo.Record {
		return &kgo.Record{Headers: []kgo.RecordHeader{ingestts.Header(ms)}}
	}

	w.ProcessBookUpdate(update(1, withIngest(2_000)))
	w.ProcessBookUpdate(update(2, withIngest(1_000))) // older ingest time arriving later must not move it back
	if state.NewestIngestMs != 2_000 {
		t.Fatalf("expected the newest ingest time 2000, got %d", state.NewestIngestMs)
	}

	w.ProcessBookUpdate(update(3, nil)) // no source record at all: nothing to read, nothing breaks
	w.ProcessBookUpdate(update(4, &kgo.Record{}))
	if state.NewestIngestMs != 2_000 {
		t.Fatalf("an update without a header must leave it alone, got %d", state.NewestIngestMs)
	}
}
