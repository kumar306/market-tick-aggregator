package converter

import (
	"testing"
	"time"

	"market-persistence/proto/generated"

	"google.golang.org/protobuf/proto"
)

func TestTickConverterConvertSuccess(t *testing.T) {
	start := time.Now().Add(-time.Minute).UnixMilli()
	end := time.Now().UnixMilli()

	msg := &generated.AggregatedTick{
		Exchange:  "binance",
		Symbol:    "BTCUSDT",
		WindowId:  "1m",
		StartTsMs: start,
		EndTsMs:   end,
		PriceMetrics: &generated.PriceMetrics{
			Ohlc: &generated.OHLC{
				Open:  10,
				High:  15,
				Low:   9,
				Close: 11,
			},
			Vwap:        11.2,
			RollingVwap: 11.1,
			Twap:        11.0,
			Microprice:  11.4,
		},
		VolumeMetrics: &generated.VolumeMetrics{
			Volume:             100,
			RollingVolume:      500,
			VolumeAcceleration: 2.2,
		},
		VolatilityMetrics: &generated.VolatilityMetrics{
			Volatility: 0.9,
			Atr:        1.5,
		},
		TrendMetrics: &generated.TrendMetrics{
			Ema:          11.3,
			Sma:          11.2,
			LogReturn:    0.05,
			SimpleReturn: 0.06,
		},
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	row, err := NewTickConverter().Convert(data)
	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}

	if row.Exchange != "binance" {
		t.Fatalf("Exchange = %q, want %q", row.Exchange, "binance")
	}
	if row.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol = %q, want %q", row.Symbol, "BTCUSDT")
	}
	if row.Open != 10 || row.Close != 11 {
		t.Fatalf("OHLC mismatch, got open=%v close=%v", row.Open, row.Close)
	}
	if row.StartTs.UnixMilli() != start || row.EndTs.UnixMilli() != end {
		t.Fatalf("time conversion mismatch")
	}
}

func TestTickConverterConvertInvalidBytes(t *testing.T) {
	_, err := NewTickConverter().Convert([]byte("invalid-protobuf"))
	if err == nil {
		t.Fatalf("Convert() error = nil, want non-nil")
	}
}

func TestBookConverterConvertSuccess(t *testing.T) {
	ts := time.Now().UnixMilli()
	msg := &generated.OrderbookFlush{
		Exchange:        "okx",
		Symbol:          "ETHUSDT",
		EventTimeMillis: ts,
		Bids: []*generated.OrderbookFlush_BookLevel{
			{Price: 100, Volume: 1},
			{Price: 99, Volume: 2},
		},
		Asks: []*generated.OrderbookFlush_BookLevel{
			{Price: 101, Volume: 1.5},
		},
		BestBid: &generated.OrderbookFlush_BookLevel{Price: 100, Volume: 1},
		BestAsk: &generated.OrderbookFlush_BookLevel{Price: 101, Volume: 1.5},
		Spread:  1,
	}

	data, err := proto.Marshal(msg)
	if err != nil {
		t.Fatalf("proto.Marshal() error = %v", err)
	}

	book, err := NewBookConverter().Convert(data)
	if err != nil {
		t.Fatalf("Convert() error = %v, want nil", err)
	}

	if book.FlushRow.Symbol != "ETHUSDT" {
		t.Fatalf("Symbol = %q, want ETHUSDT", book.FlushRow.Symbol)
	}
	if len(book.LevelRows) != 3 {
		t.Fatalf("level row count = %d, want 3", len(book.LevelRows))
	}
	if book.LevelRows[0].Side != "B" || book.LevelRows[2].Side != "A" {
		t.Fatalf("side mapping mismatch")
	}
	if book.FlushRow.EventTime.UnixMilli() != ts {
		t.Fatalf("event time mismatch")
	}
}

func TestBookConverterConvertInvalidBytes(t *testing.T) {
	_, err := NewBookConverter().Convert([]byte("invalid-protobuf"))
	if err == nil {
		t.Fatalf("Convert() error = nil, want non-nil")
	}
}
