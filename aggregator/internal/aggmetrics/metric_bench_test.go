package aggmetrics_test

import (
	"market-aggregator/constants"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/proto/generated"
	"testing"
)

var benchCfg5s = &constants.WindowConfig{
	Id:             "5s",
	DurationMs:     5_000,
	FlushCadencyMs: 1_000,
	BucketSizeMs:   500,
}

var benchTick = &generated.NormalizedTick{
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
}

// BenchmarkOHLC_Update measures the cost of a tumbling open/high/low/close update.
func BenchmarkOHLC_Update(b *testing.B) {
	m := &aggmetrics.OHLC{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}

// BenchmarkVWAP_Update measures the cost of a tumbling volume-weighted average price update.
func BenchmarkVWAP_Update(b *testing.B) {
	m := &aggmetrics.VWAP{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}

// BenchmarkRollingVWAP_Update measures the bucket-based rolling VWAP (includes bucket advance logic).
func BenchmarkRollingVWAP_Update(b *testing.B) {
	m := aggmetrics.NewRollingVWAP(benchCfg5s)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}

// BenchmarkEMA_Update measures the exponential moving average decay calculation.
func BenchmarkEMA_Update(b *testing.B) {
	m := aggmetrics.NewEMA(benchCfg5s)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}

// BenchmarkATR_Update measures Average True Range (uses three High/Low/Close comparisons).
func BenchmarkATR_Update(b *testing.B) {
	m := aggmetrics.NewATR(benchCfg5s)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}

// BenchmarkRollingVolume_Update measures the ring-buffer rolling volume accumulator.
func BenchmarkRollingVolume_Update(b *testing.B) {
	m := aggmetrics.NewRollingVolume(benchCfg5s)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.Update(benchTick)
	}
}
