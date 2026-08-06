package binance_test

import (
	"market-adapter/feeds/binance"
	"testing"
)

var benchAggTradeRaw = []byte(`{"e":"aggTrade","E":123456789,"s":"BTCUSDT","a":5933014,"p":"65000.50","q":"0.5","f":100,"l":105,"T":123456785,"m":true}`)

// BenchmarkBinanceNormalize measures the adapter hot path: raw exchange JSON
// in, symbol + normalized bytes out. This runs before the message ever
// touches the ring buffer or Kafka.
func BenchmarkBinanceNormalize(b *testing.B) {
	n := &binance.BinanceNormalizer{Channel: "aggTrade"}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, err := n.Normalize(benchAggTradeRaw); err != nil {
			b.Fatalf("Normalize failed: %v", err)
		}
	}
}
