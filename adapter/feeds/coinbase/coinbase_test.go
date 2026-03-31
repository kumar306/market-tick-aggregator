package coinbase

import "testing"

func TestNormalizeChannelMapsLevel2BatchToLevel2(t *testing.T) {
	if got := normalizeChannel(Level2BatchType); got != Level2Type {
		t.Fatalf("normalizeChannel(level2_batch) = %q, want %q", got, Level2Type)
	}

	if got := normalizeChannel(TickerType); got != TickerType {
		t.Fatalf("normalizeChannel(ticker) = %q, want %q", got, TickerType)
	}
}
