package kraken

import "testing"

func TestKrakenNormalizerSkipsHeartbeat(t *testing.T) {
	normalizer := &KrakenNormalizer{Channel: TickerType}

	symbol, normalized, err := normalizer.Normalize([]byte(`{"channel":"heartbeat"}`))
	if err != nil {
		t.Fatalf("expected heartbeat to be skipped without error, got %v", err)
	}
	if symbol != nil {
		t.Fatalf("expected nil symbol for heartbeat, got %q", symbol)
	}
	if normalized != nil {
		t.Fatalf("expected nil normalized payload for heartbeat, got %q", normalized)
	}
}

func TestKrakenNormalizerSkipsSubscribeAck(t *testing.T) {
	normalizer := &KrakenNormalizer{Channel: TickerType}

	symbol, normalized, err := normalizer.Normalize([]byte(`{"method":"subscribe","result":{"channel":"ticker","symbol":"BTC/USD"},"success":true}`))
	if err != nil {
		t.Fatalf("expected subscribe ack to be skipped without error, got %v", err)
	}
	if symbol != nil {
		t.Fatalf("expected nil symbol for subscribe ack, got %q", symbol)
	}
	if normalized != nil {
		t.Fatalf("expected nil normalized payload for subscribe ack, got %q", normalized)
	}
}

func TestKrakenNormalizerNormalizesTickerPayload(t *testing.T) {
	normalizer := &KrakenNormalizer{Channel: TickerType}

	symbol, normalized, err := normalizer.Normalize([]byte(`{"channel":"ticker","data":[{"symbol":"BTC/USD","last":123.45}]}`))
	if err != nil {
		t.Fatalf("expected ticker payload to normalize, got %v", err)
	}
	if string(symbol) != "BTC/USD" {
		t.Fatalf("expected symbol BTC/USD, got %q", string(symbol))
	}
	if len(normalized) == 0 {
		t.Fatal("expected normalized payload to be non-empty")
	}
}
