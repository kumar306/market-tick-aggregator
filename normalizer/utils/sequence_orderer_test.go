package utils_test

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/dedupe"
	"market-normalizer/factory/orderer"
	"market-normalizer/proto/generated"
	"market-normalizer/worker"
	"shared/metrics"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func MakeMsg(seq int64, symbol string) *constants.PipelineMessage {
	return &constants.PipelineMessage{
		Exchange: "binance",
		Channel:  "aggtrade",
		Symbol:   symbol,
		SeqId:    seq,
		// Record can be nil for this unit test; ProcessBuffer doesn't use record for ordering
	}
}

type MockNormalizer struct {
	// optionally record calls
}

func (m *MockNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	// return a simple object that Publisher will accept; use a lightweight struct
	tick := &generated.NormalizedTicker{Symbol: msg.Symbol, SeqId: msg.SeqId}
	stream, err := proto.Marshal(tick)
	return stream, err
}

type MockPublisher struct {
	publishedSeqs []int64
}

func (m *MockPublisher) Publish(ctx context.Context, raw []byte, partitionKey string, msg *constants.PipelineMessage) {
	var tick generated.NormalizedTicker
	err := proto.Unmarshal(raw, &tick)
	if err != nil {
		return
	}

	m.publishedSeqs = append(m.publishedSeqs, tick.SeqId)
}

func (m *MockPublisher) PublishTopic() string {
	return "test-topic"
}

func TestSequenceOrderer(t *testing.T) {
	metrics.InitNormalizerMetrics()

	var dedupeCount int32
	dedupe.TestingHook = func() error {
		atomic.AddInt32(&dedupeCount, 1)
		return nil
	}
	// ensure hook reset after test
	defer func() { dedupe.TestingHook = nil }()

	orderer := &orderer.BinanceAggTradeOrderer{}

	// call process buffer
	// before that need to have a few messages in the buffer
	ctx, _ := context.WithCancel(context.Background())
	p := &MockPublisher{}
	n := &MockNormalizer{}
	symbolState := &constants.SymbolState{
		Normalizer: n,
		Orderer:    orderer,
		Publisher:  p,
	}

	bufferKey := "binance-aggtrade-btcusdt"
	tempChan := make(chan *constants.DispatchRecord, 10)
	m1 := MakeMsg(1, "btcusdt")

	symbolState.Orderer.SetSymbolState(symbolState)
	symbolState.Orderer.InitOrdererState(m1)

	m4 := MakeMsg(4, "btcusdt")
	orderer.Order(m4, bufferKey, tempChan)

	m3 := MakeMsg(3, "btcusdt")
	orderer.Order(m3, bufferKey, tempChan)

	m2 := MakeMsg(2, "btcusdt")
	orderer.Order(m2, bufferKey, tempChan)

	d := &constants.DispatchRecord{
		BufferKey: bufferKey,
	}
	wm := make(map[string]*constants.SymbolState, 1)
	wm[bufferKey] = symbolState

	worker.FlushBuffer(ctx, d, wm)

	// here 1 is skipped in flush buffer as it never enters buffer as it was in correct order
	// only 4,3,2 enters buffer -> flush buffer -> process buffer

	// Assert publisher saw buffer messages in sequence order 2,3,4
	require.Equal(t, []int64{2, 3, 4}, p.publishedSeqs, "Published sequence should be in ascending order")

	// Assert dedupe was called for each buffer message
	require.Equal(t, int32(3), atomic.LoadInt32(&dedupeCount))

	// assert cleanup happened
	require.Len(t, symbolState.BufferSeqId, 0, "Buffer seq Id length should be 0 after cleanup")

}
