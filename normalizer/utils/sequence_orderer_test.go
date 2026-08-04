package utils_test

import (
	"context"
	"market-normalizer/constants"
	"market-normalizer/factory/orderer"
	"market-normalizer/proto/generated"
	"market-normalizer/worker"
	"shared/metrics"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

var mockOffset int64 = 0

func MakeMsg(seq int64, symbol string) *constants.PipelineMessage {
	mockOffset++
	return &constants.PipelineMessage{
		Exchange: "binance",
		Channel:  "aggtrade",
		Symbol:   symbol,
		SeqId:    seq,
		Record: &kgo.Record{
			Topic:     "sample topic",
			Partition: 3,
			Offset:    int64(mockOffset),
		},
	}
}

type MockNormalizer struct{}

func (m *MockNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	tick := &generated.NormalizedTicker{Symbol: msg.Symbol, SeqId: msg.SeqId}
	return proto.Marshal(tick)
}

type MockPublisher struct {
	publishedSeqs []int64
}

func (m *MockPublisher) Publish(ctx context.Context, raw []byte, partitionKey string, msg *constants.PipelineMessage, producer constants.Producer, onFailure func()) {
	var tick generated.NormalizedTicker
	if err := proto.Unmarshal(raw, &tick); err != nil {
		return
	}
	m.publishedSeqs = append(m.publishedSeqs, tick.SeqId)
}

func (m *MockPublisher) PublishTopic() string {
	return "test-topic"
}

type noopProducer struct{}

func (noopProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {}

func TestSequenceOrderer(t *testing.T) {
	metrics.InitNormalizerMetrics()

	orderer := &orderer.BinanceAggTradeOrderer{}

	ctx := context.Background()
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

	w := worker.NewWorker(1, tempChan, nil)
	w.WorkerMap[bufferKey] = symbolState

	w.FlushBuffer(ctx, &constants.DispatchRecord{BufferKey: bufferKey}, noopProducer{})

	// here 1,4 is skipped in flush buffer as it never enters buffer as it was in correct order
	// only 3,2 enters buffer -> flush buffer -> process buffer

	require.Equal(t, []int64{2, 3}, p.publishedSeqs, "Published sequence should be in ascending order")
	require.Len(t, symbolState.BufferSeqId, 0, "Buffer seq Id length should be 0 after cleanup")
}
