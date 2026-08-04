package worker_test

import (
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"market-aggregator/worker"
	"shared/metrics"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// is synchronous now - no dispatcher, no worker channel
func Test_WindowMetricsCreation(t *testing.T) {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()

	cfg := []*constants.WindowConfig{
		{Id: "1s", DurationMs: 1000, FlushCadencyMs: 1000, BucketSizeMs: 500},
		{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 500},
	}

	w := worker.NewWorker(1, make(chan *constants.DispatchRecord, 10), cfg, nil)

	bufferKey := "coinbase:ticker:ETH-USD"

	mockProto := &generated.NormalizedTick{
		Exchange: "coinbase", Channel: "ticker", Symbol: "ETH-USD",
		Price: 144.22, Volume: 30, EventTsMillis: 1010032331,
		Open: 136.09, Close: 140.65, Low: 136.05, High: 140.92, SeqId: 3566310,
	}
	val, err := proto.Marshal(mockProto)
	require.NoError(t, err)
	w.ProcessTick(&kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val})

	mockProto2 := &generated.NormalizedTick{
		Exchange: "coinbase", Channel: "ticker", Symbol: "ETH-USD",
		Price: 145.22, Volume: 31, EventTsMillis: 1010032334,
		Open: 138.09, Close: 144.65, Low: 134.05, High: 146.23, SeqId: 3566313,
	}
	val2, err := proto.Marshal(mockProto2)
	require.NoError(t, err)
	w.ProcessTick(&kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val2})

	windowState := w.SymbolState[bufferKey]

	require.Equal(t, "coinbase", windowState.Exchange, "exchange is not correctly set")
	require.Equal(t, "ticker", windowState.Channel, "channel is not correctly set")
	require.Equal(t, "ETH-USD", windowState.Symbol, "symbol is not correctly set")
	require.Equal(t, len(cfg), len(windowState.Windows), "number of windows should be created = number of window configs")

	foundOHLC := false
	for _, val := range windowState.Windows {
		require.Positive(t, val.DurationMs, "should be a valid duration ms")
		require.Positive(t, val.FlushCadencyMs, "should be a valid flush cadency ms")
		metrics := val.Metrics
		for _, m := range metrics {
			if _, ok := m.(*aggmetrics.OHLC); ok {
				foundOHLC = true
			}
		}
		require.NotEmpty(t, metrics)
		require.True(t, foundOHLC)
	}
}

func TestWorkerFlush(t *testing.T) {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()

	cfg := []*constants.WindowConfig{
		{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 1000},
	}

	w := worker.NewWorker(1, make(chan *constants.DispatchRecord, 10), cfg, nil)

	bufferKey := "coinbase:ticker:ETH-USD"
	mockProto := &generated.NormalizedTick{
		Exchange: "coinbase", Channel: "ticker", Symbol: "ETH-USD",
		Price: 144.22, Volume: 30, EventTsMillis: 1010032331,
		Open: 136.09, Close: 140.65, Low: 136.05, High: 140.92, SeqId: 3566310,
	}
	val, err := proto.Marshal(mockProto)
	require.NoError(t, err)
	w.ProcessTick(&kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val})

	mockProto2 := &generated.NormalizedTick{
		Exchange: "coinbase", Channel: "ticker", Symbol: "ETH-USD",
		Price: 145.22, Volume: 31, EventTsMillis: 1010032334,
		Open: 138.09, Close: 144.65, Low: 134.05, High: 146.23, SeqId: 3566313,
	}
	val2, err := proto.Marshal(mockProto2)
	require.NoError(t, err)
	w.ProcessTick(&kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val2})

	flushRec := &constants.DispatchRecord{
		Event:        constants.FlushEvent,
		WindowConfig: cfg[0],
	}

	w.FlushWindow(flushRec, &utils.MockClient{})

	st := w.SymbolState[bufferKey]
	for id, win := range st.Windows {
		if id == "5s" {
			for _, m := range win.Metrics {
				if _, ok := m.(*aggmetrics.OHLC); ok {
					require.Zero(t, m.GetValue(), "tumbling metric should be reset after flush")
				}
			}
		}
	}
}

// Replaces the old circuit-breaker test: with transactions, a produce
// failure no longer trips a breaker -- it marks the worker's current
// transaction for abort instead, so the whole cycle (including anything
// consumed) rolls back together rather than partially committing.
func TestFlushWindow_ProduceFailureAbortsTransaction(t *testing.T) {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()

	cfg := []*constants.WindowConfig{
		{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 1000},
	}
	w := worker.NewWorker(1, make(chan *constants.DispatchRecord, 10), cfg, nil)

	bufferKey := "coinbase:ticker:ETH-USD"
	mockProto := &generated.NormalizedTick{Exchange: "coinbase", Channel: "ticker", Symbol: "ETH-USD"}
	val, err := proto.Marshal(mockProto)
	require.NoError(t, err)
	w.ProcessTick(&kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val})

	failing := &utils.BreakerTestClient{}
	w.FlushWindow(&constants.DispatchRecord{Event: constants.FlushEvent, WindowConfig: cfg[0]}, failing)

	require.True(t, w.GetTxnFailed(), "produce failure should mark the transaction failed")
}
