package worker_test

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/utils"
	"market-aggregator/worker"
	"shared/logger"
	"shared/metrics"
	"sync"
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

// start a worker. let a tick arrive first time.
// it should build the windows and wire up the metrics

func Test_WindowMetricsCreation(t *testing.T) {

	metrics.InitAggregatorMetrics()

	cfg := []*constants.WindowConfig{
		{Id: "1s", DurationMs: 1000, FlushCadencyMs: 1000, BucketSizeMs: 500},
		{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 500},
	}
	client := &utils.MockClient{}

	// create a worker
	workerCh := make(chan *constants.DispatchRecord, 10)
	w := worker.NewWorker(1, workerCh, cfg)
	go w.Run(context.Background(), client)

	internal.InitMetricRegistry()

	mockProto := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         144.22,
		Volume:        30,
		EventTsMillis: 1010032331,
		Open:          136.09,
		Close:         140.65,
		Low:           136.05,
		High:          140.92,
		SeqId:         3566310,
	}

	val, err := proto.Marshal(mockProto)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}
	bufferKey := "coinbase:ticker:ETH-USD"

	rec := &kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val}

	mockProto2 := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         145.22,
		Volume:        31,
		EventTsMillis: 1010032334,
		Open:          138.09,
		Close:         144.65,
		Low:           134.05,
		High:          146.23,
		SeqId:         3566313,
	}

	val2, err := proto.Marshal(mockProto2)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}

	rec2 := &kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val2}

	// create dispatch rec
	dispatchRec := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Tick:      mockProto,
		Record:    rec,
		WorkerIdx: w.ID,
		BufferKey: bufferKey,
	}

	dispatchRec2 := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Tick:      mockProto2,
		Record:    rec2,
		WorkerIdx: w.ID,
		BufferKey: bufferKey,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	worker.WorkerTestingHook = func() {
		wg.Done()
	}

	w.Channel <- dispatchRec

	wg.Add(1)
	w.Channel <- dispatchRec2

	wg.Wait()

	windowState := w.SymbolState[bufferKey]

	require.Equal(t, "coinbase", windowState.Exchange, "exchange is not correctly set")
	require.Equal(t, "ticker", windowState.Channel, "channel is not correctly set")
	require.Equal(t, "ETH-USD", windowState.Symbol, "symbol is not correctly set")

	require.Equal(t, len(cfg), len(windowState.Windows), "number of windows should be created = number of window configs")

	foundOHLC := false

	for _, val := range windowState.Windows {
		// verify window config
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

	cfg := []*constants.WindowConfig{
		{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 1000},
	}

	client := &utils.MockClient{}

	kafka.KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "kafka-cb",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return false
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Log.Info("Aggregator kafka cb changed states", "from", from, "to", to)
		},
	})

	// create a worker
	workerCh := make(chan *constants.DispatchRecord, 10)
	w := worker.NewWorker(1, workerCh, cfg)
	go w.Run(context.Background(), client)

	internal.InitMetricRegistry()

	mockProto := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         144.22,
		Volume:        30,
		EventTsMillis: 1010032331,
		Open:          136.09,
		Close:         140.65,
		Low:           136.05,
		High:          140.92,
		SeqId:         3566310,
	}

	val, err := proto.Marshal(mockProto)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}
	bufferKey := "coinbase:ticker:ETH-USD"

	rec := &kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val}

	mockProto2 := &generated.NormalizedTick{
		Exchange:      "coinbase",
		Channel:       "ticker",
		Symbol:        "ETH-USD",
		Price:         145.22,
		Volume:        31,
		EventTsMillis: 1010032334,
		Open:          138.09,
		Close:         144.65,
		Low:           134.05,
		High:          146.23,
		SeqId:         3566313,
	}

	val2, err := proto.Marshal(mockProto2)
	if err != nil {
		t.Logf("Error in constructing mock proto: %v", err)
	}

	rec2 := &kgo.Record{Key: []byte(bufferKey), Topic: "normalized.ticks", Value: val2}

	// create dispatch rec
	dispatchRec := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Tick:      mockProto,
		Record:    rec,
		WorkerIdx: w.ID,
		BufferKey: bufferKey,
	}

	dispatchRec2 := &constants.DispatchRecord{
		Event:     constants.ProcessEvent,
		Tick:      mockProto2,
		Record:    rec2,
		WorkerIdx: w.ID,
		BufferKey: bufferKey,
	}

	wg := sync.WaitGroup{}
	wg.Add(1)

	worker.WorkerTestingHook = func() {
		wg.Done()
	}

	w.Channel <- dispatchRec

	wg.Add(1)
	w.Channel <- dispatchRec2

	wg.Wait()

	flushRec := &constants.DispatchRecord{
		Event:        constants.FlushEvent,
		WindowConfig: cfg[0],
		WorkerIdx:    w.ID,
	}

	w.FlushWindow(context.Background(), flushRec, client)

	st := w.SymbolState[bufferKey]

	for id, win := range st.Windows {
		if id == "5s" {
			for _, m := range win.Metrics {
				if _, ok := m.(*aggmetrics.OHLC); ok {
					// validate that the tumbling metric got reset
					require.Zero(t, m.GetValue())
				}
			}
		}
	}
}

func TestCB(t *testing.T) {

	kafka.KafkaBreaker = gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: "test-trigger-breaker",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			if counts.ConsecutiveFailures > 2 {
				return true
			}
			return false
		},
		OnStateChange: func(name string, from, to gobreaker.State) {
			logger.Log.Info("State change occurred", "from", from, "to", to)
		},
	})
	kafka.DownstreamTopic = "some-topic"
	kafka.ProducerErrors = make(chan error, 10)
	go kafka.MonitorKafkaBreaker(context.Background())

	agg := &generated.AggregatedTick{
		Exchange: "exchange_x",
		Channel:  "channel_y",
		Symbol:   "symbol_z",
	}
	client := &utils.BreakerTestClient{
		Promise: func(r *kgo.Record, err error) {
			kafka.ProducerErrors <- err
		},
	}

	wg := &sync.WaitGroup{}

	kafka.KafkaBreakerTestingHook = func() {
		wg.Done()
	}

	for i := 0; i < 3; i++ {
		wg.Add(1)
		kafka.PublishAggregate(agg, client)
	}

	wg.Wait()

	require.Equal(t, gobreaker.StateOpen, kafka.KafkaBreaker.State(), "Breaker should be open after consecutive failures")
}
