package worker_test

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"os"
	"path/filepath"
	"shared/metrics"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

// start a worker. let a tick arrive first time.
// it should build the windows and wire up the metrics

func Test_WindowMetricsCreation(t *testing.T) {

	metrics.InitAggregatorMetrics()

	root, err := os.Getwd()
	if err != nil {
		t.Logf("Failed with error %v", err)
	}

	cfgFilePath := filepath.Join(root+"\\..", constants.ConfigFile)
	yamlData, err := os.ReadFile(cfgFilePath)
	if err != nil {
		t.Logf("read yaml file failed with error %v", err)
	}

	var cfg constants.Config
	parseErr := yaml.Unmarshal(yamlData, &cfg)
	if parseErr != nil {
		t.Logf("Unmarshal failed with error: %v", parseErr)
	}

	// create a worker
	workerCh := make(chan *constants.DispatchRecord, 10)
	w := worker.NewWorker(1, workerCh, cfg.WindowConfig)
	go w.Run(context.Background())

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

	val2, err := proto.Marshal(mockProto)
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

	require.Equal(t, len(cfg.WindowConfig), len(windowState.Windows), "number of windows should be created = number of window configs")

	for _, val := range windowState.Windows {
		// verify window config
		require.Positive(t, val.DurationMs, "should be a valid duration ms")
		require.Positive(t, val.FlushCadencyMs, "should be a valid flush cadency ms")
		metrics := val.Metrics
		require.NotEmpty(t, metrics)
	}
}
