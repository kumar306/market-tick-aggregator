package backpressure_test

import (
	"context"
	"market-normalizer/backpressure"
	"market-normalizer/constants"
	"market-normalizer/dispatcher"
	"market-normalizer/utils/kafkatest"
	"market-normalizer/worker"
	"shared/metrics"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBackpressure(t *testing.T) {

	metrics.InitNormalizerMetrics()

	ctx, _ := context.WithCancel(context.Background())

	// create the worker channels
	workerChannels := dispatcher.CreateWorkerChannels(4, 5)

	backpressureConfig := &constants.BackpressureConfig{
		QueueUsageHighThreshold: 0.8,
		QueueUsageLowThreshold:  0.4,
		ThresholdActiveMillis:   1000,
		CooldownTimeMillis:      2000,
	}

	dispatcher.WorkerPartitionAssignmentsHandler.SetPartitionAssignments(0, "coinbase.ticker", 2)

	mock := &kafkatest.MockKafkaClient{}

	// fill in records into the worker channels
	rec := &kgo.Record{Key: []byte("ETH-USD"), Partition: 2, Topic: "coinbase.ticker", Value: []byte("{\"exchange\":\"coinbase\", \"channel\":\"ticker\"}")}

	workerRec := &constants.DispatchRecord{
		Record:    rec,
		Exchange:  constants.Coinbase,
		Channel:   constants.Ticker,
		Symbol:    "ETH-USD",
		BufferKey: "coinbase-ticker-eth-usd",
	}

	// start worker metrics
	go worker.StartWorkerMetrics(ctx, workerChannels)

	for i := 0; i < 4; i++ {
		workerChannels[0] <- workerRec
	}

	// start bp controller
	go backpressure.BackpressureController(ctx, mock, workerChannels, backpressureConfig)

	// wg.wait then sleep for 5 seconds to trigger bp
	time.Sleep(10 * time.Second)

	require.NotEmpty(t, mock.Paused, "PauseFetchPartitions was not called")

	for len(workerChannels[0]) > 0 {
		<-workerChannels[0]
	}

	time.Sleep(10 * time.Second)

	require.NotEmpty(t, mock.Resumed, "ResumeFetchPartitions was not called")
}
