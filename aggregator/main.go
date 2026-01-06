package main

import (
	"context"
	"market-aggregator/config"
	"market-aggregator/constants"
	"market-aggregator/dispatcher"
	"market-aggregator/flush"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"os/signal"
	"syscall"

	"os"
	"shared/logger"
	"shared/metrics"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	// load the config
	cfg, err := config.GetConfig(constants.ConfigFile)
	if err != nil {
		logger.Log.Error("Failed to load aggregator config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// init prom metrics
	metrics.InitAggregatorMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// init kafka client
	kafka.Init(ctx, cfg.KafkaConfig)
	defer kafka.Close()

	// breaker monitoring init
	go kafka.MonitorKafkaBreaker(ctx)

	// wires up the metrics into metric registry
	internal.InitMetricRegistry()

	// create worker channels and workers
	workerChannels := dispatcher.CreateWorkerChannels(cfg.WorkerCount, 1000)
	dispatcher.StartWorkerChannels(ctx, workerChannels, cfg.WindowConfig)

	// start metric flush schedulers
	flush.StartFlushSchedulers(ctx, workerChannels, cfg.WindowConfig)

	// start dispatcher
	dispatchCh := make(chan *kgo.Record, 1000)
	go dispatcher.RunDispatcher(ctx, dispatchCh, workerChannels)

	// start kafka consumer
	go kafka.StartConsumer(ctx, dispatchCh)

	logger.Log.Info(
		"Aggregator started",
		"workers", cfg.WorkerCount,
		"windows", len(cfg.WindowConfig),
	)
}
