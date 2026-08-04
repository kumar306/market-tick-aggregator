package main

import (
	"context"
	"market-normalizer/backpressure"
	"market-normalizer/config"
	"market-normalizer/constants"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/resync"
	"market-normalizer/worker"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"shared/logger"
	"shared/metrics"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	logger.Log.Info("Normalizer starting...")

	metrics.InitNormalizerMetrics()
	go exposeMetrics()

	cfgPath := os.Getenv("CONFIG_FILE")
	if cfgPath == "" {
		cfgPath = constants.ConfigFilePath
	}
	cfg, err := config.GetConfig(cfgPath)
	if err != nil {
		logger.Log.Error("Failed to load normalizer config. Stopping main()", "err", err)
		os.Exit(1)
	}

	InitPipelineRegistries()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go kafka.KafkaConsumerMetrics(ctx, cfg.KafkaConfig)

	resync.InitResyncProducer(cfg.KafkaConfig)

	numPartitions, err := kafka.MaxPartitionCount(ctx, cfg.KafkaConfig, cfg.KafkaConfig.Topics...)
	if err != nil {
		logger.Log.Error("Failed to fetch upstream topic partition counts. Stopping main()", "err", err)
		os.Exit(1)
	}
	if numPartitions != cfg.WorkerCount {
		logger.Log.Warn("Configured worker_count does not match live partition count; using partition count",
			"configured_worker_count", cfg.WorkerCount,
			"partition_count", numPartitions)
	}

	eventChannels := make([]chan *constants.DispatchRecord, numPartitions)
	for i := range eventChannels {
		eventChannels[i] = make(chan *constants.DispatchRecord, cfg.WorkerQueueSize)
	}

	commitInterval := time.Duration(cfg.KafkaConfig.CommitOffsetIntervalMillis) * time.Millisecond

	for i := 0; i < numPartitions; i++ {
		session, err := kafka.NewWorkerSession(ctx, cfg.KafkaConfig, i)
		if err != nil {
			logger.Log.Error("Failed to create worker session. Stopping main()", "worker", i, "err", err)
			os.Exit(1)
		}
		bp := backpressure.NewController(i, session.Client(), cfg.KafkaConfig.BackpressureConfig, int64(cfg.WorkerQueueSize))
		w := worker.NewWorker(i, eventChannels[i], bp)
		go w.Run(ctx, session, commitInterval)
	}

	logger.Log.Info("Started normalizer service successfully..")

	<-ctx.Done()
	logger.Log.Info("Received interrupt.. shutting down")
}

func InitPipelineRegistries() {
	registry.InitConverterRegistry()
	registry.InitOrdererRegistry()
	registry.InitNormalizerRegistry()
	registry.InitPublisherRegistry()
}

func exposeMetrics() {
	http.Handle("/metrics", promhttp.HandlerFor(&metrics.Registry, promhttp.HandlerOpts{}))
	logger.Log.Info("Exposed normalizer metrics endpoint at 2113", "url", ":2113/metrics")
	err := http.ListenAndServe("0.0.0.0:2113", nil)
	if err != nil {
		logger.Log.Error("Normalizer metrics have stopped", "err", err)
	}
}
