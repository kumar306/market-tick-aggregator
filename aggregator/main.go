package main

import (
	"context"
	"market-aggregator/config"
	"market-aggregator/constants"
	"market-aggregator/dedupe"
	"market-aggregator/dispatcher"
	"market-aggregator/flush"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"syscall"

	"os"
	"shared/logger"
	"shared/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {

	// load the config
	cfgPath := os.Getenv("CONFIG_FILE")
	if cfgPath == "" {
		cfgPath = constants.ConfigFile
	}
	cfg, err := config.GetConfig(cfgPath)
	if err != nil {
		logger.Log.Error("Failed to load aggregator config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// init prom metrics
	metrics.InitAggregatorMetrics()
	go exposeMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dedupe.InitRedis(cfg.RedisConfig)

	// init kafka client
	kafka.Init(ctx, cfg.KafkaConfig)
	defer kafka.Close()

	// breaker monitoring init
	go kafka.MonitorKafkaBreaker(ctx)

	// wires up the metrics into metric registry
	internal.InitMetricRegistry()

	// Worker count is derived from the upstream topic's live partition count
	// each worker must exclusively own a stable set of
	// partitions so offset commits never
	// interleave across workers for the same partition.
	numPartitions, err := kafka.PartitionCount(ctx, cfg.KafkaConfig.TopicConfig.Upstream)
	if err != nil {
		logger.Log.Error("Failed to fetch upstream topic partition count. Stopping main()", "err", err)
		os.Exit(1)
	}
	if numPartitions != cfg.WorkerCount {
		logger.Log.Warn("Configured worker_count does not match live partition count; using partition count",
			"configured_worker_count", cfg.WorkerCount,
			"partition_count", numPartitions)
	}

	// create worker channels and workers
	workerChannels := dispatcher.CreateWorkerChannels(numPartitions, 1000)
	dispatcher.StartWorkerChannels(ctx, workerChannels, cfg.WindowConfig, kafka.Client)

	// start metric flush schedulers
	flush.StartFlushSchedulers(ctx, workerChannels, cfg.WindowConfig)

	// start dispatcher
	dispatchCh := make(chan *kgo.Record, 1000)
	go dispatcher.RunDispatcher(ctx, dispatchCh, workerChannels)

	// start offset committer and kafka consumer
	go kafka.OffsetCommitter(ctx, cfg.KafkaConfig.CommitOffsetIntervalMillis)
	go kafka.StartConsumer(ctx, dispatchCh)

	logger.Log.Info(
		"Aggregator started",
		"workers", numPartitions,
		"windows", len(cfg.WindowConfig),
	)

	<-ctx.Done()
	logger.Log.Info("Aggregator shutting down")
}

func exposeMetrics() {
	http.Handle("/metrics", promhttp.HandlerFor(&metrics.Registry, promhttp.HandlerOpts{}))
	logger.Log.Info("Exposed aggregator metrics endpoint", "url", ":2114/metrics")
	err := http.ListenAndServe("0.0.0.0:2114", nil)
	if err != nil {
		logger.Log.Error("Aggregator metrics have stopped", "err", err)
	}
}
