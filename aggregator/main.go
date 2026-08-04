package main

import (
	"context"
	"market-aggregator/config"
	"market-aggregator/constants"
	"market-aggregator/flush"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/worker"
	"net/http"
	_ "net/http/pprof"
	"os/signal"
	"syscall"
	"time"

	"os"
	"shared/logger"
	"shared/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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

	kafka.DownstreamTopic = cfg.KafkaConfig.TopicConfig.Downstream

	// wires up the metrics into metric registry
	internal.InitMetricRegistry()

	go kafka.KafkaConsumerMetrics(ctx, cfg.KafkaConfig)

	// Worker count is derived from the upstream topic's live partition count
	// each worker must exclusively own a stable set of
	// partitions so offset commits never
	// interleave across workers for the same partition.
	numPartitions, err := kafka.PartitionCount(ctx, cfg.KafkaConfig, cfg.KafkaConfig.TopicConfig.Upstream)
	if err != nil {
		logger.Log.Error("Failed to fetch upstream topic partition count. Stopping main()", "err", err)
		os.Exit(1)
	}
	if numPartitions != cfg.WorkerCount {
		logger.Log.Warn("Configured worker_count does not match live partition count; using partition count",
			"configured_worker_count", cfg.WorkerCount,
			"partition_count", numPartitions)
	}

	// get the existing agg state
	checkpoints := kafka.LoadCheckpoints(ctx, cfg.KafkaConfig)

	// create worker channels and workers
	flushChannels := make([]chan *constants.DispatchRecord, numPartitions)
	for i := range flushChannels {
		flushChannels[i] = make(chan *constants.DispatchRecord, 1000)
	}

	commitInterval := time.Duration(cfg.KafkaConfig.CommitOffsetIntervalMillis) * time.Millisecond

	for i := 0; i < numPartitions; i++ {
		session, err := kafka.NewWorkerSession(ctx, cfg.KafkaConfig, i)
		if err != nil {
			logger.Log.Error("Failed to create worker session. Stopping main()", "worker", i, "err", err)
			os.Exit(1)
		}
		w := worker.NewWorker(i, flushChannels[i], cfg.WindowConfig, checkpoints)
		go w.Run(ctx, session, commitInterval)
	}

	// start metric flush schedulers
	flush.StartFlushSchedulers(ctx, flushChannels, cfg.WindowConfig)

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
