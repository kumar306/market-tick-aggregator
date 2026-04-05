package main

import (
	"context"
	"market-normalizer/backpressure"
	"market-normalizer/config"
	"market-normalizer/constants"
	"market-normalizer/dedupe"
	"market-normalizer/dispatcher"
	"market-normalizer/factory/registry"
	"market-normalizer/kafka"
	"market-normalizer/worker"
	"net/http"
	"os"
	"os/signal"
	"shared/logger"
	"shared/metrics"
	"syscall"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO:
// viii. create a bg goroutine to commit the marked offsets - use a ticker.C

func main() {

	logger.Log.Info("Normalizer starting...")

	metrics.InitNormalizerMetrics()

	go exposeMetrics()

	// load the consumer config
	cfgPath := os.Getenv("CONFIG_FILE")
	if cfgPath == "" {
		cfgPath = constants.ConfigFilePath
	}
	cfg, err := config.GetConfig(cfgPath)
	if err != nil {
		logger.Log.Error("Failed to load normalizer config. Stopping main()", "err", err)
		os.Exit(1)
	}

	_, err = kafka.NewWAL(cfg.KafkaConfig)
	if err != nil {
		logger.Log.Error("Failed to initialise WAL. Stopping main()", "err", err)
		os.Exit(1)
	}

	InitPipelineRegistries()

	// init redis for dedupe in pipeline
	dedupe.InitRedis(cfg.RedisConfig)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// consumer init
	client := kafka.Init(ctx, cfg.KafkaConfig)
	defer kafka.Close()

	// detect consumer lag for backpressure alert
	go kafka.KafkaConsumerMetrics(ctx, cfg.KafkaConfig.Topics)

	// create the dispatch channel
	var dispatchChannel chan *kgo.Record = make(chan *kgo.Record, 1000)

	// create the worker channels
	channelPool := dispatcher.CreateWorkerChannels(cfg.WorkerCount, cfg.WorkerQueueSize)

	// start worker pool
	dispatcher.StartWorkerPool(ctx, channelPool)

	// init backpressure state
	backpressure.InitBackpressureController(kafka.Client, cfg.KafkaConfig.BackpressureConfig, int64(cfg.WorkerQueueSize))

	// setup dispatcher
	go dispatcher.StartDispatcher(ctx, dispatchChannel, channelPool)

	// start offset committer
	go kafka.OffsetCommitter(ctx, cfg.KafkaConfig.CommitOffsetIntervalMillis, cfg.KafkaConfig.Topics)

	// kafka publish circuit breaker
	go kafka.MonitorKafkaBreakerState(ctx)

	// start the consumer loop
	go kafka.ConsumerLoop(ctx, client, dispatchChannel)

	// metrics goroutine for worker ingestion
	go worker.StartWorkerMetrics(ctx, channelPool)

	logger.Log.Info("Started normalizer service successfully..")

	// wait until SIGTERM
	<-ctx.Done()

	logger.Log.Info("Received interrupt.. shutting down")
}

func InitPipelineRegistries() {
	// init all pipeline registries
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
