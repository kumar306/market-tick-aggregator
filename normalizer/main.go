package main

import (
	"context"
	"market-normalizer/config"
	"market-normalizer/constants"
	"market-normalizer/dedupe"
	"market-normalizer/dispatcher"
	"market-normalizer/factory"
	"market-normalizer/kafka"
	"os"
	"os/signal"
	"shared/logger"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO:
// viii. create a bg goroutine to commit the marked offsets - use a ticker.C

func main() {

	logger.Log.Info("Normalizer starting...")

	// load the consumer config
	cfg, err := config.GetConfig(constants.ConfigFilePath)
	if err != nil {
		logger.Log.Error("Failed to load normalizer config. Stopping main()", "err", err)
		os.Exit(1)
	}

	InitPipelineRegistries()

	// init redis for dedupe in pipeline
	dedupe.InitRedis(time.Duration(cfg.RedisTtlMinutes) * time.Minute)

	// consumer init
	client := kafka.Init(cfg.KafkaConfig)
	defer kafka.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// create the dispatch channel
	var dispatchChannel chan *kgo.Record = make(chan *kgo.Record, 1000)

	// create the worker channels
	channelPool := dispatcher.CreateWorkerChannels(cfg.WorkerCount)

	// start worker pool
	dispatcher.StartWorkerPool(ctx, channelPool)

	// setup dispatcher
	go dispatcher.StartDispatcher(ctx, dispatchChannel, channelPool, cfg.WorkerCount)

	// start offset committer
	go kafka.OffsetCommitter(ctx, cfg.KafkaConfig.CommitOffsetIntervalMillis)

	// start the consumer loop
	go kafka.ConsumerLoop(ctx, client, dispatchChannel)

	// wait until SIGTERM
	<-ctx.Done()

	logger.Log.Info("Received interrupt.. shutting down")
}

func InitPipelineRegistries() {
	// init all pipeline registries
	factory.InitConverterRegistry()
	factory.InitOrdererRegistry()
	factory.InitNormalizerRegistry()
	factory.InitPublisherRegistry()
}
