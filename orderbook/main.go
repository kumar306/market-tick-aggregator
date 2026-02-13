package main

import (
	"context"
	"market-orderbook/backpressure"
	"market-orderbook/config"
	"market-orderbook/constants"
	"market-orderbook/dispatcher"
	"market-orderbook/flush"
	"market-orderbook/kafka"
	"market-orderbook/redis"
	"os"
	"os/signal"
	"shared/logger"
	"shared/metrics"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	// load the config
	cfg, err := config.GetConfig(constants.ConfigFile)
	if err != nil {
		logger.Log.Error("Failed to load orderbook config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// init prom metrics
	metrics.InitOrderbookMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// init kafka client
	kafka.Init(ctx, cfg.KafkaConfig)
	defer kafka.Close()

	// init redis
	redis.InitRedis(cfg.RedisConfig)

	// create channels
	workerChannels := dispatcher.CreateWorkerChannels(cfg.WorkerCount, cfg.WorkerQueueSize)
	workerAckChannels := dispatcher.CreateWorkerAckChannels(cfg.WorkerCount, 1000)
	dispatchChannel := make(chan *kgo.Record, 1000)

	// create coordinator
	coordinator := kafka.NewCoordinator(cfg.WorkerCount, workerAckChannels)

	// init backpressure controller config
	backpressure.InitBP(&cfg.KafkaConfig.BackpressureConfig,
		kafka.Client,
		cfg.KafkaConfig.TopicConfig.Upstream,
		int64(cfg.WorkerQueueSize))

	// start workers
	dispatcher.StartWorkerChannels(ctx, workerChannels, coordinator.FlushAckChannel, workerAckChannels)

	// start dispatcher
	go dispatcher.RunDispatcher(ctx,
		dispatchChannel,
		workerChannels)

	// start coordinator
	go coordinator.Run(ctx, kafka.Client)

	// start epoch flush scheduler to post flush events into worker
	go flush.RunEpochFlushScheduler(ctx, cfg.KafkaConfig.FlushIntervalSeconds, workerChannels, coordinator)

	// start consumer
	go kafka.StartConsumer(ctx, dispatchChannel)

	logger.Log.Info("Orderbook module running")

	<-ctx.Done()
	logger.Log.Info("Orderbook module shutting down.")
}
