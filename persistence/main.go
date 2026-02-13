package main

import (
	"context"
	"market-persistence/batcher/util"
	"market-persistence/config"
	"market-persistence/db"
	"market-persistence/db/model"
	"market-persistence/db/writer"
	"market-persistence/kafka"
	"market-persistence/pipeline"
	"net/http"
	"os"
	"os/signal"
	"shared/logger"
	"shared/metrics"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	// load the config
	cfg, err := config.GetConfig(config.ConfigFile)
	if err != nil {
		logger.Log.Error("Failed to load persistence config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// init prom metrics
	metrics.InitPersistenceMetrics()
	go exposeMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// init kafka client
	kafka.Init(ctx, cfg.KafkaConfig)
	defer kafka.Close()

	// start postgres
	if err := db.InitDB(ctx); err != nil {
		logger.Log.Error("Failed to start postgres. Stopping main()", "err", err)
		os.Exit(1)
	}

	// start the pipelines which are configured with converter and batcher
	var tickPipeline *pipeline.Pipeline[*model.AggregatedTick]
	var bookPipeline *pipeline.Pipeline[*model.OrderbookFlush]

	tickPipeline = pipeline.InitTickPipeline(ctx,
		config.TickPipelineName,
		config.AggregatedTicksTopic,
		cfg.BatcherConfig.TickBatchConfig.BatchSize,
		time.Duration(cfg.BatcherConfig.TickBatchConfig.IntervalMs)*time.Millisecond,
		func(ctx context.Context, tx util.Tx, rows []*model.AggregatedTick) error {
			return writer.FlushAggregateTicks(ctx, tx, rows)
		})

	bookPipeline = pipeline.InitBookPipeline(ctx,
		config.BookPipelineName,
		config.OrderbookFlushesTopic,
		cfg.BatcherConfig.BookBatchConfig.BatchSize,
		time.Duration(cfg.BatcherConfig.BookBatchConfig.IntervalMs)*time.Millisecond,
		func(ctx context.Context, tx util.Tx, rows []*model.OrderbookFlush) error {
			return writer.FlushOrderbook(ctx, tx, rows)
		})

	// start consumer and relevant metrics
	go kafka.StartConsumer(ctx, tickPipeline, bookPipeline)
	go kafka.RecordConsumerLag(ctx, []string{config.AggregatedTicksTopic, config.OrderbookFlushesTopic})

	logger.Log.Info("Persistence module started...")

	<-ctx.Done()
	logger.Log.Info("Persistence module shutting down...")
}

func exposeMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	logger.Log.Info("Exposed persistence metrics endpoint at 2116", "url", ":2116/metrics")
	err := http.ListenAndServe("0.0.0.0:2116", nil)
	if err != nil {
		logger.Log.Error("Persistence metrics have stopped", "err", err)
	}
}
