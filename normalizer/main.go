package main

import (
	"context"
	"market-normalizer/config"
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"os"
	"os/signal"
	"shared/logger"
	"syscall"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TODO:
// iv. create the array of bounded channels - for now 16 channels and create pool of 16 goroutines to receive on each of them. this would be high CPU usage i guess.
// v. pass the client into a goroutine which does poll fetch from the list of topics
// vi. iterate through fetched records. for each fetched record, pass it into a channel
// vii. dispatcher goroutine reading from this channel - does quick top fields retrieval. pushes to the sharded worker
// viii. create a bg goroutine to commit the marked offsets - use a ticker.C

func main() {

	logger.Log.Info("Normalizer starting...")

	// load the consumer config
	cfg, err := config.GetConfig(constants.ConfigFilePath)
	if err != nil {
		logger.Log.Error("Failed to load normalizer config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// consumer init
	client := kafka.Init(cfg.KafkaConfig)
	defer kafka.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// create the dispatch channel
	var dispatchChannel chan *kgo.Record = make(chan *kgo.Record, 1000)

	// start the consumer loop
	go kafka.ConsumerLoop(ctx, client, dispatchChannel)

	// wait until SIGTERM
	<-ctx.Done()

	logger.Log.Info("Received interrupt.. shutting down")
}
