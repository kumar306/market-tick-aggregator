package main

import (
	"context"
	"market-adapter/config"
	"market-adapter/constants"
	feedFactory "market-adapter/feeds"
	"market-adapter/feeds/binance"
	"market-adapter/feeds/coinbase"
	"market-adapter/feeds/kraken"
	"market-adapter/internal"
	"market-adapter/kafka"
	"market-adapter/metrics"
	"net/http"
	"os"
	"os/signal"
	"shared/logger"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// start feeds
func main() {

	// register the feed factories
	go registerFeedFactories()

	// init and expose prometheus metrics
	metrics.Init()
	// inc app starts metric
	metrics.AppStarts.Inc()

	go exposeMetrics()

	// have a map of string ("feed key") to its Config struct
	var c *constants.Config
	c, err := config.GetConfig(constants.ConfigFile)
	// If any validation errors, return
	if err != nil {
		logger.Log.Error("Failed to load feed config. Stopping main()", "err", err)
		os.Exit(1)
	}

	c.FeedMap = make(map[string]*constants.Feed)

	// handle shutdown on SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// kafka setup
	kafka.Init(c.BootstrapServers)
	defer kafka.Close()

	// using this to coordinate to shutdown the supervisor goroutines
	var wg sync.WaitGroup

	// stream through config streams and start supervisors
	for _, feed := range c.Feeds {
		for _, stream := range feed.Streams {

			// create supervisor and get the stream handler
			handler, handlerErr := feedFactory.GetStreamHandler(feed.Name, stream)
			if handlerErr != nil {
				logger.Log.Error("Error occurred when retrieving stream handler for stream", "error", handlerErr, "name", feed.Name)
				metrics.FeedErrors.WithLabelValues("feed_name", feed.Name).Inc()
				continue
			}

			// each supervisor has an internal waitgroup to manage its child goroutines - read, heartbeat, monitor
			supervisor := &constants.Supervisor{
				Wg:           &sync.WaitGroup{},
				Handler:      handler,
				StatusChan:   make(chan constants.Status, 3),
				LastPongTime: time.Now(),
			}

			go internal.StartSupervisor(supervisor, feed, stream, ctx, &wg)
		}
		c.FeedMap[feed.Name] = feed
	}

	logger.Log.Info("Supervisors startup execution initiated")

	// blocks until SIGTERM
	<-ctx.Done()
	logger.Log.Info("Received termination signal. Stopping supervisors and its processes gracefully.")
	wg.Wait()
	logger.Log.Info("Stopped all supervisors and its processes")

	// inc app shutdown metric
	metrics.AppShutdowns.Inc()
}

func exposeMetrics() {
	http.Handle("/metrics", promhttp.Handler())
	logger.Log.Info("Exposed metrics endpoint at 2112", "url", ":2112/metrics")
	err := http.ListenAndServe("0.0.0.0:2112", nil)
	if err != nil {
		logger.Log.Error("Metrics have stopped", "err", err)
	}
}

func registerFeedFactories() {
	feedFactory.RegisterFeedFactory("binance", &binance.BinanceFactory{})
	feedFactory.RegisterFeedFactory("coinbase", &coinbase.CBFactory{})
	feedFactory.RegisterFeedFactory("kraken", &kraken.KrakenFactory{})
}
