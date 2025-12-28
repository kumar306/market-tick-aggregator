package main

import (
	"market-aggregator/config"
	"market-aggregator/constants"
	"market-aggregator/metrics"
	"os"
	"shared/logger"
)

func main() {

	// load the config
	_, err := config.GetConfig(constants.ConfigFile)
	if err != nil {
		logger.Log.Error("Failed to load aggregator config. Stopping main()", "err", err)
		os.Exit(1)
	}

	// wires up the metrics into metric registry
	metrics.InitMetricRegistry()

}
