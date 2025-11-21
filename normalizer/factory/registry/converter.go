package registry

import (
	"market-normalizer/constants"
	"market-normalizer/factory/converter"
	"shared/logger"
	"strings"
	"sync"
)

// map of exchange:channel -> ConverterStrategies
// registered at startup so we don't require a concurrent map
var converterRegistry = make(map[string]constants.ConverterStrategy)
var onceConverter sync.Once

func InitConverterRegistry() {
	onceConverter.Do(func() {
		pairs := []struct {
			exchange string
			channel  string
		}{
			{constants.Binance, constants.AggTrade},
			{constants.Binance, constants.Depth},
			{constants.Coinbase, constants.Ticker},
			{constants.Coinbase, constants.Level2},
			{constants.Kraken, constants.Ticker},
			{constants.Kraken, constants.Book},
		}
		for _, p := range pairs {
			if err := RegisterConverter(p.exchange, p.channel); err != nil {
				logger.Log.Error("Failed to register converter, shutting down", "exchange", p.exchange, "channel", p.channel, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredConverter(exchange string, channel string) (constants.ConverterStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := converterRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered converter from map for key", nil, "key", key)
}

func RegisterConverter(exchange string, channel string) error {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	converter, err := GetConverter(key)
	if err != nil {
		return logger.LogAndWrap("Could not register converter", nil, "error", err)
	}
	converterRegistry[key] = converter
	logger.Log.Info("Registered converter for key", "key", key)
	return nil
}

func GetConverter(key string) (constants.ConverterStrategy, error) {
	switch key {
	case "binance:aggtrade":
		return &converter.BinanceAggTradeConverter{}, nil
	case "binance:depth":
		return &converter.BinanceDepthConverter{}, nil
	case "coinbase:ticker":
		return &converter.CoinbaseTickerConverter{}, nil
	case "coinbase:l2":
		return &converter.CoinbaseLevel2Converter{}, nil
	case "kraken:ticker":
		return &converter.KrakenTickerConverter{}, nil
	case "kraken:book":
		return &converter.KrakenBookConverter{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find a converter for key", nil, "key", key)
	}
}
