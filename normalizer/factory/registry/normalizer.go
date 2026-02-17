package registry

import (
	"market-normalizer/constants"
	"market-normalizer/factory/normalizer"
	"shared/logger"
	"strings"
	"sync"
)

// normalized ticker - convert from the pipeline message into the generated proto normalized ticker
// normalized depth - similar

// binance - binanceaggtradenormalizer

var normalizerRegistry = make(map[string]constants.NormalizerStrategy)
var onceNormalizer sync.Once

func InitNormalizerRegistry() {
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

	onceNormalizer.Do(func() {
		for _, p := range pairs {
			if err := RegisterNormalizer(p.exchange, p.channel); err != nil {
				logger.Log.Error("Failed to register normalizer, shutting down", "exchange", p.exchange, "channel", p.channel, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredNormalizer(exchange string, channel string) (constants.NormalizerStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := normalizerRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered normalizer from map for key", nil, "key", key)
}

func RegisterNormalizer(exchange string, channel string) error {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	normalizer, err := GetNormalizer(key)
	if err != nil {
		return logger.LogAndWrap("Could not register normalizer", nil, "error", err)
	}
	normalizerRegistry[key] = normalizer
	logger.Log.Info("Registered normalizer for key", "key", key)
	return nil
}

func GetNormalizer(key string) (constants.NormalizerStrategy, error) {
	switch key {
	case "binance:aggtrade":
		return &normalizer.BinanceAggTradeNormalizer{}, nil
	case "binance:depth":
		return &normalizer.BinanceDepthNormalizer{}, nil
	case "coinbase:ticker":
		return &normalizer.CoinbaseTickerNormalizer{}, nil
	case "coinbase:level2":
		return &normalizer.CoinbaseLevel2Normalizer{}, nil
	case "kraken:ticker":
		return &normalizer.KrakenTickerNormalizer{}, nil
	case "kraken:book":
		return &normalizer.KrakenBookNormalizer{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find normalizer for given key", nil, "key", key)
	}
}
