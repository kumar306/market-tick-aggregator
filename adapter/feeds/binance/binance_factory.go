package binance

import (
	"market-adapter/constants"
	"market-adapter/logger"
)

type BinanceFactory struct{}

// registry of string channel to Normalizer variables
func getBinanceNormalizer(stream string) (constants.Normalizer, error) {
	switch stream {
	case "aggTrade":
		return &BinanceAggTradeNormalizer{}, nil
	case "depth":
		return &BinanceDepthNormalizer{}, nil
	default:
		return nil, logger.LogAndWrap("No normalizer matches for given stream", nil, "stream", stream)
	}
}

func (b *BinanceFactory) CreateNormalizer(channel string) (constants.Normalizer, error) {
	// parse the channel and map it to normalizer
	// return that normalizer
	return getBinanceNormalizer(channel)
}

func (b *BinanceFactory) CreateSubscriber(channel string, productIds []string) constants.Subscriber {
	return &BinanceSubscriber{Channel: channel, ProductIds: productIds}
}

func (b *BinanceFactory) CreatePinger() constants.Pinger {
	return &BinancePinger{}
}
