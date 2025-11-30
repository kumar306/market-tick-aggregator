package registry

import (
	"market-normalizer/constants"
	"market-normalizer/factory/orderer"
	"shared/logger"
	"strings"
	"sync"
)

// create a registry of ordererStrategy constructors
// to prevent single orderer registry pointer to be shared across multiple symbols of a stream
// this prevents corruption of the orderer/buffer state
type OrdererCtor func() constants.OrdererStrategy

var OrdererCtorRegistry = make(map[string]OrdererCtor)
var onceOrderer sync.Once

func InitOrdererRegistry() {
	onceOrderer.Do(func() {
		pairs := []struct {
			exchange string
			channel  string
			ctor     func() constants.OrdererStrategy
		}{
			{constants.Binance, constants.AggTrade, func() constants.OrdererStrategy {
				return &orderer.BinanceAggTradeOrderer{}
			}},
			{constants.Binance, constants.Depth, func() constants.OrdererStrategy {
				return &orderer.BinanceDepthOrderer{}
			}},
			{constants.Coinbase, constants.Ticker, func() constants.OrdererStrategy {
				return &orderer.CoinbaseTickerOrderer{}
			}},
			{constants.Coinbase, constants.Level2, func() constants.OrdererStrategy {
				return &orderer.CoinbaseLevel2Orderer{}
			}},
			{constants.Kraken, constants.Ticker, func() constants.OrdererStrategy {
				return &orderer.KrakenTickerOrderer{}
			}},
			{constants.Kraken, constants.Book, func() constants.OrdererStrategy {
				return &orderer.KrakenBookOrderer{}
			}},
		}
		for _, p := range pairs {
			if err := RegisterOrdererCtor(p.exchange, p.channel, p.ctor); err != nil {
				logger.Log.Error("Failed to register orderer, shutting down", "exchange", p.exchange, "channel", p.channel, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredOrderer(exchange string, channel string) (constants.OrdererStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := OrdererCtorRegistry[key]; ok {
		return v(), nil
	}

	return nil, logger.LogAndWrap("Could not get registered orderer from map for key", nil, "key", key)
}

func RegisterOrdererCtor(exchange, channel string, ordererCtor OrdererCtor) error {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	OrdererCtorRegistry[key] = ordererCtor
	logger.Log.Info("Registered orderer constructor for key", "key", key)
	return nil
}
