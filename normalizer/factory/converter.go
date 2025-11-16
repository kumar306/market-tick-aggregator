package factory

import (
	"encoding/json"
	"market-normalizer/constants"
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
		return &BinanceAggTradeConverter{}, nil
	case "binance:depth":
		return &BinanceDepthConverter{}, nil
	// case "coinbase:ticker":
	// 	return &CoinbaseTickerConverter{}, nil
	// case "coinbase:l2":
	// 	return &CoinbaseDepthConverter{}, nil
	// case "kraken:ticker":
	// 	return &KrakenTickerConverter{}, nil
	// case "kraken:book":
	// 	return &KrakenBookConverter{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find a converter for key", nil, "key", key)
	}
}

type BinanceAggTradeConverter struct{}

func (b *BinanceAggTradeConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var aggTradeMsg constants.BinanceAggTradeMsg
	if err := json.Unmarshal(raw, &aggTradeMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for binance agg trade message.", err)
	}

	return &constants.PipelineMessage{
		SeqId:      aggTradeMsg.AggTradeID,
		RawMessage: &aggTradeMsg,
	}, nil
}

type BinanceDepthConverter struct{}

func (b *BinanceDepthConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var depthUpdateMsg constants.BinanceDepthUpdateMsg
	if err := json.Unmarshal(raw, &depthUpdateMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for binance depth update message.", err)
	}
	return &constants.PipelineMessage{
		SeqId:      depthUpdateMsg.FinalUpdateID,
		RawMessage: &depthUpdateMsg,
	}, nil
}

type CoinbaseTickerConverter struct{}

func (c *CoinbaseTickerConverter) Convert() {}

type CoinbaseDepthConverter struct{}

func (c *CoinbaseDepthConverter) Convert() {}

type KrakenTickerConverter struct{}

func (k *KrakenTickerConverter) Convert() {}

type KrakenBookConverter struct{}

func (k *KrakenBookConverter) Convert() {}
