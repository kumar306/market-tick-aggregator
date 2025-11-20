package factory

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"shared/logger"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	logger.Log.Info("Registered normalizerer for key", "key", key)
	return nil
}

func GetNormalizer(key string) (constants.NormalizerStrategy, error) {
	switch key {
	case "binance:aggtrade":
		return &BinanceAggTradeNormalizer{}, nil
	case "binance:depth":
		return &BinanceDepthNormalizer{}, nil
	case "coinbase:ticker":
		return &CoinbaseTickerNormalizer{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find normalizer for given key", nil, "key", key)
	}
}

type BinanceAggTradeNormalizer struct{}

// takes in message input
// frames it to a normalized proto message
// converts to a byte stream and returns
func (b *BinanceAggTradeNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {

	rawMessage, ok := msg.RawMessage.(*constants.BinanceAggTradeMsg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	priceFloat, _ := strconv.ParseFloat(rawMessage.Price, 64)
	volumeFloat, _ := strconv.ParseFloat(rawMessage.Quantity, 64)

	var normalizedMsg generated.NormalizedTicker = generated.NormalizedTicker{
		Exchange:      msg.Exchange,
		Channel:       msg.Channel,
		Symbol:        msg.Symbol,
		EventTsMillis: rawMessage.EventTime,
		Price:         priceFloat,
		Volume:        volumeFloat,
		SeqId:         msg.SeqId,
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}

type BinanceDepthNormalizer struct{}

func (b *BinanceDepthNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	rawMessage, ok := msg.RawMessage.(*constants.BinanceDepthUpdateMsg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	var normalizedMsg generated.NormalizedBook = generated.NormalizedBook{
		Exchange:  msg.Exchange,
		Channel:   msg.Channel,
		Symbol:    msg.Symbol,
		EventTime: rawMessage.EventTime,
		EventType: rawMessage.EventType,
		Bids:      []*generated.NormalizedBook_BookLevel{},
		Asks:      []*generated.NormalizedBook_BookLevel{},
	}

	// add bids and asks in proto msg
	for _, bid := range rawMessage.Bids {
		floatPrice, _ := strconv.ParseFloat(bid[0], 64)
		floatVolume, _ := strconv.ParseFloat(bid[1], 64)
		normalizedMsg.Bids = append(normalizedMsg.Bids, &generated.NormalizedBook_BookLevel{
			Price:  floatPrice,
			Volume: floatVolume,
		})
	}

	for _, ask := range rawMessage.Asks {
		floatPrice, _ := strconv.ParseFloat(ask[0], 64)
		floatVolume, _ := strconv.ParseFloat(ask[1], 64)
		normalizedMsg.Asks = append(normalizedMsg.Asks, &generated.NormalizedBook_BookLevel{
			Price:  floatPrice,
			Volume: floatVolume,
		})
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}

type CoinbaseTickerNormalizer struct{}

func (c *CoinbaseTickerNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	rawMessage, ok := msg.RawMessage.(*constants.CoinbaseTickerMsg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	floatPrice, _ := strconv.ParseFloat(rawMessage.Price, 64)
	floatVolume, _ := strconv.ParseFloat(rawMessage.Volume24h, 64)
	floatOpen, _ := strconv.ParseFloat(rawMessage.Open24h, 64)
	floatLow, _ := strconv.ParseFloat(rawMessage.Low24h, 64)
	floatHigh, _ := strconv.ParseFloat(rawMessage.High24h, 64)
	floatClose := floatPrice
	parsedTimestamp, err := time.Parse(time.RFC3339, rawMessage.Time)
	if err != nil {
		logger.Log.Warn("Error in parsing raw timestamp", "exchange", msg.Exchange, "channel", msg.Channel, "symbol", msg.Symbol, "seqId", msg.SeqId, "error", err)
	}

	var normalizedMsg generated.NormalizedTicker = generated.NormalizedTicker{
		Exchange:  msg.Exchange,
		Channel:   msg.Channel,
		Symbol:    msg.Symbol,
		Price:     floatPrice,
		Volume:    floatVolume,
		SeqId:     msg.SeqId,
		Open:      floatOpen,
		Close:     floatClose,
		Low:       floatLow,
		High:      floatHigh,
		Timestamp: timestamppb.New(parsedTimestamp),
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}
