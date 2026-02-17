package normalizer

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"shared/logger"
	"strconv"
	"time"

	"google.golang.org/protobuf/proto"
)

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
		Exchange:      msg.Exchange,
		Channel:       msg.Channel,
		Symbol:        msg.Symbol,
		Price:         floatPrice,
		Volume:        floatVolume,
		SeqId:         msg.SeqId,
		Open:          floatOpen,
		Close:         floatClose,
		Low:           floatLow,
		High:          floatHigh,
		EventTsMillis: parsedTimestamp.UnixMilli(),
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}

type CoinbaseLevel2Normalizer struct{}

func (c *CoinbaseLevel2Normalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	rawMessage, ok := msg.RawMessage.(*constants.CoinbaseLevel2Msg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	parsedTime, err := time.Parse(time.RFC3339, rawMessage.Time)
	if err != nil {
		return nil, logger.LogAndWrap("Normalizer stage: Error when parsing time", err, "exchange", msg.Exchange, "channel", msg.Channel, "symbol", msg.Symbol)
	}

	var normalizedMsg generated.NormalizedBook = generated.NormalizedBook{
		Exchange:        msg.Exchange,
		Channel:         msg.Channel,
		Symbol:          msg.Symbol,
		Bids:            []*generated.NormalizedBook_BookLevel{},
		Asks:            []*generated.NormalizedBook_BookLevel{},
		EventType:       rawMessage.Type,
		EventTimeMillis: parsedTime.UnixMilli(),
	}

	if rawMessage.Type == constants.Snapshot {

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

	} else {
		for _, change := range rawMessage.Changes {
			floatPrice, _ := strconv.ParseFloat(change[1], 64)
			floatVolume, _ := strconv.ParseFloat(change[2], 64)

			if change[0] == constants.Buy {
				normalizedMsg.Bids = append(normalizedMsg.Bids, &generated.NormalizedBook_BookLevel{
					Price:  floatPrice,
					Volume: floatVolume,
				})
			} else {
				normalizedMsg.Asks = append(normalizedMsg.Asks, &generated.NormalizedBook_BookLevel{
					Price:  floatPrice,
					Volume: floatVolume,
				})
			}
		}
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}
