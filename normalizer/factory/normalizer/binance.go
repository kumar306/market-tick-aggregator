package normalizer

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"shared/logger"
	"strconv"

	"google.golang.org/protobuf/proto"
)

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
		Exchange:        msg.Exchange,
		Channel:         msg.Channel,
		Symbol:          msg.Symbol,
		EventTimeMillis: rawMessage.EventTime,
		EventType:       rawMessage.EventType,
		Bids:            []*generated.NormalizedBook_BookLevel{},
		Asks:            []*generated.NormalizedBook_BookLevel{},
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
