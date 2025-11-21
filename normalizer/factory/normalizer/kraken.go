package normalizer

import (
	"market-normalizer/constants"
	"market-normalizer/proto/generated"
	"shared/logger"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type KrakenTickerNormalizer struct{}

func (k *KrakenTickerNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	rawMessage, ok := msg.RawMessage.(*constants.KrakenTickerMsg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	normalizedMsg := generated.NormalizedTicker{
		Exchange: msg.Exchange,
		Channel:  msg.Channel,
		Symbol:   msg.Symbol,
		Price:    rawMessage.Data[0].Last,
		Volume:   rawMessage.Data[0].Volume,
		Low:      rawMessage.Data[0].Low,
		High:     rawMessage.Data[0].High,
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}

type KrakenBookNormalizer struct{}

func (k *KrakenBookNormalizer) Normalize(msg *constants.PipelineMessage) ([]byte, error) {
	rawMessage, ok := msg.RawMessage.(*constants.KrakenBookMsg)
	if !ok {
		return nil, logger.LogAndWrap("Failed to parse raw message", nil,
			"feed", msg.Exchange,
			"channel", msg.Channel,
			"symbol", msg.Symbol,
			"msg", msg.RawMessage)
	}

	normalizedMsg := generated.NormalizedBook{
		Exchange:  msg.Exchange,
		Channel:   msg.Channel,
		Symbol:    msg.Symbol,
		EventType: msg.EventType,
		Bids:      []*generated.NormalizedBook_BookLevel{},
		Asks:      []*generated.NormalizedBook_BookLevel{},
	}

	for _, bid := range rawMessage.Data[0].Bids {
		normalizedMsg.Bids = append(normalizedMsg.Bids, &generated.NormalizedBook_BookLevel{
			Price:  bid.Price,
			Volume: bid.Qty,
		})
	}

	for _, ask := range rawMessage.Data[0].Asks {
		normalizedMsg.Asks = append(normalizedMsg.Asks, &generated.NormalizedBook_BookLevel{
			Price:  ask.Price,
			Volume: ask.Qty,
		})
	}

	if rawMessage.Type == constants.Update {
		parsedTime, err := time.Parse(time.RFC3339, rawMessage.Data[0].Timestamp)
		if err != nil {
			return nil, logger.LogAndWrap("Normalizer stage: Error when parsing time", err, "exchange", msg.Exchange, "channel", msg.Channel, "symbol", msg.Symbol)
		}

		normalizedMsg.Timestamp = timestamppb.New(parsedTime)
	}

	protoStream, err := proto.Marshal(&normalizedMsg)
	if err != nil {
		return nil, logger.LogAndWrap("Error in marshalling protobuf", err, "exchange", msg.Exchange, "channel", msg.Channel)
	}

	return protoStream, nil
}
