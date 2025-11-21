package converter

import (
	"encoding/json"
	"market-normalizer/constants"
	"shared/logger"
	"time"
)

type KrakenTickerConverter struct{}

func (k *KrakenTickerConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var krakenTickerMsg constants.KrakenTickerMsg
	if err := json.Unmarshal(raw, &krakenTickerMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for kraken ticker message.", err)
	}

	return &constants.PipelineMessage{
		Exchange:   constants.Kraken,
		Channel:    constants.Ticker,
		Symbol:     krakenTickerMsg.Data[0].Symbol,
		EventType:  krakenTickerMsg.Type,
		RawMessage: &krakenTickerMsg,
	}, nil
}

type KrakenBookConverter struct{}

func (k *KrakenBookConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var krakenBookMsg constants.KrakenBookMsg
	if err := json.Unmarshal(raw, &krakenBookMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for kraken book message.", err)
	}

	msg := &constants.PipelineMessage{
		Exchange:   constants.Kraken,
		Channel:    constants.Book,
		EventType:  krakenBookMsg.Type,
		Symbol:     krakenBookMsg.Data[0].Symbol,
		RawMessage: &krakenBookMsg,
	}

	if krakenBookMsg.Type == constants.Update {
		parsedTime, err := time.Parse(time.RFC3339, krakenBookMsg.Data[0].Timestamp)
		if err != nil {
			return nil, logger.LogAndWrap("Converter error: Could not parse timestamp for kraken book update message", err)
		}

		msg.Ts = parsedTime.UnixNano()
	}

	return msg, nil
}
