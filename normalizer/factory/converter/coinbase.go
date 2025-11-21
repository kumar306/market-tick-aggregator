package converter

import (
	"encoding/json"
	"market-normalizer/constants"
	"shared/logger"
	"time"
)

type CoinbaseTickerConverter struct{}

func (c *CoinbaseTickerConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var coinbaseTickerMsg constants.CoinbaseTickerMsg
	if err := json.Unmarshal(raw, &coinbaseTickerMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for coinbase ticker message.", err)
	}

	return &constants.PipelineMessage{
		Exchange:   constants.Coinbase,
		Channel:    constants.Ticker,
		Symbol:     coinbaseTickerMsg.ProductID,
		SeqId:      coinbaseTickerMsg.Sequence,
		RawMessage: &coinbaseTickerMsg,
	}, nil
}

type CoinbaseLevel2Converter struct{}

func (c *CoinbaseLevel2Converter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var coinbaseLevel2Msg constants.CoinbaseLevel2Msg
	if err := json.Unmarshal(raw, &coinbaseLevel2Msg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for coinbase level2 message.", err)
	}

	msg := &constants.PipelineMessage{
		Exchange:   constants.Coinbase,
		Channel:    constants.Level2,
		Symbol:     coinbaseLevel2Msg.ProductId,
		RawMessage: &coinbaseLevel2Msg,
	}

	if coinbaseLevel2Msg.Type == constants.L2Update {
		ts, err := time.Parse(time.RFC3339, coinbaseLevel2Msg.Time)
		if err != nil {
			return nil, logger.LogAndWrap("Error in parsing time from string to time",
				err, "stage", "converter",
				"exchange", msg.Exchange,
				"channel", msg.Channel,
				"symbol", msg.Symbol)
		}

		msg.Ts = ts.UnixNano()
	} else {
		msg.EventType = constants.Snapshot
	}

	return msg, nil
}
