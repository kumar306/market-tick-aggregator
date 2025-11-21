package converter

import (
	"encoding/json"
	"market-normalizer/constants"
	"shared/logger"
)

type BinanceAggTradeConverter struct{}

func (b *BinanceAggTradeConverter) Convert(raw []byte) (*constants.PipelineMessage, error) {
	var aggTradeMsg constants.BinanceAggTradeMsg
	if err := json.Unmarshal(raw, &aggTradeMsg); err != nil {
		return nil, logger.LogAndWrap("Converter error: Could not deserialize for binance agg trade message.", err)
	}

	return &constants.PipelineMessage{
		Exchange:   constants.Binance,
		Channel:    constants.AggTrade,
		Symbol:     aggTradeMsg.Symbol,
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
		Exchange:   constants.Binance,
		Channel:    constants.Depth,
		Symbol:     depthUpdateMsg.Symbol,
		SeqId:      depthUpdateMsg.FinalUpdateID,
		RawMessage: &depthUpdateMsg,
	}, nil
}
