package orderer

import (
	"market-normalizer/constants"
	"market-normalizer/utils"
)

type CoinbaseTickerOrderer struct {
	SymbolState *constants.SymbolState
}

func (c *CoinbaseTickerOrderer) SetSymbolState(symbolState *constants.SymbolState) {
	c.SymbolState = symbolState
}

func (c *CoinbaseTickerOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	utils.InitSequenceOrdererState(c.SymbolState, msg)
}

func (c *CoinbaseTickerOrderer) Order(msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {
	return utils.SequenceOrderer(msg, c.SymbolState, bufferKey, workerChannel)
}

func (c *CoinbaseTickerOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return utils.SequenceSortBufferFlush(c.SymbolState)
}

func (c *CoinbaseTickerOrderer) Ack(msg *constants.PipelineMessage) {
	utils.SequenceAck(c.SymbolState, msg)
}

func (c *CoinbaseTickerOrderer) Cleanup() {
	utils.SequenceOrdererCleanup(c.SymbolState)
}

func (c *CoinbaseTickerOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return utils.GetSequenceOrderingId(msg)
}

type CoinbaseLevel2Orderer struct {
	SymbolState *constants.SymbolState
}

func (c *CoinbaseLevel2Orderer) SetSymbolState(symbolState *constants.SymbolState) {
	c.SymbolState = symbolState
}

func (c *CoinbaseLevel2Orderer) InitOrdererState(msg *constants.PipelineMessage) {
	utils.InitTsOrdererState(c.SymbolState, msg)
}

func (c *CoinbaseLevel2Orderer) Order(msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {
	if msg.EventType == constants.Snapshot {
		return []*constants.PipelineMessage{msg}, nil
	}

	return utils.TsOrder(msg, c.SymbolState, bufferKey, workerChannel)
}

func (c *CoinbaseLevel2Orderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return utils.PrepareTsBufferFlush(c.SymbolState)
}

func (c *CoinbaseLevel2Orderer) Ack(msg *constants.PipelineMessage) {
	if msg.EventType == constants.Snapshot {
		return
	}

	utils.TsAck(msg, c.SymbolState)
}

func (c *CoinbaseLevel2Orderer) Cleanup() {
	utils.TsCleanup(c.SymbolState)
}

func (c *CoinbaseLevel2Orderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return utils.GetTsOrderingId(msg)
}
