package orderer

import (
	"market-normalizer/constants"
	"market-normalizer/utils"
)

// create the orderers
type BinanceAggTradeOrderer struct {
	SymbolState *constants.SymbolState
}

// lets say last seq id: x
// got x + 1 - all good
// got x + 2 instead of x + 1: put it in the buffer. start the timer and return.

// now timer is active, received another record. is it x + 1 ? No.
// put it in the buffer. timer is still active. return

// if timer is active, received x+1 ? then stop timer and flush it right now in the order.

// timer hits 10th second -> timer.C fires. -> its time to process buffer
// should this happens parallelly with the new message ingest
// when the buffer decides to flush, wait until new record is added into buffer. and block on new message processing.
// i can reinsert flush event into the worker channel
// apply buffer updates to pipeline, update the last seq id with each and mark record for commit

func (b *BinanceAggTradeOrderer) SetSymbolState(symbolState *constants.SymbolState) {
	b.SymbolState = symbolState
}

func (b *BinanceAggTradeOrderer) Order(
	msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {

	return utils.SequenceOrderer(msg, b.SymbolState, bufferKey, workerChannel)
}

func (b *BinanceAggTradeOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	utils.InitSequenceOrdererState(b.SymbolState, msg)
}

// sort in seq ids and create a new view of the buffer
func (b *BinanceAggTradeOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return utils.SequenceSortBufferFlush(b.SymbolState)
}

// current seq number of the message - update it to msg and remove from buffer
// ack ensures safe crash
func (b *BinanceAggTradeOrderer) Ack(msg *constants.PipelineMessage) {
	utils.SequenceAck(b.SymbolState, msg)
}

// cleanup buffer after flush
func (b *BinanceAggTradeOrderer) Cleanup() {
	utils.SequenceOrdererCleanup(b.SymbolState)
}

func (b *BinanceAggTradeOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return utils.GetSequenceOrderingId(msg)
}

type BinanceDepthOrderer struct {
	SymbolState *constants.SymbolState
}

func (b *BinanceDepthOrderer) SetSymbolState(symbolState *constants.SymbolState) {
	b.SymbolState = symbolState
}

func (b *BinanceDepthOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	utils.InitSequenceOrdererState(b.SymbolState, msg)
}

func (b *BinanceDepthOrderer) Order(msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {
	return utils.SequenceOrderer(msg, b.SymbolState, bufferKey, workerChannel)
}

func (b *BinanceDepthOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return utils.SequenceSortBufferFlush(b.SymbolState)
}

func (b *BinanceDepthOrderer) Ack(msg *constants.PipelineMessage) {
	utils.SequenceAck(b.SymbolState, msg)
}

func (b *BinanceDepthOrderer) Cleanup() {
	utils.SequenceOrdererCleanup(b.SymbolState)
}

func (b *BinanceDepthOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return utils.GetSequenceOrderingId(msg)
}
