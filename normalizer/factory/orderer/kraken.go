package orderer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"market-normalizer/constants"
	"market-normalizer/utils"
)

type KrakenTickerOrderer struct {
	SymbolState *constants.SymbolState
}

// noop
func (k *KrakenTickerOrderer) SetSymbolState(symbolState *constants.SymbolState) {}

// noop
func (k *KrakenTickerOrderer) InitOrdererState(msg *constants.PipelineMessage) {}

// no ordering here
func (k *KrakenTickerOrderer) Order(msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {
	return []*constants.PipelineMessage{msg}, nil
}

// noop
func (k *KrakenTickerOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return nil
}

// noop
func (k *KrakenTickerOrderer) Ack(msg *constants.PipelineMessage) {}

// noop
func (k *KrakenTickerOrderer) Cleanup() {}

// i am creating a dedupe key as exchange provides no info
func (k *KrakenTickerOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	raw := msg.RawMessage.(*constants.KrakenTickerMsg).Data[0]
	key := fmt.Sprintf(
		"%.8f|%.8f|%.8f|%.8f|%.8f|%.8f|%.8f|%.8f",
		raw.Bid,
		raw.Ask,
		raw.Last,
		raw.Volume,
		raw.VWAP,
		raw.Low,
		raw.High,
		raw.Change,
	)

	shaHash := sha256.Sum256([]byte(key))
	return hex.EncodeToString(shaHash[:])
}

type KrakenBookOrderer struct {
	SymbolState *constants.SymbolState
}

func (k *KrakenBookOrderer) SetSymbolState(symbolState *constants.SymbolState) {
	k.SymbolState = symbolState
}

func (k *KrakenBookOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	utils.InitTsOrdererState(k.SymbolState, msg)
}

// ts order here
func (k *KrakenBookOrderer) Order(msg *constants.PipelineMessage,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {
	// directly process the snapshot
	if msg.EventType == constants.Snapshot {
		return []*constants.PipelineMessage{msg}, nil
	}

	return utils.TsOrder(msg, k.SymbolState, bufferKey, workerChannel)
}

func (k *KrakenBookOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	return utils.PrepareTsBufferFlush(k.SymbolState)
}

func (k *KrakenBookOrderer) Ack(msg *constants.PipelineMessage) {
	if msg.EventType == constants.Snapshot {
		return
	}

	utils.TsAck(msg, k.SymbolState)
}

func (k *KrakenBookOrderer) Cleanup() {
	utils.TsCleanup(k.SymbolState)
}

func (k *KrakenBookOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return utils.GetTsOrderingId(msg)
}
