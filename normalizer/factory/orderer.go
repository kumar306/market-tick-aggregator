package factory

import (
	"market-normalizer/constants"
	"market-normalizer/utils"
	"shared/logger"
	"strings"
	"sync"
)

// create a registry of ordererStrategy constructors
// to prevent single orderer registry pointer to be shared across multiple symbols of a stream
// this prevents corruption of the orderer/buffer state
type OrdererCtor func() constants.OrdererStrategy

var ordererCtorRegistry = make(map[string]OrdererCtor)
var onceOrderer sync.Once

func InitOrdererRegistry() {
	onceOrderer.Do(func() {
		pairs := []struct {
			exchange string
			channel  string
			ctor     func() constants.OrdererStrategy
		}{
			{constants.Binance, constants.AggTrade, func() constants.OrdererStrategy {
				return &BinanceAggTradeOrderer{}
			}},
			{constants.Binance, constants.Depth, func() constants.OrdererStrategy {
				return &BinanceDepthOrderer{}
			}},
			{constants.Coinbase, constants.Ticker, func() constants.OrdererStrategy {
				return &CoinbaseTickerOrderer{}
			}},
			{constants.Coinbase, constants.Level2, func() constants.OrdererStrategy {
				return &CoinbaseLevel2Orderer{}
			}},
			// {constants.Kraken, constants.Ticker},
			// {constants.Kraken, constants.Book},
		}
		for _, p := range pairs {
			if err := RegisterOrdererCtor(p.exchange, p.channel, p.ctor); err != nil {
				logger.Log.Error("Failed to register orderer, shutting down", "exchange", p.exchange, "channel", p.channel, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredOrderer(exchange string, channel string) (constants.OrdererStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := ordererCtorRegistry[key]; ok {
		return v(), nil
	}

	return nil, logger.LogAndWrap("Could not get registered orderer from map for key", nil, "key", key)
}

func RegisterOrdererCtor(exchange, channel string, ordererCtor OrdererCtor) error {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	ordererCtorRegistry[key] = ordererCtor
	logger.Log.Info("Registered orderer constructor for key", "key", key)
	return nil
}

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
