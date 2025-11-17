package factory

import (
	"market-normalizer/constants"
	"shared/logger"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
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
			// {constants.Binance, constants.Depth},
			// {constants.Coinbase, constants.Ticker},
			// {constants.Coinbase, constants.Level2},
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

	if b.SymbolState.GapActive {
		b.SymbolState.BufferSeqMap[msg.SeqId] = msg
		b.SymbolState.BufferSeqId = append(b.SymbolState.BufferSeqId, msg.SeqId)
		return []*constants.PipelineMessage{}, nil
	}

	// if worker buffer not empty and it resumed after crash/shutdown
	// send a flush event after enqueuing the current message
	if len(b.SymbolState.BufferSeqMap) > 0 {
		b.SymbolState.BufferSeqMap[msg.SeqId] = msg
		b.SymbolState.BufferSeqId = append(b.SymbolState.BufferSeqId, msg.SeqId)
		workerChannel <- &constants.DispatchRecord{
			Event:     constants.FlushBuffer,
			BufferKey: bufferKey,
		}
		return []*constants.PipelineMessage{}, nil
	}

	// get the last seqId -> msg seq id should be that + 1
	if msg.SeqId > b.SymbolState.LastSeqId+1 {
		// dropped message. start timer
		logger.Log.Warn("Detected a message drop")
		b.SymbolState.GapActive = true
		b.SymbolState.BufferSeqMap[msg.SeqId] = msg
		b.SymbolState.BufferSeqId = append(b.SymbolState.BufferSeqId, msg.SeqId)
		b.SymbolState.Gap = time.NewTimer(10 * time.Second)

		// send a timer event to worker channel to flush the buffer
		go func(t *time.Timer) {
			<-t.C
			workerChannel <- &constants.DispatchRecord{
				Event:     constants.FlushBuffer,
				BufferKey: bufferKey,
			}
		}(b.SymbolState.Gap)

		return []*constants.PipelineMessage{}, nil

	} else {
		// can be <= last processed seq id + 1: just apply it.
		return []*constants.PipelineMessage{msg}, nil
	}
}

func (b *BinanceAggTradeOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	// pre allocate cap
	b.SymbolState.BufferSeqMap = make(map[int64]*constants.PipelineMessage, 128)
	b.SymbolState.BufferSeqId = make([]int64, 0, 100)
	b.SymbolState.Gap = nil
	b.SymbolState.GapActive = false
	b.SymbolState.LastSeqId = int64(msg.SeqId) - 1
}

// sort in seq ids and create a new view of the buffer
func (b *BinanceAggTradeOrderer) PrepareBufferFlush() []*constants.PipelineMessage {
	var preparedBuffer []*constants.PipelineMessage
	sort.Slice(b.SymbolState.BufferSeqId, func(i, j int) bool {
		return b.SymbolState.BufferSeqId[i] < b.SymbolState.BufferSeqId[j]
	})

	for _, seqId := range b.SymbolState.BufferSeqId {
		if entry, exists := b.SymbolState.BufferSeqMap[seqId]; exists {
			preparedBuffer = append(preparedBuffer, entry)
		}
	}

	return preparedBuffer
}

// current seq number of the message - update it to msg and remove from buffer
// ack ensures safe crash
func (b *BinanceAggTradeOrderer) Ack(msg *constants.PipelineMessage) {
	b.SymbolState.LastSeqId = msg.SeqId
	delete(b.SymbolState.BufferSeqMap, msg.SeqId)
}

// cleanup buffer after flush
func (b *BinanceAggTradeOrderer) Cleanup() {
	b.SymbolState.BufferSeqId = b.SymbolState.BufferSeqId[:0]
	// pre allocate cap
	b.SymbolState.BufferSeqMap = make(map[int64]*constants.PipelineMessage, 128)
	if b.SymbolState.Gap != nil {
		if !b.SymbolState.Gap.Stop() {
			// drain timer channel if needed
			select {
			case <-b.SymbolState.Gap.C:
			default:
			}
		}
		b.SymbolState.Gap = nil
	}
	b.SymbolState.GapActive = false
}

func (b *BinanceAggTradeOrderer) GetOrderingId(msg *constants.PipelineMessage) string {
	return strconv.FormatInt(msg.SeqId, 10)
}

// binance depth orderer: similar seq. the depth orderer and agg trade orderer can share the same code
// utils.SequenceOrderer and pass in the type, field
