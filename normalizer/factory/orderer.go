package factory

import (
	"market-normalizer/constants"
	"shared/logger"
	"strings"
	"sync"
	"time"
)

var ordererRegistry = make(map[string]constants.OrdererStrategy)
var onceOrderer sync.Once

func InitOrdererRegistry() {
	onceOrderer.Do(func() {
		pairs := []struct {
			exchange string
			channel  string
		}{
			{constants.Binance, constants.AggTrade},
			{constants.Binance, constants.Depth},
			{constants.Coinbase, constants.Ticker},
			{constants.Coinbase, constants.Level2},
			{constants.Kraken, constants.Ticker},
			{constants.Kraken, constants.Book},
		}
		for _, p := range pairs {
			if err := RegisterOrderer(p.exchange, p.channel); err != nil {
				logger.Log.Error("Failed to register orderer, shutting down", "exchange", p.exchange, "channel", p.channel, "error", err)
				panic(err)
			}
		}
	})
}

func GetRegisteredOrderer(exchange string, channel string) (constants.OrdererStrategy, error) {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	if v, ok := ordererRegistry[key]; ok {
		return v, nil
	}

	return nil, logger.LogAndWrap("Could not get registered orderer from map for key", nil, "key", key)
}

func RegisterOrderer(exchange string, channel string) error {
	key := strings.ToLower(exchange) + ":" + strings.ToLower(channel)
	orderer, err := GetOrderer(key)
	if err != nil {
		return logger.LogAndWrap("Could not register orderer", nil, "error", err)
	}
	ordererRegistry[key] = orderer
	logger.Log.Info("Registered orderer for key", "key", key)
	return nil
}

func GetOrderer(key string) (constants.OrdererStrategy, error) {
	switch key {
	case "binance:aggtrade":
		return &{}, nil
	case "binance:depth":
		return &BinanceDepthConverter{}, nil
	case "coinbase:ticker":
		return &CoinbaseTickerConverter{}, nil
	case "coinbase:l2":
		return &CoinbaseDepthConverter{}, nil
	case "kraken:ticker":
		return &KrakenTickerConverter{}, nil
	case "kraken:book":
		return &KrakenBookConverter{}, nil
	default:
		return nil, logger.LogAndWrap("Could not find an orderer for key", nil, "key", key)
	}
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

func (b *BinanceAggTradeOrderer) Order(
	msg *constants.PipelineMessage, 
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {

	if b.SymbolState.GapActive {
		b.SymbolState.Buffer = append(b.SymbolState.Buffer, msg)
		return []*constants.PipelineMessage{}, nil 
	}

	// get the last seqId -> msg seq id should be that + 1
	if msg.SeqId > b.SymbolState.LastSeqId + 1 {
		// dropped message. start timer
		logger.Log.Warn("Detected a message drop")
		b.SymbolState.GapActive = true
		b.SymbolState.Buffer = append(b.SymbolState.Buffer, msg)
		b.SymbolState.Gap = *time.NewTimer(10 * time.Second)

		// send a timer event to worker channel to flush the buffer
		go func() {
			<-b.SymbolState.Gap.C
			workerChannel <- &constants.DispatchRecord{
				Event: constants.FlushBuffer,
				BufferKey: bufferKey,
			}
		}()

	} else {
		// can be <= last processed seq id + 1: just apply it.
		return []*constants.PipelineMessage{msg}, nil
	}
}

func (b *BinanceAggTradeOrderer) InitOrdererState(msg *constants.PipelineMessage) {
	b.SymbolState.LastSeqId = uint64(msg.SeqId) - 1
}

func (b *BinanceAggTradeOrderer) Less(i, j *constants.PipelineMessage) bool {
	return i.SeqId < j.SeqId
}

// binance depth orderer: similar seq. the depth orderer and agg trade orderer can share the same code
// utils.SequenceOrderer and pass in the type, field