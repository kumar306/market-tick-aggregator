package utils

import (
	"market-normalizer/constants"
	"market-normalizer/resync"
	"shared/logger"
	"shared/metrics"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
)

// marks a freshly seen binance depth symbol as unsynced
// binance never sends a book snapshot over ws - every new symbol needs a rest api snapshot base before its deltas can be ingested
func InitBinanceDepthResyncState(symbolState *constants.SymbolState, msg *constants.PipelineMessage) {
	InitSequenceOrdererState(symbolState, msg)
	symbolState.NeedsSnapshot = true
	symbolState.SnapshotPending = false
}

// buffers depth updates and drives the REST-snapshot resync upon cold start or a drop marker > 0
// which shows adapter's ring buffer dropped a message.. so refetch book to ensure we didnt lose a single message due to slow adapter
// ws on its own is tcp so its lossless, but stalled adapter could skip some book deltas
// once synced, fallback to sequenceOrderer for out of order/replay handling
func BinanceDepthOrder(
	msg *constants.PipelineMessage,
	symbolState *constants.SymbolState,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord,
) ([]*constants.PipelineMessage, error) {

	dropped, hasDrop := hasDropMarker(msg.Record)

	if hasDrop && !symbolState.NeedsSnapshot {
		logger.Log.Warn("Detected adapter buffer drop on Binance depth stream. Triggering resync.",
			"symbol", msg.Symbol, "dropped", dropped)
		metrics.Normalizer_ResyncTriggeredTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
		symbolState.NeedsSnapshot = true
	}

	if symbolState.NeedsSnapshot {
		// never drop while unsynced - buffer every update until the splice completes
		if _, exists := symbolState.BufferSeqMap[msg.SeqId]; !exists {
			symbolState.BufferSeqId = append(symbolState.BufferSeqId, msg.SeqId)
		}
		symbolState.BufferSeqMap[msg.SeqId] = msg

		if !symbolState.SnapshotPending {
			symbolState.SnapshotPending = true
			go FetchBinanceSnapshotAsync(msg.Exchange, msg.Channel, msg.Symbol, bufferKey, workerChannel)
		}

		return []*constants.PipelineMessage{}, nil
	}

	return SequenceOrderer(msg, symbolState, bufferKey, workerChannel)
}

// goroutine which does the rest api call and doesnt mutate the existing symbol state - no data race
// post it back to same worker channel - single threaded event loop
func FetchBinanceSnapshotAsync(exchange, channel, symbol, bufferKey string, workerChannel chan *constants.DispatchRecord) {
	snapshot, err := resync.FetchBinanceDepthSnapshot(symbol)
	if err != nil {
		logger.Log.Error("Failed to fetch Binance depth snapshot, will retry on next message", "symbol", symbol, "err", err)
		workerChannel <- &constants.DispatchRecord{
			Event:     constants.SnapshotReady,
			BufferKey: bufferKey,
			Exchange:  exchange,
			Channel:   channel,
			Symbol:    symbol,
		}
		return
	}

	workerChannel <- &constants.DispatchRecord{
		Event:          constants.SnapshotReady,
		BufferKey:      bufferKey,
		Exchange:       exchange,
		Channel:        channel,
		Symbol:         symbol,
		SnapshotResult: snapshot,
	}
}

func hasDropMarker(rec *kgo.Record) (uint64, bool) {
	if rec == nil {
		return 0, false
	}
	for _, h := range rec.Headers {
		if h.Key == constants.DroppedCountHeader {
			n, err := strconv.ParseUint(string(h.Value), 10, 64)
			return n, err == nil && n > 0
		}
	}
	return 0, false
}
