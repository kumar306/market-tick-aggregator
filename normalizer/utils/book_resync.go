package utils

import (
	"market-normalizer/constants"
	"market-normalizer/resync"
	"shared/logger"
	"shared/metrics"
)

// handles coinbase/kraken book ordering with drop marker resync.
// snapshot arrives inline as the first message of a fresh subscription.
// request adapter to resubscribe, discard everything from the old session until the fresh snapshot arrives,
// then resume normal ts ordering
func BookOrderWithResync(
	msg *constants.PipelineMessage,
	symbolState *constants.SymbolState,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord,
) ([]*constants.PipelineMessage, error) {

	if msg.EventType == constants.Snapshot {
		symbolState.AwaitingSnapshot = false
		return []*constants.PipelineMessage{msg}, nil
	}

	if _, hasDrop := hasDropMarker(msg.Record); hasDrop && !symbolState.AwaitingSnapshot {
		symbolState.AwaitingSnapshot = true
		logger.Log.Warn("Detected adapter buffer drop, requesting resubscribe",
			"exchange", msg.Exchange, "channel", msg.Channel, "symbol", msg.Symbol)
		metrics.Normalizer_ResyncTriggeredTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
		if msg.Record != nil {
			resync.PublishResyncRequest(msg.Record.Topic, msg.Symbol)
		}
	}

	if symbolState.AwaitingSnapshot {
		metrics.Normalizer_ResyncDiscardedMessagesTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()
		return []*constants.PipelineMessage{}, nil
	}

	return TsOrder(msg, symbolState, bufferKey, workerChannel)
}
