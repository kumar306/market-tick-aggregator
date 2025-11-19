package utils

import (
	"market-normalizer/constants"
	"shared/logger"
	"shared/metrics"
	"sort"
	"strconv"
	"time"
)

func InitSequenceOrdererState(symbolState *constants.SymbolState, msg *constants.PipelineMessage) {
	// pre allocate cap
	symbolState.BufferSeqMap = make(map[int64]*constants.PipelineMessage, 128)
	symbolState.BufferSeqId = make([]int64, 0, 100)
	symbolState.Gap = nil
	symbolState.GapActive = false
	symbolState.LastSeqId = int64(msg.SeqId) - 1
}

func SequenceOrderer(
	msg *constants.PipelineMessage,
	symbolState *constants.SymbolState,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {

	if symbolState.GapActive {
		// if same message resent, we will have duplicate in seq id means duplicate publish so check before appending
		if _, exists := symbolState.BufferSeqMap[msg.SeqId]; !exists {
			symbolState.BufferSeqId = append(symbolState.BufferSeqId, msg.SeqId)
		}
		symbolState.BufferSeqMap[msg.SeqId] = msg
		return []*constants.PipelineMessage{}, nil
	}

	// if worker buffer not empty and it resumed after crash/shutdown
	// send a flush event after enqueuing the current message
	if len(symbolState.BufferSeqMap) > 0 {
		symbolState.BufferSeqMap[msg.SeqId] = msg
		symbolState.BufferSeqId = append(symbolState.BufferSeqId, msg.SeqId)
		workerChannel <- &constants.DispatchRecord{
			Event:     constants.FlushBuffer,
			BufferKey: bufferKey,
		}
		return []*constants.PipelineMessage{}, nil
	}

	// get the last seqId -> msg seq id should be that + 1
	if msg.SeqId > symbolState.LastSeqId+1 {
		// dropped message. start timer
		logger.Log.Warn("Detected a message drop")
		symbolState.GapActive = true
		if _, exists := symbolState.BufferSeqMap[msg.SeqId]; !exists {
			symbolState.BufferSeqId = append(symbolState.BufferSeqId, msg.SeqId)
		}
		symbolState.BufferSeqMap[msg.SeqId] = msg
		symbolState.Gap = time.NewTimer(10 * time.Second)

		// send a timer event to worker channel to flush the buffer
		go func(t *time.Timer) {
			<-t.C
			workerChannel <- &constants.DispatchRecord{
				Event:     constants.FlushBuffer,
				BufferKey: bufferKey,
			}
		}(symbolState.Gap)

		metrics.Normalizer_DroppedTimerTotal.WithLabelValues(msg.Exchange, msg.Channel, msg.Symbol).Inc()

		return []*constants.PipelineMessage{}, nil

	} else {
		// can be <= last processed seq id + 1: just apply it.
		return []*constants.PipelineMessage{msg}, nil
	}
}

func SequenceSortBufferFlush(symbolState *constants.SymbolState) []*constants.PipelineMessage {
	var preparedBuffer []*constants.PipelineMessage
	sort.Slice(symbolState.BufferSeqId, func(i, j int) bool {
		return symbolState.BufferSeqId[i] < symbolState.BufferSeqId[j]
	})

	for _, seqId := range symbolState.BufferSeqId {
		if entry, exists := symbolState.BufferSeqMap[seqId]; exists {
			preparedBuffer = append(preparedBuffer, entry)
		}
	}

	return preparedBuffer
}

func SequenceAck(symbolState *constants.SymbolState, msg *constants.PipelineMessage) {
	symbolState.LastSeqId = msg.SeqId
	delete(symbolState.BufferSeqMap, msg.SeqId)
}

func SequenceOrdererCleanup(symbolState *constants.SymbolState) {
	symbolState.BufferSeqId = symbolState.BufferSeqId[:0]
	// pre allocate cap
	symbolState.BufferSeqMap = make(map[int64]*constants.PipelineMessage, 128)
	if symbolState.Gap != nil {
		if !symbolState.Gap.Stop() {
			// drain timer channel if needed
			select {
			case <-symbolState.Gap.C:
			default:
			}
		}
		symbolState.Gap = nil
	}
	symbolState.GapActive = false
}

func GetSequenceOrderingId(msg *constants.PipelineMessage) string {
	return strconv.FormatInt(msg.SeqId, 10)
}
