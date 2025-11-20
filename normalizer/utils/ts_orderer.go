package utils

import (
	"market-normalizer/constants"
	"slices"
	"sort"
	"strconv"
	"time"
)

func InitTsOrdererState(symbolState *constants.SymbolState, msg *constants.PipelineMessage) {
	symbolState.BufferTime = make([]int64, 0, 128)
	symbolState.BufferTimeMap = make(map[int64][]*constants.PipelineMessage)
	symbolState.Gap = nil
	symbolState.GapActive = false
}

// persist to buffer. sort in asc time and send flush buffer event in intervals
// we cannot detect dropped messages in a stream which doesnt send sequences.
// it is possible messages in asc time differing by milliseconds can arrive in a different order due to jitter in websocket stream
func TsOrder(msg *constants.PipelineMessage,
	symbolState *constants.SymbolState,
	bufferKey string,
	workerChannel chan *constants.DispatchRecord) ([]*constants.PipelineMessage, error) {

	if !symbolState.GapActive {
		symbolState.Gap = time.NewTimer(20 * time.Second)
		symbolState.GapActive = true

		go func(t *time.Timer) {
			<-t.C
			workerChannel <- &constants.DispatchRecord{
				Event:     constants.FlushBuffer,
				BufferKey: bufferKey,
			}
		}(symbolState.Gap)
	}

	// append it only once to buffer time slice else when preparing buffer flush, we will get duplicates
	if _, exists := symbolState.BufferTimeMap[msg.Ts]; !exists {
		symbolState.BufferTime = append(symbolState.BufferTime, msg.Ts)
	}

	tsMsgs := symbolState.BufferTimeMap[msg.Ts]
	tsMsgs = append(tsMsgs, msg)
	symbolState.BufferTimeMap[msg.Ts] = tsMsgs

	return []*constants.PipelineMessage{}, nil
}

func PrepareTsBufferFlush(symbolState *constants.SymbolState) []*constants.PipelineMessage {
	var preparedBuffer []*constants.PipelineMessage
	sort.Slice(symbolState.BufferTime, func(i, j int) bool {
		return symbolState.BufferTime[i] < symbolState.BufferTime[j]
	})

	for _, time := range symbolState.BufferTime {
		preparedBuffer = append(preparedBuffer, symbolState.BufferTimeMap[time]...)
	}

	return preparedBuffer
}

func TsAck(msg *constants.PipelineMessage, symbolState *constants.SymbolState) {
	symbolState.LastSeenTs = msg.Ts

	// remove the first entry of map. if len = 0, then delete it
	symbolState.BufferTimeMap[msg.Ts] = slices.Delete(symbolState.BufferTimeMap[msg.Ts], 0, 1)
	if len(symbolState.BufferTimeMap[msg.Ts]) == 0 {
		delete(symbolState.BufferTimeMap, msg.Ts)
	}
}

func TsCleanup(symbolState *constants.SymbolState) {
	symbolState.GapActive = false
	symbolState.BufferTime = symbolState.BufferTime[:0]
	symbolState.BufferTimeMap = make(map[int64][]*constants.PipelineMessage, 128)

	if symbolState.Gap != nil {
		// stop is guaranteed to stop the timer so its fine not to drain the channel
		symbolState.Gap.Stop()
		symbolState.Gap = nil
	}
}

func GetTsOrderingId(msg *constants.PipelineMessage) string {
	return strconv.FormatInt(msg.Ts, 10)
}
