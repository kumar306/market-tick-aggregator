package worker

import (
	"market-normalizer/constants"
	"market-normalizer/kafka"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

func src(offset int64) *constants.PipelineMessage {
	return &constants.PipelineMessage{Record: &kgo.Record{Topic: "kraken.raw.book", Partition: 2, Offset: offset}}
}

// a record parked in an orderer has been consumed but has no output yet. If the offset commit passed it,
// a crash inside the buffering window would lose it. bufferedSources is what stops the commit in front of it.
func TestBufferedSources_FindsTheOldestParkedRecordInEveryBufferKind(t *testing.T) {
	w := NewWorker(0, nil, nil)
	w.WorkerMap["kraken:book:eth"] = &constants.SymbolState{
		// timestamp orderer: several messages can share a timestamp
		BufferTimeMap: map[int64][]*constants.PipelineMessage{
			1000: {src(57), src(60)},
			1001: {src(55)},
		},
	}
	w.WorkerMap["binance:depth:btc"] = &constants.SymbolState{
		// sequence orderer
		BufferSeqMap: map[int64]*constants.PipelineMessage{
			9: src(70),
			8: src(41),
		},
	}

	got := w.bufferedSources()
	require.Equal(t, int64(41), got[kafka.TP{Topic: "kraken.raw.book", Partition: 2}])
}

func TestBufferedSources_EmptyWhenNothingIsParked(t *testing.T) {
	w := NewWorker(0, nil, nil)
	w.WorkerMap["kraken:ticker:eth"] = &constants.SymbolState{}
	require.Empty(t, w.bufferedSources())
}

func TestBufferedSources_IgnoresMessagesWithoutASourceRecord(t *testing.T) {
	w := NewWorker(0, nil, nil)
	w.WorkerMap["x"] = &constants.SymbolState{
		BufferSeqMap: map[int64]*constants.PipelineMessage{1: {}},
	}
	require.Empty(t, w.bufferedSources())
}
