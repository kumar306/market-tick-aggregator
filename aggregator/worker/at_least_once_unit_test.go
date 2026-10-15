package worker_test

import (
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/internal/aggmetrics"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"shared/metrics"
	"shared/srcoffset"
	"testing"

	"bytes"
	"encoding/gob"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

const symbolKey = "kraken:ticker:ETH-USD"

var oneWindow = []*constants.WindowConfig{{Id: "5s", DurationMs: 5000, FlushCadencyMs: 1000, BucketSizeMs: 1000}}

func newALOWorker(t *testing.T, ckpts map[string]*kafka.SymbolCheckpoint) *worker.Worker {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()
	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), oneWindow, nil)
	w.EnableAtLeastOnce(ckpts)
	return w
}

// a tick of volume 1, stamped as the normalizer stamps it with the raw record it came from
func tickFrom(t *testing.T, rawPartition int32, rawOffset int64) *kgo.Record {
	val, err := proto.Marshal(&generated.NormalizedTick{Exchange: "kraken", Channel: "ticker", Symbol: "ETH-USD", Price: 100, Volume: 1})
	require.NoError(t, err)
	return &kgo.Record{
		Key: []byte(symbolKey), Topic: "normalized.ticks", Value: val,
		Headers: []kgo.RecordHeader{{Key: srcoffset.HeaderKey, Value: srcoffset.Encode("kraken.raw.ticks", rawPartition, rawOffset)}},
	}
}

func volume(w *worker.Worker) float64 {
	for _, m := range w.SymbolState[symbolKey].Windows["5s"].Metrics {
		if v, ok := m.(*aggmetrics.Volume); ok {
			return v.GetValue()
		}
	}
	return -1
}

func TestAtLeastOnce_ReplayedTickIsAppliedOnce(t *testing.T) {
	w := newALOWorker(t, nil)

	for off := int64(10); off < 15; off++ {
		w.ProcessTick(tickFrom(t, 2, off))
	}
	require.Equal(t, 5.0, volume(w))

	// the normalizer crashed and re-sent 12..14
	for off := int64(12); off < 15; off++ {
		w.ProcessTick(tickFrom(t, 2, off))
	}
	require.Equal(t, 5.0, volume(w), "a replayed copy must not be counted twice")

	w.ProcessTick(tickFrom(t, 2, 15))
	require.Equal(t, 6.0, volume(w))
}

func TestAtLeastOnce_DifferentSourcePartitionsDoNotCollide(t *testing.T) {
	w := newALOWorker(t, nil)
	w.ProcessTick(tickFrom(t, 0, 7))
	w.ProcessTick(tickFrom(t, 1, 7)) // same offset, different partition: a different record
	require.Equal(t, 2.0, volume(w))
}

func TestAtLeastOnce_TickWithoutSourceHeaderIsAlwaysApplied(t *testing.T) {
	w := newALOWorker(t, nil)
	rec := tickFrom(t, 0, 1)
	rec.Headers = nil
	w.ProcessTick(rec)
	w.ProcessTick(rec)
	require.Equal(t, 2.0, volume(w), "nothing to dedupe on, so nothing is dropped")
}

// the crash path: state and seen-offsets come back from the checkpoint as one unit
func TestAtLeastOnce_RestoredFromCheckpoint_KeepsStateAndDropsWhatItAlreadyHolds(t *testing.T) {
	before := newALOWorker(t, nil)
	for off := int64(100); off < 110; off++ {
		before.ProcessTick(tickFrom(t, 0, off))
	}
	require.Equal(t, 10.0, volume(before))

	// what checkpointDirty writes for the symbol
	st := before.SymbolState[symbolKey]
	ckpt := kafka.SymbolCheckpoint{Windows: map[string]map[constants.MetricName]constants.Metric{}, Seen: st.Seen}
	for id, win := range st.Windows {
		ckpt.Windows[id] = win.Metrics
	}
	raw, err := kafka.EncodeSymbolCheckpoint(ckpt)
	require.NoError(t, err)
	var restored kafka.SymbolCheckpoint
	require.NoError(t, gob.NewDecoder(bytes.NewReader(raw)).Decode(&restored))

	// "restart": a fresh worker that has only the checkpoint
	after := newALOWorker(t, map[string]*kafka.SymbolCheckpoint{symbolKey: &restored})

	// Kafka redelivers from the committed offset: 105..109 were already in the checkpoint, 110..112 were not
	for off := int64(105); off < 113; off++ {
		after.ProcessTick(tickFrom(t, 0, off))
	}
	require.Equal(t, 13.0, volume(after), "10 restored + 3 new; the 5 already inside the checkpoint are dropped, not re-added")
}

func TestSymbolCheckpoint_GobRoundTripKeepsMetricsAndSeen(t *testing.T) {
	seen := srcoffset.SeenSet{}
	seen.Check(srcoffset.SrcKey{Topic: "kraken.raw.ticks", Partition: 4}, 900)

	in := kafka.SymbolCheckpoint{
		Windows: map[string]map[constants.MetricName]constants.Metric{
			"5s": {"volume": &aggmetrics.Volume{Volume: 42}, "ohlc": &aggmetrics.OHLC{Open: 1, High: 3, Low: 1, Close: 2, OpenSet: true}},
		},
		Seen: seen,
	}
	raw, err := kafka.EncodeSymbolCheckpoint(in)
	require.NoError(t, err)

	var out kafka.SymbolCheckpoint
	require.NoError(t, gob.NewDecoder(bytes.NewReader(raw)).Decode(&out))
	require.Equal(t, in.Windows["5s"]["volume"], out.Windows["5s"]["volume"])
	require.Equal(t, in.Windows["5s"]["ohlc"], out.Windows["5s"]["ohlc"])
	require.True(t, srcoffset.SeenSet(out.Seen).Check(srcoffset.SrcKey{Topic: "kraken.raw.ticks", Partition: 4}, 900))
}
