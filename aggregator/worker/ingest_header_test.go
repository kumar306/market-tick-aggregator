package worker_test

import (
	"context"
	"market-aggregator/constants"
	"market-aggregator/internal"
	"market-aggregator/kafka"
	"market-aggregator/proto/generated"
	"market-aggregator/worker"
	"shared/ingestts"
	"shared/metrics"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
)

type capturingClient struct{ sent []*kgo.Record }

func (c *capturingClient) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	c.sent = append(c.sent, r)
	promise(r, nil)
}

func tickWithIngest(t *testing.T, ingestMs int64) *kgo.Record {
	val, err := proto.Marshal(&generated.NormalizedTick{Exchange: "kraken", Channel: "ticker", Symbol: "ETH-USD", Price: 100, Volume: 1})
	require.NoError(t, err)
	rec := &kgo.Record{Key: []byte(symbolKey), Topic: "normalized.ticks", Value: val}
	if ingestMs > 0 {
		rec.Headers = []kgo.RecordHeader{ingestts.Header(ingestMs)}
	}
	return rec
}

// The aggregate reports the ingest time of the NEWEST tick it contains, and only ticks that arrived since the
// window's last flush. Otherwise a symbol that went quiet would show an age that grows forever.
func TestFlush_EmitsNewestIngestTimeOfTicksSinceLastFlush(t *testing.T) {
	metrics.InitAggregatorMetrics()
	internal.InitMetricRegistry()
	kafka.DownstreamTopic = "aggregated.ticks"

	w := worker.NewWorker(0, make(chan *constants.DispatchRecord, 10), oneWindow, nil)
	flush := &constants.DispatchRecord{Event: constants.FlushEvent, WindowConfig: oneWindow[0], FlushTsMs: 1}
	out := &capturingClient{}

	// two ticks, the second is newer (and arrives second); a third arrives out of order but is older than the newest
	w.ProcessTick(tickWithIngest(t, 1_000))
	w.ProcessTick(tickWithIngest(t, 3_000))
	w.ProcessTick(tickWithIngest(t, 2_000))

	w.FlushWindow(flush, out)
	require.Len(t, out.sent, 1)
	ms, ok := ingestts.Find(out.sent[0].Headers)
	require.True(t, ok)
	require.Equal(t, int64(3_000), ms, "the newest ingest time, not the first or the last to arrive")

	// nothing new since: the next flush must not report a stale age
	w.FlushWindow(flush, out)
	require.Len(t, out.sent, 2)
	_, ok = ingestts.Find(out.sent[1].Headers)
	require.False(t, ok, "no new tick since the last flush, so no ingest-to-insert sample")

	// a tick without the header (older producer, load generator) cannot be measured and must not break anything
	w.ProcessTick(tickWithIngest(t, 0))
	w.FlushWindow(flush, out)
	_, ok = ingestts.Find(out.sent[2].Headers)
	require.False(t, ok)
}
