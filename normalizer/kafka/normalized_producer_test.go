package kafka

import (
	"context"
	"market-normalizer/constants"
	"shared/ingestts"
	"shared/metrics"
	"shared/srcoffset"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

// captures what would be sent, completes the produce immediately
type captureProducer struct{ got *kgo.Record }

func (c *captureProducer) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	c.got = r
	promise(r, nil)
}

func TestProduceAsync_StampsSourceAndForwardsIngestTime(t *testing.T) {
	metrics.InitNormalizerMetrics()

	raw := &kgo.Record{
		Topic: "kraken.raw.ticks", Partition: 3, Offset: 77,
		Headers: []kgo.RecordHeader{ingestts.Header(1_700_000_000_123)},
	}
	p := &captureProducer{}
	ProduceAsync(p, "normalized.ticks", &constants.PipelineMessage{Exchange: "kraken", Record: raw}, []byte("k"), []byte("v"), func() {})

	topic, partition, offset, ok := srcoffset.Decode(headerValue(p.got, srcoffset.HeaderKey))
	require.True(t, ok)
	require.Equal(t, "kraken.raw.ticks", string(topic))
	require.Equal(t, int32(3), partition)
	require.Equal(t, int64(77), offset)

	ms, ok := ingestts.Find(p.got.Headers)
	require.True(t, ok, "the websocket-read time must travel with the output")
	require.Equal(t, int64(1_700_000_000_123), ms)
}

func TestProduceAsync_NoIngestHeaderUpstream_NoneInvented(t *testing.T) {
	metrics.InitNormalizerMetrics()

	p := &captureProducer{}
	ProduceAsync(p, "normalized.ticks", &constants.PipelineMessage{Exchange: "kraken", Record: &kgo.Record{Topic: "t"}}, nil, nil, func() {})

	_, ok := ingestts.Find(p.got.Headers)
	require.False(t, ok, "a record from a producer that does not stamp ingest time must not get a made-up one")
}

func headerValue(r *kgo.Record, key string) []byte {
	for _, h := range r.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}
