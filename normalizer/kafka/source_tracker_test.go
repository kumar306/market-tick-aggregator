package kafka

import (
	"testing"

	"shared/srcoffset"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kgo"
)

var raw0 = TP{Topic: "kraken.raw.ticks", Partition: 0}

func polled(off int64) map[string]map[int32]kgo.EpochOffset {
	return map[string]map[int32]kgo.EpochOffset{raw0.Topic: {raw0.Partition: {Epoch: 3, Offset: off}}}
}

func TestFrontier_NothingPending_CommitsPolledPosition(t *testing.T) {
	f := Frontier(polled(100), nil, nil, nil)
	require.Equal(t, int64(100), f[raw0.Topic][raw0.Partition].Offset)
}

func TestFrontier_UnackedProduceHoldsCommitBack(t *testing.T) {
	// offsets 90..99 polled; the output of offset 94 is still unacked, so a crash must replay from 94
	f := Frontier(polled(100), map[TP]int64{raw0: 94}, nil, nil)
	require.Equal(t, int64(94), f[raw0.Topic][raw0.Partition].Offset)
}

func TestFrontier_RecordParkedInOrdererHoldsCommitBack(t *testing.T) {
	// offset 91 is buffered in a TsOrder/sequence buffer: consumed, but no output exists yet
	f := Frontier(polled(100), map[TP]int64{raw0: 97}, map[TP]int64{raw0: 91}, nil)
	require.Equal(t, int64(91), f[raw0.Topic][raw0.Partition].Offset)
}

func TestFrontier_UsesTheLowestOfAllThree(t *testing.T) {
	f := Frontier(polled(100), map[TP]int64{raw0: 80}, map[TP]int64{raw0: 91}, nil)
	require.Equal(t, int64(80), f[raw0.Topic][raw0.Partition].Offset)
}

func TestFrontier_SkipsPartitionsThatDidNotAdvance(t *testing.T) {
	f := Frontier(polled(100), nil, nil, map[TP]int64{raw0: 100})
	require.Empty(t, f, "an unchanged frontier must not cost a commit round trip")

	f = Frontier(polled(100), map[TP]int64{raw0: 94}, nil, map[TP]int64{raw0: 94})
	require.Empty(t, f)
}

func TestFrontier_KeepsTheEpochOfThePolledPosition(t *testing.T) {
	f := Frontier(polled(100), map[TP]int64{raw0: 94}, nil, nil)
	require.Equal(t, int32(3), f[raw0.Topic][raw0.Partition].Epoch)
}

func TestSourceTracker_MinInflightAndFailure(t *testing.T) {
	tr := NewSourceTracker()
	tr.add(raw0, 12)
	tr.add(raw0, 10)
	tr.add(raw0, 11)
	require.Equal(t, int64(10), tr.MinInflight()[raw0])

	tr.done(raw0, 10)
	require.Equal(t, int64(11), tr.MinInflight()[raw0])

	// a failed produce is never removed, so the frontier cannot pass it
	tr.fail(raw0, 11)
	tr.done(raw0, 12)
	require.Equal(t, int64(11), tr.MinInflight()[raw0])
}

func TestSourceHeader_RoundTrip(t *testing.T) {
	topic, partition, off, ok := srcoffset.Decode(srcoffset.Encode("coinbase.raw.level2", 5, 1234567890123))
	require.True(t, ok)
	require.Equal(t, "coinbase.raw.level2", string(topic))
	require.Equal(t, int32(5), partition)
	require.Equal(t, int64(1234567890123), off)

	_, _, _, ok = srcoffset.Decode([]byte{1, 2, 3})
	require.False(t, ok)
}
