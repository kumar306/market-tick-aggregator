package srcoffset

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSeen_DropsAReplayedCopy(t *testing.T) {
	s := &Seen{}
	for off := int64(100); off < 110; off++ {
		require.False(t, s.Check(off), "first sight of %d", off)
	}
	// the producer crashed and re-sent 105..109
	for off := int64(105); off < 110; off++ {
		require.True(t, s.Check(off), "replayed %d must be dropped", off)
	}
	require.False(t, s.Check(110), "the stream carries on past the replay")
}

func TestSeen_AcceptsALateRecordThatWasNeverApplied(t *testing.T) {
	// an orderer can emit offset 103 after 108: it is out of order, not a duplicate
	s := &Seen{}
	for _, off := range []int64{100, 101, 102, 104, 105, 106, 107, 108} {
		require.False(t, s.Check(off))
	}
	require.False(t, s.Check(103), "a hole in the window is a record we have not applied yet")
	require.True(t, s.Check(103), "but only once")
}

func TestSeen_OlderThanTheWindowIsTreatedAsApplied(t *testing.T) {
	s := &Seen{}
	require.False(t, s.Check(1_000_000))
	require.True(t, s.Check(1_000_000-SeenSpan))
	require.True(t, s.Check(5))
}

func TestSeen_SlidingForwardForgetsOnlyTheOldest(t *testing.T) {
	s := &Seen{}
	require.False(t, s.Check(0))
	require.False(t, s.Check(SeenSpan-1))
	require.True(t, s.Check(0), "still inside the window")

	require.False(t, s.Check(SeenSpan)) // window is now (0, SeenSpan]
	require.True(t, s.Check(0), "just fell out of the window: treated as applied")
	require.True(t, s.Check(SeenSpan-1), "still remembered")
}

func TestSeen_BigJumpResetsTheWindow(t *testing.T) {
	s := &Seen{}
	for off := int64(0); off < 50; off++ {
		s.Check(off)
	}
	require.False(t, s.Check(10*SeenSpan))
	require.False(t, s.Check(10*SeenSpan-5), "inside the new window and never seen")
	require.True(t, s.Check(10*SeenSpan-5))
}

// property test against an obviously correct model: a map of every offset ever applied, bounded by the window
func TestSeen_MatchesAMapModel(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	s := &Seen{}
	model := map[int64]bool{}
	var hi int64 = -1

	for i := 0; i < 200_000; i++ {
		var off int64
		switch rng.Intn(4) {
		case 0: // replay something recent
			off = hi - int64(rng.Intn(2000))
		case 1: // slightly reordered
			off = hi + int64(rng.Intn(20)) - 10
		default: // moving forward, sometimes jumping
			off = hi + 1 + int64(rng.Intn(5))
			if rng.Intn(500) == 0 {
				off += int64(rng.Intn(3 * SeenSpan))
			}
		}
		if off < 0 {
			off = 0
		}

		want := model[off] || (hi >= 0 && off <= hi-SeenSpan)
		require.Equal(t, want, s.Check(off), "step %d offset %d hi %d", i, off, hi)
		model[off] = true
		if off > hi {
			hi = off
		}
	}
}

func TestSeenSet_SurvivesAGobRoundTripInsideACheckpoint(t *testing.T) {
	k := SrcKey{Topic: "kraken.raw.ticks", Partition: 3}
	set := SeenSet{}
	for off := int64(500); off < 600; off++ {
		set.Check(k, off)
	}

	var buf bytes.Buffer
	require.NoError(t, gob.NewEncoder(&buf).Encode(map[SrcKey]*Seen(set)))
	var restored map[SrcKey]*Seen
	require.NoError(t, gob.NewDecoder(&buf).Decode(&restored))

	back := SeenSet(restored)
	require.True(t, back.Check(k, 550), "a copy applied before the checkpoint is still recognised after a restart")
	require.False(t, back.Check(k, 600), "and the next record is new")
}

func BenchmarkSeen_Check(b *testing.B) {
	s := &Seen{}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s.Check(int64(i) * 40) // a symbol that sees roughly one record in 40 on its source partition
	}
}
