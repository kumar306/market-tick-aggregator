package srcoffset

// SeenSpan is how many source offsets, counting back from the highest one seen, a Seen remembers.
// It must be a power of two.
//
// How far back can a duplicate be? A replay starts at the producer's committed offset (its "frontier"), and the
// frontier is held back by three things: the commit interval (1s), the longest the producer keeps a record
// parked in an orderer (the sequence gap timer, 2s), and the time for a produce ack to come back (about an RTT, ~1s
// allowed). So the span has to cover (gap timer + commit interval + ack time) x records per second on one source
// partition. Sizing: 10 symbol pairs x 100 msg/s x 2 channels = 2,000 msg/s, taken as the worst case for a single
// partition; (2 + 1 + 1)s x 2,000 = 8,000 offsets. 16384 gives 2x headroom (about 8s at that rate).
// If the gap timer, the commit interval or the load grows, redo this product.
const SeenSpan = 1 << 14

// SrcKey identifies a source partition, the unit in which offsets are totally ordered.
type SrcKey struct {
	Topic     string
	Partition int32
}

// Seen is a sliding bitset over the last SeenSpan offsets of one source partition. Fields are exported so the
// whole thing can be gob-encoded into a checkpoint and restored after a crash.
type Seen struct {
	Hi      int64 // highest offset seen
	Started bool
	Bits    []uint64
}

// Check reports whether off was already applied, and records it if not.
//   - above Hi: new, the window slides forward
//   - inside the window: duplicate iff its bit is set, so a record that arrives late (reordered) is still new
//   - older than the window: reported as a duplicate. We cannot actually know. The reasoning is structural: a
//     replay can only reach back as far as the producer's frontier, and SeenSpan is sized to cover that. A record
//     older than the window is therefore either an impossible replay or an impossibly late record, and when in
//     doubt "already applied" avoids double counting. If SeenSpan is ever sized too small, this branch is where
//     a late-but-genuine record would be wrongly dropped.
func (s *Seen) Check(off int64) (duplicate bool) {
	if s.Bits == nil {
		s.Bits = make([]uint64, SeenSpan/64)
	}
	if !s.Started {
		s.Started, s.Hi = true, off
		s.set(off)
		return false
	}

	switch {
	// if off - high >= a whole window, clear all bits - circular bitset
	// else , if off > high, clear those slots between hi+1, off as it will be reused

	case off > s.Hi:
		if off-s.Hi >= SeenSpan {
			clear(s.Bits)
		} else {
			s.clearRange(s.Hi+1, off)
		}
		s.Hi = off
		s.set(off)
		return false
		// we assume that if offset is out of order, before window start, it must be a duplicate so return true
	case off <= s.Hi-SeenSpan:
		return true

	default:
		// check if duplicate, then return true
		if s.get(off) {
			return true
		}
		// set the slot the offset occupies, so future redelivery in this slot will be a duplicate
		s.set(off)
		return false
	}
}

// How an offset becomes a position in Bits (the same three lines are in set, get and clearRange):
//
//	i := off & (SeenSpan - 1)   // slot: off mod SeenSpan. SeenSpan is a power of two, so keeping the low bits
//	                            // is the modulo, and a bitwise AND is much cheaper than a real division.
//	i >> 6                      // which word: divide by 64, because one uint64 holds 64 flags. A shift is
//	                            // the same trick as above, a single instruction.
//	i & 63                      // which bit inside that word: the remainder of the divide by 64.
//
// Offsets SeenSpan apart land on the same slot. That is the "circular" part, and why moving Hi forward has to
// clear the slots it passes over before they are reused.

func (s *Seen) set(off int64) {
	i := off & (SeenSpan - 1)
	s.Bits[i>>6] |= 1 << uint(i&63)
}

func (s *Seen) get(off int64) bool {
	i := off & (SeenSpan - 1)
	return s.Bits[i>>6]&(1<<uint(i&63)) != 0
}

// clearRange clears the bits for offsets lo..hi inclusive, a word at a time instead of a bit at a time.
// Each pass: find the word and the bit where o sits, build a mask covering the bits from there to the end of
// that word (or to hi, whichever comes first), clear those bits with AND-NOT, then jump o forward by the number
// of bits just cleared so the next pass starts at the beginning of the next word.
func (s *Seen) clearRange(lo, hi int64) {
	for o := lo; o <= hi; {
		i := o & (SeenSpan - 1)
		bit := uint(i & 63)
		n := 64 - bit // bits left in this word, from `bit` to the end
		if rem := hi - o + 1; int64(n) > rem {
			n = uint(rem) // the range ends inside this word
		}
		mask := ^uint64(0)
		if n < 64 {
			mask = ((uint64(1) << n) - 1) << bit // n ones, moved up to start at `bit`
		}
		s.Bits[i>>6] &^= mask
		o += int64(n) // jump to the first offset of the next word
	}
}

// SeenSet holds one Seen per source partition. A symbol's records normally all come from one source partition,
// so this usually has a single entry.
type SeenSet map[SrcKey]*Seen

func (s SeenSet) Check(k SrcKey, off int64) (duplicate bool) {
	seen := s[k]
	if seen == nil {
		seen = &Seen{}
		s[k] = seen
	}
	return seen.Check(off)
}
