package ring

import (
	"shared/metrics"
	"sync/atomic"
)

// per feed ring buffer
// producer pushes websocket messages to ring buffer
// consumer reads and publishes to kafka topic
// add 7 byte padding after head to prevent false sharing between head and tail - move to different cache lines
type SpscDropOldestRing[T any] struct {
	Buf      []T
	Name     string
	Mask     uint64
	Capacity uint64
	Head     uint64
	_        [7]uint64 // pad to a full 64-byte cache line, isolating Head from Tail below
	Tail     uint64
	_        [7]uint64
}

// using bitwise AND instead of modulo operator for wrapping
func NewSpscDropOldestRing[T any](capacity uint64, name string) *SpscDropOldestRing[T] {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		panic("capacity must be a power of 2")
	}

	r := &SpscDropOldestRing[T]{
		Buf:      make([]T, capacity),
		Mask:     capacity - 1,
		Capacity: capacity,
		Name:     name,
	}

	metrics.Adapter_BufferCapacity.WithLabelValues(name).Set(float64(capacity))
	return r
}

// Push is producer-only: it never touches Head, so it never races with Pop.
// It always succeeds and never blocks. If the consumer has fallen behind by
// more than Capacity, older unread slots get overwritten here - Pop detects
// and accounts for that on the read side, since Head belongs exclusively to
// the consumer.
func (r *SpscDropOldestRing[T]) Push(v T) {
	t := atomic.LoadUint64(&r.Tail)
	r.Buf[t&r.Mask] = v
	atomic.StoreUint64(&r.Tail, t+1)
	r.updateLenMetric()
}

// Pop is consumer-only: it exclusively owns Head. The returned uint64 is how
// many entries were dropped (overwritten before being read) to catch up to
// this one, if any - the caller uses this to tag the next published message.
func (r *SpscDropOldestRing[T]) Pop() (T, bool, uint64) {
	var zero T
	h := atomic.LoadUint64(&r.Head)
	t := atomic.LoadUint64(&r.Tail)
	if h == t {
		// empty
		return zero, false, 0
	}

	var dropped uint64
	if t-h > r.Capacity {
		dropped = (t - h) - r.Capacity
		h = t - r.Capacity
		metrics.Adapter_BufferDrops.WithLabelValues(r.Name).Add(float64(dropped))
	}

	val := r.Buf[h&r.Mask]
	atomic.StoreUint64(&r.Head, h+1)
	r.updateLenMetric()
	return val, true, dropped
}

// update len metric
func (r *SpscDropOldestRing[T]) updateLenMetric() {
	curLen := atomic.LoadUint64(&r.Tail) - atomic.LoadUint64(&r.Head)
	metrics.Adapter_BufferLen.WithLabelValues(r.Name).Set(float64(curLen))
}
