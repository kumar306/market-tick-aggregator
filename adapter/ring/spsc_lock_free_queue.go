package ring

import (
	"market-adapter/metrics"
	"sync/atomic"
)

// per feed ring buffer
// producer pushes websocket messages to ring buffer
// consumer reads and publishes to kafka topic
type SpscDropOldestRing[T any] struct {
	Buf      []T
	Mask     uint64
	Capacity uint64
	Head     uint64
	Tail     uint64
	Name     string
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

	metrics.BufferCapacity.WithLabelValues(name).Set(float64(capacity))
	return r
}

// ring buffer push
// if reached capacity, then overwrite
func (r *SpscDropOldestRing[T]) Push(v T) {
	t := atomic.LoadUint64(&r.Tail)
	h := atomic.LoadUint64(&r.Head)
	if t-h >= r.Capacity {
		// inc head
		atomic.StoreUint64(&r.Head, h+1)
		metrics.BufferDrops.WithLabelValues(r.Name).Inc()
	}

	r.Buf[t&r.Mask] = v
	atomic.StoreUint64(&r.Tail, t+1)
	r.updateLenMetric()
}

// ring buffer pop
func (r *SpscDropOldestRing[T]) Pop() (T, bool) {
	var zero T
	h := atomic.LoadUint64(&r.Head)
	t := atomic.LoadUint64(&r.Tail)
	if h == t {
		// empty
		return zero, false
	}
	val := r.Buf[h&r.Mask]
	atomic.StoreUint64(&r.Head, h+1)
	r.updateLenMetric()
	return val, true
}

// update len metric
func (r *SpscDropOldestRing[T]) updateLenMetric() {
	curLen := atomic.LoadUint64(&r.Tail) - atomic.LoadUint64(&r.Head)
	metrics.BufferLen.WithLabelValues(r.Name).Set(float64(curLen))
}
