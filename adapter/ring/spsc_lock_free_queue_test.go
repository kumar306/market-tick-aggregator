package ring_test

import (
	"market-adapter/ring"
	"shared/metrics"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// test the ring buffer to ensure it drops oldest message

func Test_RingBufferDropsOldest(t *testing.T) {
	bufName := "test_drop_oldest"
	cap := uint64(4)
	ring := ring.NewSpscDropOldestRing[int](cap, bufName)

	for i := 1; i <= int(cap); i++ {
		ring.Push(i)
	}

	ring.Push(5)
	ring.Push(6)

	var result []int
	for {
		val, ok := ring.Pop()
		if !ok {
			break
		}
		result = append(result, val)
	}

	// verify that 1,2 got dropped. expected is 5,6,3,4
	expected := []int{3, 4, 5, 6}

	require.Equal(t, expected, result, "Ring buffer should drop oldest entries")

	dropMetric := testutil.ToFloat64(metrics.Adapter_BufferDrops.WithLabelValues(bufName))
	require.Equal(t, float64(2), dropMetric, "expected 2 drops")

	lenMetric := testutil.ToFloat64(metrics.Adapter_BufferLen.WithLabelValues(bufName))
	require.Equal(t, float64(0), lenMetric, "expected buffer to be empty after full pop")
}

// test whether it can read all messages
// a sample test run:
// a buffer size of 1024 ensured 0 buffer drops for 10000 writes in under a second
// a buffer size of 128 gave around 400 buffer drops out of 10000 writes
// a buffer size of 64 gave around 1453 drops out of 10000 writes
// the buffer drops are inconsistent between test runs
func Test_RingBufferConcurrency(t *testing.T) {

	tests := []struct {
		name string
		cap  uint64
	}{
		{"test_size_1024", 1024},
		{"test_size_128", 128},
		{"test_size_64", 64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ring := ring.NewSpscDropOldestRing[int](tc.cap, tc.name)

			const totalWrites = 10_000
			var produced, consumed uint64
			done := make(chan struct{})

			// producer to push into ring
			go func() {
				for i := 0; i < totalWrites; i++ {
					ring.Push(i)
					atomic.AddUint64(&produced, 1)
				}

				close(done)
			}()

		outer:
			for {
				val, ok := ring.Pop()
				if ok {
					_ = val
					atomic.AddUint64(&consumed, 1)
				} else {
					select {
					case <-done:
						// if all messages are consumed
						t.Logf("Current produced: %v, Current consumed: %v", atomic.LoadUint64(&produced), atomic.LoadUint64(&consumed))
						break outer
					default:
						continue
					}
				}
			}

			producedVal := atomic.LoadUint64(&produced)
			consumedVal := atomic.LoadUint64(&consumed)

			drops := testutil.ToFloat64(metrics.Adapter_BufferDrops.WithLabelValues(tc.name))
			t.Logf("Capacity: %v, Produced: %v, Consumed: %v, Buffer drops: %v", tc.cap, producedVal, consumedVal, drops)

			require.LessOrEqual(t, drops, float64(producedVal), "buffer drops should be less than total produced")
			require.Greater(t, producedVal, tc.cap)
		})
	}

}

// on average seeing ~230 ns/op across different buffer sizes. 0 memory allocations per operation
// this equates to 1s/230 ns = 4.44 mil msgs/sec on 1 core
// go ran it with 12 threads
/*
goos: windows
goarch: amd64
pkg: market-adapter/ring
cpu: 12th Gen Intel(R) Core(TM) i7-1255U
=== RUN   Benchmark_RingBuffer_VariableSize
Benchmark_RingBuffer_VariableSize
=== RUN   Benchmark_RingBuffer_VariableSize/size_64
Benchmark_RingBuffer_VariableSize/size_64
Benchmark_RingBuffer_VariableSize/size_64-12
 5159960               225.4 ns/op             0 B/op          0 allocs/op
=== RUN   Benchmark_RingBuffer_VariableSize/size_128
Benchmark_RingBuffer_VariableSize/size_128
Benchmark_RingBuffer_VariableSize/size_128-12
 5137339               226.3 ns/op             0 B/op          0 allocs/op
=== RUN   Benchmark_RingBuffer_VariableSize/size_512
Benchmark_RingBuffer_VariableSize/size_512
Benchmark_RingBuffer_VariableSize/size_512-12
 5326266               225.9 ns/op             0 B/op          0 allocs/op
=== RUN   Benchmark_RingBuffer_VariableSize/size_1024
Benchmark_RingBuffer_VariableSize/size_1024
Benchmark_RingBuffer_VariableSize/size_1024-12
 5241522               237.4 ns/op             0 B/op          0 allocs/op
*/
func Benchmark_PushRingBuffer_VariableSize(b *testing.B) {
	tests := []struct {
		name string
		cap  uint64
	}{
		{"size_64", 64},
		{"size_128", 128},
		{"size_512", 512},
		{"size_1024", 1024},
	}
	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			ring := ring.NewSpscDropOldestRing[int](tc.cap, "benchmark")
			done := make(chan struct{})
			b.ReportAllocs()
			b.ResetTimer()

			go func() {
				for {
					select {
					case <-done:
						return
					default:
						ring.Pop()
					}
				}
			}()

			for i := 0; i < b.N; i++ {
				ring.Push(i)
			}
			close(done)

		})
	}
}

// this takes ~81 ns/op and no mem allocs
/*

goos: windows
goarch: amd64
pkg: market-adapter/ring
cpu: 12th Gen Intel(R) Core(TM) i7-1255U
=== RUN   Benchmark_PushPopRingBuffer_VarSize
Benchmark_PushPopRingBuffer_VarSize
=== RUN   Benchmark_PushPopRingBuffer_VarSize/size_64
Benchmark_PushPopRingBuffer_VarSize/size_64
Benchmark_PushPopRingBuffer_VarSize/size_64-12
13068464                88.85 ns/op            0 B/op          0 allocs/op
=== RUN   Benchmark_PushPopRingBuffer_VarSize/size_128
Benchmark_PushPopRingBuffer_VarSize/size_128
Benchmark_PushPopRingBuffer_VarSize/size_128-12
13267658                81.92 ns/op            0 B/op          0 allocs/op
=== RUN   Benchmark_PushPopRingBuffer_VarSize/size_512
Benchmark_PushPopRingBuffer_VarSize/size_512
Benchmark_PushPopRingBuffer_VarSize/size_512-12
14038962                79.87 ns/op            0 B/op          0 allocs/op
=== RUN   Benchmark_PushPopRingBuffer_VarSize/size_1024
Benchmark_PushPopRingBuffer_VarSize/size_1024
Benchmark_PushPopRingBuffer_VarSize/size_1024-12
15834685                81.92 ns/op            0 B/op          0 allocs/op
PASS
ok      market-adapter/ring     7.402s
*/
func Benchmark_PushPopRingBuffer_VarSize(b *testing.B) {
	tests := []struct {
		name string
		cap  uint64
	}{
		{"size_64", 64},
		{"size_128", 128},
		{"size_512", 512},
		{"size_1024", 1024},
	}

	for _, tc := range tests {
		b.Run(tc.name, func(b *testing.B) {
			ring := ring.NewSpscDropOldestRing[int](tc.cap, "benchmark")
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				ring.Push(i)
				ring.Pop()
			}
		})
	}
}
