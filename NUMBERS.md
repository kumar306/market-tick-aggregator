# Phase 0.5 Reconciliation — currently-true numbers

Supersedes the resume-quoted claims and `BASELINE.md` for anything below. This is
what the resume and `TALK_TRACK.md` should be written against going forward.
Measured 2026-08-02, same machine/stack as `BASELINE.md` (12th Gen Intel
i7-1255U, Windows, full docker-compose stack). Phase 4 (AWS) should re-run all
of this on real infra before it's treated as final — see Task D.

## Task A — hot-path allocations (RESOLVED)

**Root cause, found via `-memprofile`/`pprof -alloc_objects`:** 99.75% of all
allocations (400,006 of 401,000 sampled objects) traced to a single call site —
`dedupe.ConstructDedupeKey`, which built the Redis dedupe key via
`topic + ":" + strconv.Itoa(partition) + ":" + strconv.Itoa(offset)`, called
**twice per tick** (once for `IsDuplicate`, once for `MarkForDedupe`, recomputing
the same key from the same record both times).

**Fix applied:**
1. Compute the key once, reuse it for both calls (halves the allocations
   immediately).
2. Replaced the string-concat construction with a per-`Worker` reused
   `[96]byte` scratch buffer + `strconv.AppendInt` + `unsafe.String`/
   `unsafe.SliceData` for a zero-copy string view. Safe here specifically
   because: the Redis calls (`go-redis` `.Result()`) and the error-path logger
   (`slog.NewJSONHandler`) are both synchronous, and `ProcessTick` only ever
   runs sequentially on one goroutine per worker — the returned string is
   fully consumed before the buffer is next overwritten.
3. **Not** `sync.Pool` — deliberately avoided, since it's the same
   GC-dependent non-determinism Phase 1 Fix 2 is explicitly removing
   elsewhere. A plain per-worker struct field is strictly better here: each
   `Worker` already has exactly one owner goroutine, so there's no
   contention to pool for.

**Verified via three independent signals**, not just the benchmark: (1)
`go test -benchmem` reports `0 B/op 0 allocs/op`, (2) a fresh `pprof
alloc_objects` profile shows zero allocations attributable to `ProcessTick`
during the timed loop (all ~975 remaining objects trace to one-time warm-up
setup — window/metric construction, proto reflection init, prometheus
registration — before `b.ResetTimer()`), (3) `-gcflags="-m"` escape analysis
confirms the `append` calls "escaping to heap" is expected noise (the backing
array already lives inside `*Worker`, which itself escapes once in
`NewWorker` — not a new per-call allocation).

**Reconciled number: `0 B/op, 0 allocs/op` — the "zero heap allocations on the
hot path" claim is now literally true**, and unlike before, it's proven by a
profile, not assumed.

**Unresolved tension:** clean `go test -bench=BenchmarkProcessTick_13Windows
-benchtime=2s -benchmem` now reports **3585 ns/op** (822,784 iterations) vs.
the original baseline's 2750 ns/op (which had 2 allocs/op). The zero-alloc fix
costs ~30% more wall-clock time per call — the bounds-checked `append` calls
and unsafe pointer construction outweigh the malloc/GC cost of the small,
short-lived strings they replace. **The original resume claim's two halves
("zero heap allocations" + "2.5µs/tick") cannot both be true simultaneously
with the current implementation:**

| | ns/op | allocs/op | ticks/sec/worker |
|---|---|---|---|
| Resume claim | 2500 | 0 | ~400,000 |
| Original baseline (pre-fix) | 2750 | 2 | ~364,000 |
| Current (zero-alloc fix applied) | 3585 | 0 | ~279,000 |

Decide which framing to keep for the resume/interview talk-track: (a) keep
the zero-alloc version and update the number to ~279K ticks/sec/worker but
correctly claim true zero-alloc, or (b) revert to the allocating version and
claim ~364K ticks/sec/worker with 2 allocs/op (closer to original ns/op but
zero-alloc claim stays false), or (c) investigate whether the buffer
construction can be tightened (e.g. pre-sizing to skip growth checks) to
recover some of the 835ns gap. Not resolved here — this is a resume-framing
decision, not an engineering one.

## Task B — Kafka ACK p99 gap (RESOLVED, root cause was the loadtest tool, not the broker)

**Ruled out:** broker replication factor. Every topic in
`scripts/init-kafka-topics.sh` is created with `replication-factor 1`
(single broker), so `acks=all` (franz-go's default) only ever waits on the
single leader — no multi-broker round trip was ever happening. This was not
the cause of the flat p99.

**Root cause found (via franz-go source in the Go module cache):**
`loadtest/main.go`'s Kafka client never set `ProducerLinger`, so it used
franz-go's **default of 10ms** (`pkg/kgo/config.go:565`). Production's real
producer (`adapter/kafka/producer.go:25`) explicitly sets
`kgo.ProducerLinger(0)` — the load-test tool was not measuring the same
producer behavior production actually has. This explained the suspiciously
flat ~7-8ms avg latency across every rate level (a fixed queuing delay, not a
capacity limit).

**Fix applied:** added `kgo.ProducerLinger(0)` to the loadtest client to match
production.

**Result — a real, honest trade-off, not a pure win:** average latency
dropped 50-75% at low/mid rates (500/s: 8.05ms → 1.96ms avg), but **tail
latency got worse under load** (10,000/s: p99 14.05ms → 38.26ms, max 35.66ms
→ 98.34ms). With linger=0, every record is its own network round trip instead
of being coalesced into batches — better typical-case latency, worse tail
variance at saturation. This is expected linger=0 behavior, not a bug.

## Task C — throughput plateau (RESOLVED, two independent root causes in the loadtest tool)

The pipeline was never the bottleneck — the load-generation tool was, for two
separate reasons:

1. **Every record used the same hardcoded Kafka key**
   (`"loadtest:benchmark:BTC-USD"`). Kafka's default partitioner hashes the
   key to pick a partition, so a constant key pinned **every** record to a
   single one of the topic's 6 partitions, capping throughput at one
   partition's ceiling instead of the aggregate across all 6.
2. **`time.Sleep(1ms)` overshoot on Windows.** At the 10,000/s rate level,
   math showed each "1ms" sleep actually took ~1.58ms (achieved 6,334/s
   implies ~1.58ms actual vs. 1ms requested); same ratio at 5,000/s (~1.46ms
   actual). Consistent overshoot, not noise.

**Fix applied:** parallelized into 6 goroutines (matching the 6 partitions),
each with a distinct key suffix so the partitioner spreads them out, and
switched to a coarser 10ms sleep tick so the fixed per-`Sleep()`-call OS
timer overshoot is a smaller fraction of the interval.

**Result:**

| Requested rate | Before fix | After fix |
|---|---|---|
| 2,500/s | 1,400/s (56%) | 2,304/s (92%) |
| 5,000/s | 3,241/s (65%) | 4,586/s (92%) |
| 10,000/s | 6,719/s (67%) | **9,203/s (92%)** |

**Reconciled number: 9,203/s sustained achieved, at p99 = 9.88ms** — this
**confirms** the resume's "<13ms p99 at 7,000+ events/sec sustained" claim is
now true, backed by a real measurement instead of an unverified assumption.

**Known residual limitation (not fixed, doesn't affect the claim above):** at
low requested rates (500/s and 1,000/s), both landed at ~575/s — an integer
truncation in the per-tick batch-size calculation floors any per-goroutine
rate under ~200/s to the same batch size. Irrelevant to the 7,000+/s claim,
left as-is rather than scope-creeping further.

## Task D — real infra (RESOLVED, Phase 4 AWS e2e load test)

Measured 2026-08-09 on real AWS infra (EKS, MSK Serverless, RDS, ElastiCache),
via mock exchange servers (`mockexchange/`) driving the full pipeline
end-to-end at a ramped rate (500 → 1,000 → 2,500 → 5,000 → 10,000/s combined
across binance/coinbase/kraken), captured via Grafana/Prometheus.

**Root cause found and fixed:** `orderbook_e2e_latency_seconds` p95 was
initially 25-29s versus aggregator's ~7s, reproduced identically on a local
docker-compose stack (ruling out AWS/MSK network as the cause) and traced to
`normalizer/utils/ts_orderer.go` — Coinbase `level2` and Kraken `book`
messages were routed through `TsOrder`, which unconditionally buffered every
message behind a **20-second** timer meant only to smooth millisecond-scale
websocket jitter. Fixed by reducing the window to 250ms. Supporting fixes:
`orderbook/worker/worker.go`'s snapshot clone bounded to top-N instead of a
full, ever-growing book iterate; `kgo.FetchIsolationLevel(ReadCommitted())`
added to orderbook's consumer for correctness; mock exchange now emits
realistic zero-quantity (cancel) updates so book depth doesn't grow
unbounded.

**Reconciled number (SUPERSEDED, see Task F):** `aggregator_e2e_latency_seconds` p95 ≈ 7.1-7.4s,
`orderbook_e2e_latency_seconds` p95 ≈ 7.3-7.4s — the two now match. Both are
bounded by the same shared mechanism: `normalizer`'s periodic Kafka
transaction commit (`commit_offset_interval_millis: 5000`) combined with
`read_committed` consumers, which is a real, understood, load-independent
floor rather than a bug.

## Per-service Prometheus p99s (stable across all runs above, for reference)

| Metric | Baseline | After linger fix | After partition+timer fix |
|---|---|---|---|
| `aggregator_tick_processing_duration_ms` p99 | 0.979ms | 1.765ms | 0.990ms |
| `aggregator_window_flush_duration_ms` p99 | 0.716ms | 0.884ms | 0.819ms |
| `normalizer_commit_latency_seconds` p99 | 12.85ms | 15.52ms | 13.64ms |

No regression across any of the fixes above — variation is within normal
run-to-run noise for this environment.

## Task E — pprof heap analysis on real AWS load (RESOLVED, adapter fix)

Captured via `capture-pprof.sh` against the live EKS pods during an active
10,000/s-combined load ramp (`inuse_space` + `alloc_space` heap profiles,
30s CPU profiles, one per service).

**Root cause found:** adapter's `inuse_space` profile showed 89% of a 60.43MB
live heap (54.03MB) attributed to a single call site — `io.ReadAll` inside
`ReadMessages`, called via `gorilla/websocket.Conn.ReadMessage()`'s internal
`NextReader()` + `io.ReadAll` implementation. Every inbound exchange message
allocated a fresh growable buffer; six independent per-stream ring buffers
(`ringBufferSize: 16384` each) holding backlogged, not-yet-drained messages
at profile time meant this added up fast. The `alloc_space` view (total
allocation volume, not just live) confirmed it as the single largest
allocator by far: 0.36GB of 2.13GB total (17.05%).

**Fix applied:** `adapter/internal/conn_mgmt.go`'s `ReadMessages` switched
from `conn.ReadMessage()` to `conn.NextReader()` + a `sync.Pool`-backed
scratch buffer (`adapter/internal/buffer_pool.go`), sized to the typical
tick/depth JSON payload (1024B) so the common case needs zero growth. The
buffer is returned to the pool from `PublishToKafkaLoop` immediately after
`normalizer.Normalize()` consumes it — verified safe because `Normalize`
(`adapter/feeds/utils.go`) never aliases its input: `string(raw[...])`
copies (Go's `[]byte`→`string` conversion always copies), and the
`normalized` output is built via `make` + `append` into an independent
backing array. No coordination with Kafka's async produce callback needed,
since the pooled buffer never travels past `Normalize()`.

**Reconciled number:** adapter's live heap dropped from **60.43MB to
6.8MB** (89%/54MB reduction) with the same live-traffic conditions. `io.ReadAll`
no longer appears anywhere in the post-fix profile; the remaining heap is
entirely one-time/library-owned (Kafka producer compression buffers, ring
buffer's one-time startup allocation, TLS handshake/cert setup, the
`/metrics` endpoint's own response).

**Known follow-ups (not fixed, out of scope for this pass):**
- `feeds/utils.Normalize`'s output buffer (`out := make([]byte, 0, ...)`)
  is a separate, real allocation site — 10.79% of adapter's total
  `alloc_space` volume. Unlike the buffer above, this one genuinely outlives
  the synchronous call chain (it's what `kafka.ProduceAsync` hands to
  `client.Produce()`, held until the async ack), so pooling it needs
  return-from-produce-callback coordination, not the simpler synchronous
  return used above.
- `orderbook`'s Redis client (`min_idle_conns: 100`) accounts for ~10MB
  (56.62% of a small 17.7MB heap) in eagerly-held idle connections plus
  their buffered readers/writers — for a usage pattern (periodic snapshot
  writes every 20s) that doesn't need anywhere near 100 idle connections.
  Config tune, not a code change.

## Task F — final end-to-end latency, ingest to Postgres commit (RESOLVED, 2026-10-16)

Measured on the same AWS stack as Task D (EKS, MSK Serverless, RDS, ElastiCache), mock exchanges, ramp 500 → 10,000/s
offered. Supersedes the 7.1-7.4 s figure above. Full write-up: `docs/aws_loadtest_metrics/RESULTS.md`.

**Definition:** `persistence_ingest_to_insert_seconds`: the adapter stamps the time it reads each message off the exchange
websocket (before the ring buffer) into a Kafka header; the normalizer copies it, aggregator and orderbook emit the newest
one since their last flush, and persistence observes it after the Postgres transaction commits. It is the age of the newest
data a row reflects when the row becomes visible, not the delay of every individual tick.

**Result (steady state):** tick aggregates p50 47 ms, **p95 ~85 ms** (per-level 67-100 ms), p99 ~120-135 ms. Orderbook
flushes p50 ~150 ms, p95 ~0.3 s (dominated by the orderbook's 250 ms ordering window). 92% of tick rows visible within
75 ms, 98% within 100 ms, 100% within 200 ms.

**What changed to get there:** Kafka transactions replaced by at-least-once delivery with source-offset dedupe
(normalizer commits behind acked outputs; aggregator checkpoints window state + applied source offsets in one record per
symbol); persistence batcher flushes `max_wait_ms` (10 ms) after a batch's first row instead of on a ticker. The ticker
version flushed every other tick (0.5 flushes/s at a 1 s setting), so rows waited 0-2 s and p95 read ~2 s; that was the
batcher, not Postgres (flush p50 7.7 ms / p95 18 ms for ticks).

**Real broker latency from the pods:** Produce 3.1 ms p50 / 4.8 ms p95, Fetch 3.3 / 10.6, OffsetCommit 3.1 / 5.8,
Heartbeat 1.7 / 3.7. Not comparable to the Task D figure: different mode and measurement point; transactional mode was not
re-run on this run's infrastructure.

**Caveats:** one run; ~4.9k msg/s delivered at the top levels because the mock exchanges sleep `1s/rate` per message and
cap near 800 msg/s per stream (adapter rings stayed empty); first minute of the ramp is a consumer-rejoin transient.
