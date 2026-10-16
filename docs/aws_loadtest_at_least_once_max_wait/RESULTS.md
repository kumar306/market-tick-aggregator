# AWS load test, run 2: persistence batcher flushes `max_wait_ms` after the first row

Run: 2026-10-16, ramp 12:09:55 to 12:14:55 IST, image tag `v15`, same cluster as run 1
(`../aws_loadtest_at_least_once/`). Same `at_least_once` normalizer and aggregator, same mock-exchange ramp. The only
changes: the persistence batcher redesign (`max_wait_ms: 10`, batch size 100 / 200 unchanged) and fine histogram
buckets. One run, one pass.

Files: `e2e-ingest-to-insert.png` (steady state 12:12:30 to 12:16:30), `e2e-ingest-to-insert-full-range.png` (from
12:09:30, includes the startup transient), `hops-and-broker.png`, `persistence-batching.png`, `e2e-by-level.json`,
`pprof/` (heap + 30s CPU, five services, captured at the plateau after 12:16:39).

## What was wrong in run 1

The tick pipeline flushed 0.5 times a second although the interval was 1 s. The batcher used a 1 s ticker plus a check
"has a full interval passed since the last flush". With both equal to 1 s, scheduler jitter made the check fail every
other tick, so a row's wait was uniform over 0 to 2 s: median about 1 s, 46.8% of rows under 1.024 s, 100% under
2.048 s. That is where the "2.05 s" came from; it was also the upper edge of a doubling histogram bucket, so it was never
a measured value. Postgres was not the limit: a flush costs 7.7 ms p50 / 18 ms p95 (ticks).

## What changed

- A batch is flushed at `batch_size` rows, or `max_wait_ms` after its FIRST row arrived. A per-batch timer, no ticker.
  The wait is bounded by max_wait plus the flush, whatever the arrival phase (a test arrives rows every 7 ms against a
  40 ms wait and asserts no row waits longer, repeated 10 times).
- A failed flush is retried after another max_wait.
- A batch whose rows all failed validation is now cleared and its offsets committed. It used to stay in the batch and be
  re-sent to the dead-letter queue on every later flush, which a short wait would turn into a flood.
- `interval_ms` became `max_wait_ms` (floor 1 ms, was 1000 ms). Set to 10 ms for both pipelines.
- `persistence_ingest_to_insert_seconds` has 23 buckets from 5 ms to 60 s.
- `batch_flush_duration` was observing nanoseconds into second-scale buckets; fixed.

## Result (ingest to Postgres commit, newest data in the row)

| Offered msg/s | Delivered | Tick rows p50 | p95 | p99 | Book rows p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| 500 | not usable: startup transient | | | | | | |
| 1,000 | 932 | 44 ms | 67 ms | 106 ms | 88 ms | 355 ms | 391 ms |
| 2,500 | 1,804 | 47 ms | 75 ms | 136 ms | 138 ms | 355 ms | 391 ms |
| 5,000 | 2,709 | 47 ms | 71 ms | 74 ms | 125 ms | 285 ms | 297 ms |
| 10,000 | 4,886 | 48 ms | 92 ms | 135 ms | 94 ms | 255 ms | 291 ms |

Run 1 for comparison (same levels): tick p50 about 1.3 s, p95 somewhere in 1.02 to 2.05 s, p99 about 2 s.

Distribution of 1,351 tick rows, 12:12 to 12:16 IST: 66.6% within 50 ms, 92.2% within 75 ms, 98.2% within 100 ms,
99.4% within 150 ms, 100% within 200 ms. Book rows (72): 33% within 75 ms, 70% within 200 ms, 90.5% within 300 ms,
100% within 400 ms.

## Where the remaining time goes

- Persistence now adds at most the 10 ms wait plus the flush (7.7 ms p50, 17.5 ms p95 for ticks; 17.5 ms p50 and 24 ms
  p95 for book). About 1.07 tick flushes/s and 0.1 book flushes/s, 5.3 and 3 rows per flush.
- Ingest to aggregator consume is 30 to 50 ms at p95, so most of the remaining ~47 ms median is before persistence.
- Book rows (p95 about 0.3 s) are dominated by the orderbook itself: ingest to orderbook-applied p95 is 0.26 to 0.36 s,
  which includes the 250 ms ordering window on Coinbase and Kraken book.
- Going below 10 ms would save at most about 8 ms of the ~47 ms median and is bounded by the flush time. It was NOT
  tested. A burst of aggregates arrives from three partitions in separate fetches a few milliseconds apart; a shorter
  wait risks splitting a burst into several transactions that run one after another on the single batcher goroutine.

## Caveats

- One run. Percentiles are from 60 s windows (per level) and 1 m rate windows (graphs) on a 30 s scrape interval.
- The first minute of the ramp is dominated by consumers rejoining their groups after the offset reset, not by
  steady-state behaviour (the 500/s row; its p99 reads 60 s). The full-range image keeps it visible.
- The metric is the age of the NEWEST data a row reflects, not the delay of every individual tick.
- Delivered rate is about 4.9k msg/s at the top levels because the mock exchanges send about 800 msg/s per stream
  (they sleep `1s / rate` per message), not because of the pipeline.
- Transactional mode has not been run on this infrastructure.
- A 3 minute window showed one slow tick flush (Postgres p99 128 ms); everything else stayed under 25 ms.
