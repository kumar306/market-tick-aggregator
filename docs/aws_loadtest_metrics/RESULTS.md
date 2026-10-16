# AWS load test results

Run: 2026-10-16, ramp 12:09:55 to 12:14:55 IST, EKS (2 x t3.medium) + MSK Serverless + RDS + ElastiCache, mock
exchanges driving the adapter. `delivery_mode: at_least_once` in normalizer and aggregator. Persistence batching: 100 rows
(ticks) / 200 rows (book) or 10 ms after the first row of a batch. One run, one pass; nothing is averaged over repeats.

Files: `e2e-ingest-to-insert.png` (steady state, 12:12:30 to 12:16:30), `e2e-ingest-to-insert-full-range.png` (from
12:09:30, includes the startup transient), `hops-and-broker.png`, `persistence-batching.png`, `e2e-by-level.json`
(per-level numbers), and `../aws_loadtest_pprof/` (heap + 30 s CPU, five services, captured at the plateau).

## The metric

`persistence_ingest_to_insert_seconds{pipeline}`: the adapter stamps the time it reads each message off the exchange
websocket (before the ring buffer) into a Kafka header; the normalizer copies it; the aggregator and orderbook emit the
newest one since their last flush; persistence observes it after the Postgres transaction that inserted the row commits,
which is when the row becomes visible. It is the age of the NEWEST data a row reflects, not the delay of every individual
tick: a tick that arrives just after a window flush also waits for that window's next flush, and this metric does not show
that. The histogram has 23 buckets from 5 ms to 60 s.

## How persistence batches

A batch is flushed when it reaches `batch_size` rows, or `max_wait_ms` (10 ms) after its FIRST row arrived, whichever
comes first. The clock is a per-batch timer started by the first row, so a row never waits longer than `max_wait_ms` plus
the flush, whatever its arrival phase. A failed flush is retried after another `max_wait_ms`. A batch whose rows all fail
validation is sent to the dead-letter queue once, then cleared with its offsets committed. A flush is one Postgres
transaction (temp table, COPY, INSERT ... ON CONFLICT DO NOTHING, COMMIT): 7.7 ms p50 / 18 ms p95 for ticks, 17.5 ms p50 /
24 ms p95 for book, at about 1.07 tick flushes/s (5.3 rows each) and 0.1 book flushes/s (3 rows each).

## Result per ramp level (60 s window each, ingest to Postgres commit)

| Offered msg/s | Delivered | Tick rows p50 | p95 | p99 | Book rows p50 | p95 | p99 |
|---|---|---|---|---|---|---|---|
| 500 | not usable: window is the startup transient | | | | | | |
| 1,000 | 932 | 44 ms | 67 ms | 106 ms | 88 ms | 355 ms | 391 ms |
| 2,500 | 1,804 | 47 ms | 75 ms | 136 ms | 138 ms | 355 ms | 391 ms |
| 5,000 | 2,709 | 47 ms | 71 ms | 74 ms | 125 ms | 285 ms | 297 ms |
| 10,000 | 4,886 | 48 ms | 92 ms | 135 ms | 94 ms | 255 ms | 291 ms |

Steady-state summary (graphs, 12:12:30 to 12:16:30): tick rows p50 47 ms, **p95 about 85 ms**, p99 about 120 ms; book rows
p50 about 150 ms, p95 about 0.3 s, p99 about 0.34 s.

Distribution of 1,351 tick rows, 12:12 to 12:16 IST: 66.6% within 50 ms, 92.2% within 75 ms, 98.2% within 100 ms, 99.4%
within 150 ms, 100% within 200 ms. Book rows (72): 33% within 75 ms, 70% within 200 ms, 90.5% within 300 ms, 100% within
400 ms.

## Earlier hops

| Offered | Ingest to aggregator pickup, p95 | Ingest to orderbook applied, p95 |
|---|---|---|
| 1,000 | 0.03 s | 0.26 s |
| 2,500 | 0.03 s | 0.31 s |
| 5,000 | 0.05 s | 0.33 s |
| 10,000 | 0.05 s | 0.34 s |

The orderbook figure includes the 250 ms ordering window on Coinbase and Kraken book, which is also why book rows are
slower than tick rows.

## Broker call latency on real MSK (normalizer, 5 min at the plateau)

| Request | p50 | p95 | calls/s |
|---|---|---|---|
| Fetch | 3.3 ms | 10.6 ms | 1,160 |
| Produce | 3.1 ms | 4.8 ms | 274 |
| OffsetCommit | 3.1 ms | 5.8 ms | 5.4 |
| Heartbeat | 1.7 ms | 3.7 ms | 0.9 |

A pod-to-MSK call is a few milliseconds. The slow `JoinGroup` (8 s) is the consumer group rejoining after the offset reset
at the start of the run.

## Throughput

Offered load reached 10,000 msg/s but about 4.9k msg/s was delivered at the top levels. The pipeline was not the limit:
the adapter published 4,883 msg/s spread evenly over its six streams (about 800 each), its ring buffers were empty and no
drops were recorded. The mock exchanges send about 800 msg/s per stream when asked for 1,666 (they sleep `1s / rate` per
message), so "10,000 msg/s" here means about 4.9k delivered.

CPU at the plateau (30 s profile, share of one core): normalizer 40%, adapter 31%, aggregator 7%, orderbook 5%,
persistence 0.2%.

## Caveats

- One run. Percentiles come from 60 s windows (per level) and 1 m rate windows (graphs) on a 30 s scrape interval.
- The first minute of the ramp is dominated by consumers rejoining their groups after the offset reset, not by
  steady-state behaviour (the 500/s row; its p99 reads about a minute). The full-range image keeps it visible.
- A 3 minute window showed one slow tick flush (Postgres p99 128 ms); everything else stayed under 25 ms.
