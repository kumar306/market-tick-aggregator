# AWS load test: at_least_once mode, tuned persistence batching

Run: 2026-10-16, 11:02:42 to 11:07:42 IST ramp, image tag `v14`, EKS (2 x t3.medium) + MSK Serverless + RDS +
ElastiCache. Mock exchanges drive the adapter. `delivery_mode: at_least_once` in normalizer and aggregator, persistence
batching 100 rows or 1s (was 30s / 50s). One run, one pass. Nothing here is averaged over repeats.

Files: `e2e-ingest-to-insert.png` (steady state, 11:04 to 11:12), `e2e-ingest-to-insert-full-range.png` (whole run, includes
the startup transient), `e2e-by-level.json` (per-level numbers), `pprof/` (heap and 30s CPU for all five services,
captured at the 10k plateau).

## The metric

`persistence_ingest_to_insert_seconds{pipeline}`: age of the NEWEST exchange message a row reflects, measured when the
Postgres transaction that inserted the row commits. The adapter stamps the time at websocket read, before the ring
buffer. It is data freshness, not the delay of every individual tick: a tick that arrives just after a window flush still
waits for the next flush, and this metric does not show that.

## Result per ramp level (60s window each, ingest to Postgres commit)

| Offered msg/s | Delivered msg/s (ticks + book) | Tick rows p50 | Tick rows p95 | Book rows p50 | Book rows p95 |
|---|---|---|---|---|---|
| 500 | not usable: window is the startup transient (6 samples) | | | | |
| 1,000 | 933 | 1.30 s | <= 2.05 s | 0.28 s | <= 0.51 s |
| 2,500 | 1,826 | 1.29 s | <= 2.05 s | 0.28 s | <= 0.51 s |
| 5,000 | 2,658 | 1.27 s | <= 2.05 s | 0.32 s | <= 0.51 s |
| 10,000 | 4,857 | 0.89 s | <= 2.05 s | 0.28 s | <= 0.51 s |

**The p95 cannot be read more precisely than the bucket it falls in.** The histogram buckets double (0.512, 1.024,
2.048, ...). Steady state, 1,022 tick rows (11:04:30 to 11:07:30 IST): 0% finished within 0.512 s, 46.8% within 1.024 s,
100% within 2.048 s. So the tick p95 is somewhere in (1.024 s, 2.048 s]. The flat ~1.9 to 2.0 s line in the graph is the
interpolation inside that one bucket, not a measured plateau. It does NOT show 1.5 s p95, and it does not rule it out.
Book rows (54 samples): 40% within 0.256 s, 100% within 0.512 s.

## Throughput: offered is not delivered

At the 5k and 10k levels only about half the offered rate was delivered (about 4.9k msg/s total). It was not the pipeline:
the adapter published 4,883 msg/s, spread evenly over its six streams (about 800 each), its ring buffers were empty, and
no ring drops were recorded. The mock exchanges are sending about 800 msg/s per stream when asked for 1,666. I have not
looked at why. Until that is fixed, "10,000 msg/s" here means about 4.9k delivered.

## Earlier hops (for context)

| Offered | Ingest to aggregator consume, p95 | Ingest to orderbook applied, p95 |
|---|---|---|
| 1,000 | 0.03 s | 0.25 s |
| 2,500 | 0.03 s | 0.31 s |
| 5,000 | 0.05 s | 0.33 s |
| 10,000 | 0.06 s | 0.36 s |

The orderbook figure includes the 250 ms ordering window on Coinbase and Kraken book.

## Broker call latency on real MSK (normalizer, 5 min at the plateau)

| Request | p50 | p95 | calls/s |
|---|---|---|---|
| Fetch | 3.3 ms | 10.6 ms | 1,160 |
| Produce | 3.1 ms | 4.8 ms | 274 |
| OffsetCommit | 3.1 ms | 5.8 ms | 5.4 |
| Heartbeat | 1.7 ms | 3.7 ms | 0.9 |

A pod-to-MSK call here is a few milliseconds, not about 0.5 s. The slow `JoinGroup` (8 s) is the consumer group
rejoining after the offset reset, and it is what produces the startup transient in the full-range graph.

## Caveats

- One run. Percentiles come from 60s windows with a 30s scrape interval.
- The first minute of the ramp is dominated by consumers rejoining their groups, not by steady-state behaviour.
- Transactional mode was NOT run on this infrastructure, so this run does not show what removing transactions bought.
- At about 5.6 aggregated rows/s a 100-row batch never fills; every batch is flushed by the 1s timer. The timer logic
  can take up to about 2x the interval in the worst case, which fits the ~2 s ceiling seen above.
