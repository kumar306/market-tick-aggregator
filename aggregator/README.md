# Aggregator

The aggregator consumes normalized ticks and computes windowed OHLC and derived market metrics across many time horizons.

## Responsibilities

- consume `normalized.ticks`
- maintain per-symbol state across multiple windows
- compute tumbling and rolling metrics
- flush aggregated results into `aggregated.ticks`
- preserve deterministic per-key updates by routing each stream to one worker

## Metrics

Representative metrics include:

- OHLC
- VWAP
- TWAP
- rolling VWAP
- volume
- rolling volume
- volume acceleration
- volatility
- ATR
- EMA
- SMA
- log return
- simple return
- microprice

## Windows

The default configuration includes windows from:

- `5s`
- `10s`
- `30s`
- `1m`
- `2m`
- `5m`
- `10m`
- `30m`
- `1h`
- `2h`
- `6h`
- `12h`
- `24h`

## Internal Architecture

1. `main.go` loads config, initializes Redis dedupe and Kafka, starts workers, schedulers, dispatcher, and consumer.
2. The dispatcher routes records by symbol key so all windows for a symbol live on the same worker.
3. Workers hold long-lived window state for many symbols.
4. Each window owns a set of metric implementations.
5. On tick arrival, every relevant metric for every relevant window is updated.
6. On flush, each window snapshots its metrics into an `AggregatedTick` and resets only the tumbling metrics.

## Runtime Flow

For each normalized tick, the aggregator does three things:

1. identify the symbol stream key
2. update every relevant window for that stream
3. let the window scheduler flush snapshots on its configured cadence

That means the aggregator is stateful by design. It is not a stateless transform over Kafka records; it is a long-lived in-memory metric engine that accumulates context between events.

## Design Approach

The window abstraction is intentionally simple:

- the window owns timing and flush lifecycle
- the metric owns update/apply/reset behavior

That separation makes the metric engine extensible. Adding a new metric does not require rewriting the worker or the window scheduler.

It also makes reasoning about correctness easier:

- flush timing is centralized in the window
- metric math is localized to the metric implementation
- worker ownership keeps stream state deterministic for a symbol

## Tradeoffs

- per-stream keyed state is favored over maximal parallel fanout
- some metrics are approximate streaming constructs rather than strict source-of-truth records
- the system is designed to degrade safely under load rather than guarantee lossless flush of every intermediate metric state

The practical consequence is that this module optimizes for stable, explainable rolling analytics rather than perfect historical reconstruction of every intermediate market microstate.

## Key Packages

- `internal/aggmetrics/`: metric implementations
- `worker/`: window state and per-symbol processing
- `dispatcher/`: keyed dispatch
- `flush/`: flush schedulers
- `kafka/`: consumer, producer, commit loop
- `dedupe/`: Redis-backed duplicate handling

## Testing

Representative tests cover:

- dispatcher routing
- worker window creation
- metric implementations
- flush behavior
- reset behavior for tumbling metrics

The test surface is strongest where bugs would be hardest to detect visually:

- metric update/reset semantics
- window creation and reuse
- flush correctness
- deterministic routing of per-symbol state to the same worker

Run:

```bash
go test ./...
```
