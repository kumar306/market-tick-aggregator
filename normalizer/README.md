# Normalizer

The normalizer converts raw exchange-specific messages into shared tick and book schemas that the rest of the pipeline can process consistently.

## Responsibilities

- consume raw Kafka topics from the adapter
- convert exchange-specific payloads into shared protobuf-backed message types
- preserve ordered processing per stream key
- apply bounded buffering and backpressure
- protect downstream publishing with a circuit breaker and WAL fallback
- publish normalized ticks and books into Kafka

## Inputs and Outputs

Inputs:

- `binance.raw.ticks`
- `binance.raw.level2`
- `coinbase.raw.ticks`
- `coinbase.raw.level2`
- `kraken.raw.ticks`
- `kraken.raw.book`

Outputs:

- `normalized.ticks`
- `normalized.book`

## Internal Architecture

1. `main.go` loads config, initializes registries, Redis dedupe, WAL, consumer, dispatcher, workers, and offset committer.
2. `factory/registry` wires exchange-specific converter, orderer, normalizer, and publisher strategies.
3. The dispatcher hashes records by stream identity so related records stay on the same worker.
4. Workers apply ordering semantics and emit normalized protobuf payloads downstream.
5. The WAL and circuit breaker provide a degraded-but-survivable path if Kafka publishing starts failing.

## Runtime Flow

The normalizer is the first place where the pipeline begins to impose an internal contract.

1. consume raw Kafka records
2. identify the exchange, channel, symbol, and ordering semantics for the record
3. route the record to a deterministic worker based on stream identity
4. convert the raw payload into a shared tick or book message
5. publish the normalized protobuf to downstream Kafka topics

This gives the rest of the system a stable event shape without requiring the aggregator or orderbook layers to understand exchange-specific wire formats.

## Why This Layer Exists

Without this module, downstream services would each need to understand:

- different exchange payload shapes
- different sequence/timestamp semantics
- different control message types
- symbol-format differences
- tick versus book event contracts

The normalizer turns those into one internal contract so the aggregator and orderbook modules do not need exchange-specific branching everywhere.

## Reliability and Control

This module contains the main stream-ingestion resilience logic:

- per-key ordering
- dedupe via Redis
- bounded worker queues
- partition pause/resume backpressure
- WAL-backed spillover path when downstream publish is unhealthy

The normalizer is also where the project makes an explicit engineering tradeoff: it prefers controlled degradation over pretending the pipeline is infinitely elastic. If downstream publish health degrades, this layer is allowed to shed, pause, or spool work rather than letting memory grow without bound.

## Key Packages

- `factory/`: converter/orderer/normalizer/publisher strategies
- `dispatcher/`: keyed dispatch to workers
- `worker/`: ordering and downstream emission
- `kafka/`: consumer, producer, commit loop, breaker, WAL
- `dedupe/`: Redis-based duplicate protection
- `backpressure/`: partition pause/resume controller

## Testing

Representative tests cover:

- backpressure controller behavior
- dedupe logic
- exchange normalizer correctness
- Kafka plumbing
- worker processing

The most valuable tests here are the ones that protect invariants:

- records for the same key remain ordered
- invalid or duplicate records do not corrupt downstream flow
- exchange-specific converters produce the shared schema expected by later services
- backpressure decisions happen before workers grow unbounded queues

Run:

```bash
go test ./...
```
