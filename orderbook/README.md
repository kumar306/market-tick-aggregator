# Orderbook

The orderbook module consumes normalized book updates, maintains in-memory books per symbol, and periodically flushes top-of-book snapshots for persistence and UI streaming.

## Responsibilities

- consume `normalized.book`
- maintain deterministic book state per exchange/symbol
- build top-N flush payloads for downstream consumers
- persist lightweight snapshots to Redis for worker recovery
- coordinate offset commit only after safe flush progression

## Why It Exists Separately

Book processing has different state and performance characteristics from tick aggregation:

- larger and more bursty update volumes
- per-level upsert/delete behavior
- snapshot/recovery requirements
- top-of-book presentation requirements for the UI

Separating it keeps tick metrics and book state from interfering with each other.

## Internal Architecture

1. `main.go` initializes Kafka, Redis, workers, dispatcher, flush scheduler, and commit coordinator.
2. Each worker owns many exchange/symbol books in memory.
3. Book updates are applied to a skip-list-backed structure for efficient upsert, delete, best price, and top-N extraction.
4. Flush epochs produce `OrderbookFlush` messages for downstream persistence and live UI use.
5. Snapshots are periodically written to Redis and later restored if a worker restarts.

## Runtime Flow

For each normalized book event, the orderbook service:

1. routes the event to the worker that owns that exchange/symbol book
2. applies level inserts, updates, or deletes to the in-memory sides
3. keeps best bid, best ask, and top-N extraction cheap by maintaining ordered book state continuously
4. emits periodic flushes rather than writing every micro-update directly downstream

This is why the module exists independently from the tick aggregator. The data model, event shape, and recovery story are materially different.

## Data Structures

The in-memory book uses an ordered skip list abstraction rather than a balanced BST.

Why:

- fast insert/update/delete
- efficient top-N extraction
- simpler operational behavior for a high-update book path

Best bid/ask and top-N levels are derived from the maintained ordered sides rather than recomputed from scratch.

## Reliability Notes

- snapshots are gated against committed offsets so Redis backups do not get ahead of Kafka commit state
- backpressure is used to pause hot partitions when worker queues saturate
- book state is isolated per key for deterministic replay/application

The snapshot/commit relationship is especially important here. Orderbooks are mutable state machines, so the system has to avoid restoring a Redis snapshot that is logically ahead of the Kafka offsets the worker has durably committed.

## Key Packages

- `book/`: in-memory orderbook abstraction
- `orderedtree/`: skip list implementation
- `worker/`: book state, flush, snapshot, recovery
- `dispatcher/`: keyed dispatch to workers
- `flush/`: epoch scheduler
- `redis/`: snapshot storage and restore
- `kafka/`: consumer, producer, coordinator

## Testing

Representative tests cover:

- skip list ordering and top-N correctness
- worker update application
- snapshot gating and cleanup
- coordinator behavior
- dispatcher/backpressure paths

The tests in this module are mostly about state correctness:

- price ordering must remain correct after many inserts and deletes
- flush metadata must reflect the true best bid and best ask
- snapshot recovery must not move the book ahead of committed Kafka progress

Run:

```bash
go test ./...
```
