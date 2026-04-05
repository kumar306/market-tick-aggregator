# Persistence

The persistence module is the durable sink of the pipeline. It consumes aggregated Kafka topics, batches records, writes them to Postgres inside transactions, and commits Kafka offsets only after successful writes.

## Responsibilities

- consume `aggregated.ticks` and `aggregated.book`
- convert protobuf payloads into database row models
- batch writes for better fsync and transaction efficiency
- commit Kafka offsets only after durable DB write
- route malformed records to a DLQ

## Inputs and Outputs

Inputs:

- `aggregated.ticks`
- `aggregated.book`

Outputs:

- Postgres tables for aggregated ticks and orderbook flushes
- DLQ entries in `persistence.dlq` for bad records

## Internal Architecture

1. `main.go` loads config, initializes Kafka and Postgres, and wires the tick/book pipelines.
2. Each pipeline has:
   - a converter
   - a batcher
   - a flush callback
   - an invalid-record predicate
3. The batcher groups records by size or time threshold.
4. Flush callbacks write the batch to Postgres inside a transaction.
5. Kafka offsets are committed only after the write succeeds.

## Runtime Flow

Persistence is intentionally the simplest part of the pipeline:

1. consume aggregated Kafka records
2. convert them into DB row models
3. batch them by size or time threshold
4. write the batch in a transaction
5. commit offsets only after the transaction succeeds

That gives the service a clear durability boundary and makes replay behavior straightforward to explain.

## Why This Design

This module is intentionally simple and synchronous.

Benefits:

- clear durability boundary
- straightforward replay behavior
- easy reasoning about commit-after-write semantics
- swap-friendly flush path if the backing database changes later

This is also why malformed records are routed to a DLQ instead of trying to keep partially valid writes mixed into the main transactional path.

## Partitioning

The Postgres tables are range-partitioned by time:

- `aggregated_ticks` partitioned by `start_ts`
- `orderbook_flushes` partitioned by `event_time`

Containerized deployments include bootstrap and maintainer services so the needed daily partitions exist before persistence writes begin.

## Key Packages

- `batcher/`: size/time-based batching
- `converter/`: protobuf -> DB model conversion
- `db/`: Postgres initialization and schema support
- `db/writer/`: transactional flush logic
- `pipeline/`: pipeline abstraction for tick/book streams
- `kafka/`: consumer, lag metrics, DLQ, offset commit

## Testing

Representative tests cover:

- batcher behavior
- config loading
- converters
- DB initialization and writer behavior
- pipeline operation
- Kafka commit-map helpers

The most important tests here are the durability-path tests:

- batch boundaries
- transaction write behavior
- offset bookkeeping after success
- invalid-record handling and DLQ routing

Run:

```bash
go test ./...
```
