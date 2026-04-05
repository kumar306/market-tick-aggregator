# Adapter

The adapter is the ingestion edge of the system. It connects to exchange WebSocket feeds, handles exchange-specific subscribe/control behavior, and publishes raw events into Kafka topics.

## Responsibilities

- establish and maintain exchange WebSocket connections
- subscribe to configured symbols and channels
- handle reconnect, heartbeat, pong timeout, and retry behavior
- normalize only enough to extract the partition key and preserve the raw payload
- publish raw exchange messages into Kafka

## Inputs and Outputs

Inputs:

- Binance WebSocket feeds
- Coinbase WebSocket feeds
- Kraken WebSocket feeds

Outputs:

- `binance.raw.ticks`
- `binance.raw.level2`
- `coinbase.raw.ticks`
- `coinbase.raw.level2`
- `kraken.raw.ticks`
- `kraken.raw.book`

## Internal Architecture

The adapter is intentionally lightweight.

1. `main.go` loads feed configuration and starts one supervisor per configured stream.
2. Each supervisor owns connection lifecycle for one exchange/channel/symbol set.
3. Exchange-specific `Subscriber`, `Normalizer`, and `Pinger` implementations live under `feeds/`.
4. Messages flow through a ring buffer before being published to Kafka.

This keeps exchange protocol details isolated from the rest of the system.

## Runtime Flow

For each configured feed, the adapter follows a simple lifecycle:

1. connect to the exchange WebSocket endpoint
2. send the exchange-specific subscribe payload
3. receive raw frames and filter control-only messages when needed
4. extract only the routing identity needed for Kafka partitioning
5. publish the untouched raw payload into the exchange-specific raw topic

The key design decision is that the adapter stops short of full downstream normalization. That keeps this layer focused on connectivity and raw event capture rather than schema ownership.

## Design Notes

- Coinbase orderbook is wired through `level2_batch` so the feed can be consumed without websocket authentication while still mapping into the downstream logical level-2 flow.
- Kraken control frames such as heartbeat and subscribe acknowledgements are skipped before entering the shared normalize/publish path.
- The adapter does not attempt to impose a full downstream schema. It is only responsible for stable raw ingestion.

## Failure Model

This module is designed to fail and recover at the edge instead of pushing connection concerns deeper into the stack.

- reconnects are owned by the supervisor
- heartbeat and pong timeout handling stay exchange-local
- bounded ring buffers absorb short bursts without letting one slow publish path grow unbounded
- Kafka publishing failures are surfaced here, but schema-specific recovery is intentionally deferred to downstream modules

That split makes the adapter easier to reason about: connectivity problems stay in the adapter, schema and ordering problems start in the normalizer.

## Key Packages

- `feeds/`: exchange-specific stream handling
- `internal/`: supervisors, retry, connection management, publishing loop
- `kafka/`: raw Kafka producer
- `ring/`: drop-oldest SPSC ring buffer

## Testing

Representative test coverage includes:

- config validation
- feed normalization helpers
- Kraken control-frame skipping
- Kafka producer compile/integration paths
- connection/retry helper behavior

The most important tests in this module are not about business logic; they are about protecting the ingestion boundary:

- ensuring exchange-specific control frames do not leak into raw market topics
- ensuring feed config is parsed and validated correctly
- ensuring publish/retry wiring compiles and behaves sanely under mocked failure paths

Run:

```bash
go test ./...
```
