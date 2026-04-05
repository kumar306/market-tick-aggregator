# UI Backend

The UI backend is the serving layer between storage/streaming infrastructure and the browser dashboard.

## Responsibilities

- serve historical candles, metrics, and orderbook snapshots over REST
- consume aggregated Kafka topics for live tick and book updates
- fan live updates out to browser clients over WebSocket
- bridge Postgres-backed history and Kafka-backed real-time updates

## REST Endpoints

- `GET /api/candles`
- `GET /api/metrics`
- `GET /api/orderbook`
- `GET /ws`

## Internal Architecture

1. `main.go` loads config and database connection settings.
2. The repository layer reads historical rows from Postgres.
3. The service layer shapes those rows into API-friendly DTOs.
4. The Kafka consumer reads `aggregated.ticks` and `aggregated.book`.
5. The stream manager tracks active WebSocket subscribers by `exchange:symbol` key and broadcasts matching updates.

## Serving Model

The serving model is intentionally split by access pattern:

1. REST is used for historical slices and queryable ranges
2. WebSocket is used for live continuation

That means the frontend can load an initial historical window from Postgres, then continue from Kafka-backed live updates without trying to read Kafka directly from the browser.

## Why It Exists Separately

The UI should not talk directly to Kafka or Postgres:

- REST is better for historical slices and queryable lookback ranges
- WebSocket is better for live continuation
- the backend centralizes DTO shaping and subscription fanout

That keeps the frontend simpler and removes storage/Kafka concerns from the browser.

This separation also gives the repository a clean place to evolve query logic independently from the frontend component tree.

## Key Packages

- `repository/`: Postgres queries
- `service/`: response shaping and domain logic
- `controller/`: REST and WebSocket handlers
- `stream/`: Kafka consumer and WebSocket subscription manager
- `dto/`: frontend-facing payloads

## Testing

This module is relatively thin, so most of the value is in integration with the rest of the stack. Basic package compilation and service tests can be run with:

- repository and service behavior
- DTO shaping
- stream fanout plumbing
- package-level integration sanity

```bash
go test ./...
```
