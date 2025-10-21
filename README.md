Big-picture plan (phases)

Finish Adapter MVP (Kafka integration, ring buffer, config, basic metrics) — core foundation.

Normalizer + Aggregator (dedupe, out-of-order handling, aggregations like VWAP/ohlc) — data correctness & value.

Storage & Pipelines (persist raw ticks to S3, aggregated results to OLAP/ClickHouse) — durability & analytics.

Orderbook service & gRPC API + Redis (snapshots, low-latency read) — UI data & low latency paths.

Alerts + Monitoring (Prometheus rules, Grafana dashboards).

UI (React) (charts, live ticks, historical queries).

Strategy Backtester (C++ core + orchestrator + UI).

Polish / Hardening / CI / docs (tests, deployment, readmes, demo).