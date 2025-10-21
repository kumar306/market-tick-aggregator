✅ Adapter SDE-2 Readiness Checklist
A. Core Reliability (Must-Have)

 Each feed runs in an isolated goroutine with supervision & auto-reconnect

 Exponential backoff (initial <1s, max ~30s, jittered) on reconnect

 Graceful shutdown: context cancellation, stop reads, flush Kafka, exit cleanly

 Bounded channels for incoming ticks — no unbounded queues

 Backpressure: producer blocks or throttles when queue full

 Structured logging (INFO lifecycle, ERROR failures)

 Prometheus metrics for connection state, publish latency, tick rate

 Config-driven (env or YAML: brokers, symbols, intervals)

B. Scalability / Throughput (Should-Have)

 Kafka partition key by symbol → ordered per symbol, parallelizable

 Configurable parallelism: N feeds, adjustable by config/env

 Bounded Kafka producer queue (avoid unbounded memory use)

 Stateless process: horizontal scaling by replicas is safe

 Global semaphore / throttling to prevent overload under high tick rate

C. Observability & Operations (Must-Know, Nice-to-Have)

 Metrics: feed_connected, feed_reconnects, kafka_publish_latency_ms, ticks_dropped_total

 Structured logs include feed, symbol, seq, latency_ms

 Alert thresholds: feed down >30s, publish errors >X/sec, tick drops >0

 README includes runbook: restart steps, debug metrics, replay plan

 Optional: tracing spans around parse→publish path (OpenTelemetry)

D. Data Integrity (Strong-Plus)

 Messages carry source_seq, source_ts, recv_ts

 Kafka acks=all, retry policy configured

 Optional fallback: WAL (write-ahead log) for Kafka downtime

 Document at-least-once semantics and downstream dedupe plan

 Each symbol’s ticks strictly ordered (via Kafka key)

E. Testing & Validation

 Unit tests: parser correctness, producer mock, reconnect logic

 Integration test: adapter + Kafka docker-compose, verifies publish flow

 Chaos test: simulate feed disconnect, verify auto-reconnect

 Load test: sustained tick rate (e.g., 50k/s) without data loss

 Documentation: metrics explained, assumptions & trade-offs noted

F. Design Hygiene

 Clear interfaces (Feed, Publisher) and constructor functions

 No hardcoded values; all injected or configurable

 Packages follow clean boundaries (internal/adapter, pkg/kafka, etc.)

 Code passes golangci-lint, go fmt, go vet

 Comment headers explain module purpose & lifecycle

🎯 Passing Bar Summary

To reach SDE-2 readiness:

✅ Implement all A + B + E + F

🧠 Be able to explain / justify the C & D items (even if marked TODO)

📈 Include a short “future enhancements” section mentioning WAL fallback, autoscaling, tracing