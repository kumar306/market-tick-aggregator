#!/usr/bin/env bash
set -euo pipefail

BROKER="${KAFKA_BOOTSTRAP_SERVER:-kafka:9093}"
# creating the topics with 6 partitions each and replication factor 1
TOPICS=(
  "binance.raw.ticks:6:1"
  "binance.raw.level2:6:1"
  "coinbase.raw.ticks:6:1"
  "coinbase.raw.level2:6:1"
  "kraken.raw.ticks:6:1"
  "kraken.raw.book:6:1"
  "normalized.ticks:6:1"
  "normalized.book:6:1"
  "aggregated.ticks:6:1"
  "aggregated.book:6:1"
)

echo "Waiting for Kafka at ${BROKER}..."
for i in {1..60}; do
  if kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; then
    echo "Kafka is ready."
    break
  fi
  sleep 2
done

if ! kafka-topics --bootstrap-server "${BROKER}" --list >/dev/null 2>&1; then
  echo "Kafka is not reachable at ${BROKER}" >&2
  exit 1
fi

for spec in "${TOPICS[@]}"; do
  IFS=':' read -r topic partitions replication_factor <<<"${spec}"
  kafka-topics --bootstrap-server "${BROKER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${replication_factor}"
done

echo "Kafka topics available:"
kafka-topics --bootstrap-server "${BROKER}" --list | sort