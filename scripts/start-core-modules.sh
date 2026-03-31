#!/usr/bin/env bash
set -euo pipefail

# orchestration script which creates postgres partitions and daily partition creation script
# kafka topic creation is done via init-kafka-topics.sh which is called by docker container process
# copies the init kafka topics sh to the container, then executes inside container

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
START_DELAY_SECONDS="${START_DELAY_SECONDS:-2}"
AUTO_PARTITION_MAINTAINER="${AUTO_PARTITION_MAINTAINER:-1}"

# starting downstream first, then upstream at last. in the correct order
SCRIPTS=(
  "run-ui-backend.sh"
  "run-ui.sh"
  "run-persistence.sh"
  "run-aggregator.sh"
  "run-orderbook.sh"
  "run-normalizer.sh"
  "run-adapter.sh"
)

pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait || true
}
trap cleanup INT TERM EXIT

bash "${ROOT}/scripts/ensure-postgres-partitions.sh"

if [ "${AUTO_PARTITION_MAINTAINER}" = "1" ]; then
  echo "[start] launching partition maintainer"
  bash "${ROOT}/scripts/partition-maintainer.sh" &
  pids+=("$!")
fi

for script in "${SCRIPTS[@]}"; do
  echo "[start] launching ${script}"
  bash "${ROOT}/scripts/${script}" &
  pid=$!
  pids+=("$pid")
  echo "[start] pid=${pid} script=${script}"
  sleep "${START_DELAY_SECONDS}"
done

wait
