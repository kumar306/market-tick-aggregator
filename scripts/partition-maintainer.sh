#!/usr/bin/env bash
set -euo pipefail
# this script will be running and creating partitions 14 days in advance on a daily basis
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-21600}"

if ! [[ "${INTERVAL_SECONDS}" =~ ^[0-9]+$ ]]; then
  echo "INTERVAL_SECONDS must be a non-negative integer (got: ${INTERVAL_SECONDS})" >&2
  exit 1
fi

echo "[partition-maintainer] started (interval_seconds=${INTERVAL_SECONDS})"
while true; do
  bash "${ROOT}/scripts/ensure-postgres-partitions.sh"
  sleep "${INTERVAL_SECONDS}"
done
