#!/usr/bin/env bash
set -euo pipefail

POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_DB="${POSTGRES_DB:-marketdb}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"
DAYS_AHEAD="${DAYS_AHEAD:-14}"
DAYS_BACK="${DAYS_BACK:-1}"
INTERVAL_SECONDS="${INTERVAL_SECONDS:-21600}"

export PGPASSWORD="${POSTGRES_PASSWORD}"

ensure_partitions() {
  psql \
    -h "${POSTGRES_HOST}" \
    -p "${POSTGRES_PORT}" \
    -U "${POSTGRES_USER}" \
    -d "${POSTGRES_DB}" \
    -v ON_ERROR_STOP=1 <<SQL
WITH days AS (
  SELECT generate_series(
    (now() AT TIME ZONE 'UTC')::date - ${DAYS_BACK},
    (now() AT TIME ZONE 'UTC')::date + ${DAYS_AHEAD},
    interval '1 day'
  )::date AS d
)
SELECT format(
  'CREATE TABLE IF NOT EXISTS aggregated_ticks_%s PARTITION OF aggregated_ticks FOR VALUES FROM (%L) TO (%L);',
  to_char(d, 'YYYY_MM_DD'),
  to_char(d, 'YYYY-MM-DD') || ' 00:00:00+00',
  to_char(d + 1, 'YYYY-MM-DD') || ' 00:00:00+00'
)
FROM days
ORDER BY d;
\gexec

WITH days AS (
  SELECT generate_series(
    (now() AT TIME ZONE 'UTC')::date - ${DAYS_BACK},
    (now() AT TIME ZONE 'UTC')::date + ${DAYS_AHEAD},
    interval '1 day'
  )::date AS d
)
SELECT format(
  'CREATE TABLE IF NOT EXISTS orderbook_flushes_%s PARTITION OF orderbook_flushes FOR VALUES FROM (%L) TO (%L);',
  to_char(d, 'YYYY_MM_DD'),
  to_char(d, 'YYYY-MM-DD') || ' 00:00:00+00',
  to_char(d + 1, 'YYYY-MM-DD') || ' 00:00:00+00'
)
FROM days
ORDER BY d;
\gexec
SQL
}

echo "[partition-maintainer] started (interval_seconds=${INTERVAL_SECONDS})"
while true; do
  ensure_partitions
  sleep "${INTERVAL_SECONDS}"
done
