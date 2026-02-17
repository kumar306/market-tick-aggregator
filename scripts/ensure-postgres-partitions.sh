#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ -f "${ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT}/.env"
  set +a
fi

# Handle CRLF .env files from Windows editors.
POSTGRES_USER="${POSTGRES_USER//$'\r'/}"
POSTGRES_DB="${POSTGRES_DB//$'\r'/}"

DAYS_AHEAD="${DAYS_AHEAD:-14}"
DAYS_BACK="${DAYS_BACK:-1}"

if ! [[ "${DAYS_AHEAD}" =~ ^[0-9]+$ ]]; then
  echo "DAYS_AHEAD must be a non-negative integer (got: ${DAYS_AHEAD})" >&2
  exit 1
fi

if ! [[ "${DAYS_BACK}" =~ ^[0-9]+$ ]]; then
  echo "DAYS_BACK must be a non-negative integer (got: ${DAYS_BACK})" >&2
  exit 1
fi

if [ -z "${POSTGRES_USER:-}" ] || [ -z "${POSTGRES_DB:-}" ]; then
  echo "POSTGRES_USER and POSTGRES_DB must be set (usually from .env)" >&2
  exit 1
fi

echo "[partitions] ensuring postgres partitions (days_back=${DAYS_BACK}, days_ahead=${DAYS_AHEAD})"

docker compose exec -T postgres psql \
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

SELECT inhrelid::regclass::text AS partition_name
FROM pg_inherits
WHERE inhparent IN ('aggregated_ticks'::regclass, 'orderbook_flushes'::regclass)
ORDER BY partition_name;
SQL

echo "[partitions] done"
