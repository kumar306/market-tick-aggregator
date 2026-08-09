#!/usr/bin/env bash
# End-to-end smoke test: brings up the full local stack against the mock
# exchanges, drives a small amount of real traffic through the whole
# pipeline, then asserts rows actually landed in Postgres for both the
# tick and orderbook paths. Meant to be run by hand after a change that
# touches the pipeline, not wired into CI - the full stack (Kafka,
# Postgres, Redis, 5 app services, 3 mock exchanges) is too heavy for a
# shared CI runner to run reliably on every push.
#
# Usage: ./scripts/integration-smoke-test.sh
set -euo pipefail
cd "$(dirname "$0")/.."

COMPOSE="docker compose -f docker-compose.yml -f docker-compose.app.yml -f docker-compose.mock.yml"

echo "== bringing up the stack =="
$COMPOSE up -d --build \
  zookeeper kafka kafka-init redis postgres postgres-init prometheus \
  binance-mock coinbase-mock kraken-mock \
  adapter normalizer aggregator orderbook persistence

echo "== waiting for pipeline services to settle =="
sleep 15

echo "== driving a small amount of traffic through all three mocks =="
curl -s "http://localhost:8081/rate?value=50" >/dev/null
curl -s "http://localhost:8082/rate?value=50" >/dev/null
curl -s "http://localhost:8083/rate?value=50" >/dev/null

# persistence batches on a timer (30s for ticks, 50s for orderbook flushes -
# see persistence/config/docker.config.yaml), so this has to outlast the
# slower of the two plus some margin for upstream propagation.
echo "== waiting 75s for traffic to flow and persistence to flush both batchers =="
sleep 75

echo "== asserting rows landed in postgres =="
TICK_COUNT=$(docker exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM aggregated_ticks;"')
BOOK_COUNT=$(docker exec postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "SELECT count(*) FROM orderbook_flushes;"')

echo "aggregated_ticks rows:    $TICK_COUNT"
echo "orderbook_flushes rows:   $BOOK_COUNT"

FAILED=0
if [ "$TICK_COUNT" -eq 0 ]; then
  echo "FAIL: no rows in aggregated_ticks - tick path did not complete end to end"
  FAILED=1
fi
if [ "$BOOK_COUNT" -eq 0 ]; then
  echo "FAIL: no rows in orderbook_flushes - book path did not complete end to end"
  FAILED=1
fi

if [ "$FAILED" -eq 0 ]; then
  echo "PASS: both the tick path and the book path made it from mock exchange to Postgres"
fi

echo
echo "Stack is still running for inspection. Tear it down with:"
echo "  $COMPOSE down"

exit $FAILED
