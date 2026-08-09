#!/usr/bin/env bash
# Local equivalent of infra/k8s/loadtest-ramp-job.yaml - ramps the three local
# mock exchanges through the same rate levels used in the AWS load test, so
# orderbook_e2e_latency_seconds (and friends) can be compared apples-to-apples
# against localhost, where there's no MSK/cross-AZ network to blame.
#
# Usage: ./scripts/local-ramp.sh
set -euo pipefail

STEP=60
for TOTAL in 500 1000 2500 5000 10000; do
  PER_STREAM=$((TOTAL / 6))
  echo "$(date +%Y-%m-%dT%H:%M:%S%z) level start: total=${TOTAL}/s per-stream=${PER_STREAM}/s"
  curl -s "http://localhost:8081/rate?value=${PER_STREAM}"
  curl -s "http://localhost:8082/rate?value=${PER_STREAM}"
  curl -s "http://localhost:8083/rate?value=${PER_STREAM}"
  sleep "$STEP"
done
echo "$(date +%Y-%m-%dT%H:%M:%S%z) ramp complete - last level (10000/s total) keeps running until the mock containers are stopped"
