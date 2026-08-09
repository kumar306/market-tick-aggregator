#!/usr/bin/env bash
# Switches adapter between real exchanges and the mock exchange servers, by
# updating infra-config's feed URL overrides and restarting adapter - no
# per-service YAML regeneration or ConfigMap delete/recreate needed anymore.
#
# Usage: ./infra/scripts/toggle-mock-feeds.sh on|off
set -euo pipefail
cd "$(dirname "$0")/../.."
source infra/scripts/lib.sh

MODE="${1:-}"
if [ "$MODE" != "on" ] && [ "$MODE" != "off" ]; then
  echo "Usage: $0 <on|off>"
  exit 1
fi

MSK_BROKER=$(kubectl get configmap infra-config -o jsonpath='{.data.KAFKA_BOOTSTRAP_SERVERS}')

if [ "$MODE" == "on" ]; then
  echo "== pointing adapter at mock exchanges =="
  export BINANCE_WS_URL="ws://binance-mock.default.svc.cluster.local:8081/ws"
  export COINBASE_WS_URL="ws://coinbase-mock.default.svc.cluster.local:8081/ws"
  export KRAKEN_WS_URL="ws://kraken-mock.default.svc.cluster.local:8081/ws"
else
  echo "== pointing adapter back at real exchanges =="
  export BINANCE_WS_URL=""
  export COINBASE_WS_URL=""
  export KRAKEN_WS_URL=""
fi

apply_infra_config "$MSK_BROKER"
kubectl rollout restart deployment/adapter
kubectl rollout status deployment/adapter --timeout=120s
