#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/../.."

OUT_DIR="pprof-captures/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$OUT_DIR"

taskkill //F //IM kubectl.exe //T 2>/dev/null || true
sleep 1

for svc_port in adapter:2112 aggregator:2114 orderbook:2115 normalizer:2113 persistence:2116; do
  svc="${svc_port%%:*}"; port="${svc_port##*:}"
  echo "== capturing $svc =="

  kubectl port-forward deploy/$svc $port:$port &
  PF=$!

  ready=false
  for i in $(seq 1 15); do
    if curl -s -o /dev/null "http://localhost:$port/debug/pprof/"; then
      ready=true
      break
    fi
    sleep 1
  done

  if [ "$ready" = true ]; then
    go tool pprof -top -cum "http://localhost:$port/debug/pprof/heap" > "$OUT_DIR/$svc-heap-top.txt" \
      || echo "WARN: heap capture failed for $svc"
    go tool pprof -top -cum "http://localhost:$port/debug/pprof/profile?seconds=30" > "$OUT_DIR/$svc-cpu-top.txt" \
      || echo "WARN: cpu capture failed for $svc"
  else
    echo "WARN: $svc port-forward never became ready, skipping"
  fi

  kill $PF 2>/dev/null || true
  wait $PF 2>/dev/null || true
  sleep 2
done

echo "== saved to $OUT_DIR =="
