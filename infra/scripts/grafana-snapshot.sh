#!/usr/bin/env bash
# Uploads infra/grafana/e2e-dashboard.json to the cluster's Grafana and saves a screenshot of it, the way the
# images in docs/ were taken (a real Grafana page, header and time range included).
#
# Usage:
#   ./infra/scripts/grafana-snapshot.sh <from_epoch_ms> <to_epoch_ms> <out.png> [dashboard.json]
# dashboard.json defaults to infra/grafana/e2e-dashboard.json; others: hops-dashboard.json, persistence-dashboard.json
#
# Needs: kubectl + helm pointed at the cluster, curl, Node 22+, and Microsoft Edge or Chrome.
# The port-forward is local-only. Anonymous Viewer access is switched on so the headless browser needs no login;
# it only exists for the life of the cluster and is never exposed outside this machine.
set -euo pipefail
cd "$(dirname "$0")/../.."

FROM="${1:?from epoch ms}"; TO="${2:?to epoch ms}"; OUT="${3:?output png}"
DASH="${4:-infra/grafana/e2e-dashboard.json}"
case "$OUT" in /*|?:*) ;; *) OUT="$PWD/$OUT" ;; esac   # an absolute path: the browser resolves a relative one against its own cwd
PORT=3000
BASE="http://localhost:${PORT}"

browser() {
  for b in "/c/Program Files (x86)/Microsoft/Edge/Application/msedge.exe" \
           "/c/Program Files/Google/Chrome/Application/chrome.exe"; do
    [ -x "$b" ] && { echo "$b"; return; }
  done
  echo "no Edge/Chrome found" >&2; exit 1
}

pf_pid=""
stop_pf() { [ -n "$pf_pid" ] && kill "$pf_pid" 2>/dev/null || true; }
trap stop_pf EXIT
start_pf() {
  stop_pf
  kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana ${PORT}:80 >/dev/null 2>&1 &
  pf_pid=$!
  for _ in $(seq 1 30); do curl -s -o /dev/null "$BASE/api/health" && return; sleep 1; done
  echo "grafana port-forward never became ready" >&2; exit 1
}

PASS=$(kubectl get secret -n monitoring kube-prometheus-stack-grafana -o jsonpath="{.data.admin-password}" | base64 -d)

start_pf
code=$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/search")
if [ "$code" != "200" ]; then
  echo "== enabling anonymous Viewer on Grafana (restarts the pod) =="
  cat > /tmp/grafana-anon.yaml <<'YAML'
grafana:
  grafana.ini:
    auth.anonymous:
      enabled: true
      org_role: Viewer
YAML
  helm upgrade kube-prometheus-stack prometheus-community/kube-prometheus-stack -n monitoring \
    --reuse-values -f /tmp/grafana-anon.yaml >/dev/null
  kubectl rollout status deployment/kube-prometheus-stack-grafana -n monitoring --timeout=180s
  start_pf
fi

echo "== uploading dashboard =="
python - <<PY > /tmp/grafana-dash-body.json
import json
d = json.load(open("${DASH}"))
print(json.dumps({"dashboard": d, "overwrite": True}))
PY
curl -s -u "admin:${PASS}" -H 'Content-Type: application/json' -X POST "$BASE/api/dashboards/db" -d @/tmp/grafana-dash-body.json
echo

UID_=$(python -c "import json;print(json.load(open('${DASH}'))['uid'])")
URL="${BASE}/d/${UID_}?orgId=1&from=${FROM}&to=${TO}"
echo "== screenshot =="
# DevTools capture with a real wait: the plain --screenshot flag fires at page load, before the panels have drawn
node infra/scripts/cdp-screenshot.mjs "$(browser)" "$URL" "$(cygpath -w "$OUT")" 30
ls -la "$OUT"
