#!/usr/bin/env bash
# Prints the Grafana admin password, then port-forwards Grafana to localhost.
# Ctrl+C to stop the port-forward when you're done.
set -euo pipefail

echo "== Grafana admin password =="
kubectl get secret -n monitoring kube-prometheus-stack-grafana -o jsonpath="{.data.admin-password}" | base64 -d
echo
echo "== opening port-forward on http://localhost:3000 (Ctrl+C to stop) =="
kubectl port-forward -n monitoring svc/kube-prometheus-stack-grafana 3000:80
