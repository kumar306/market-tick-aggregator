#!/usr/bin/env bash
# Deploys the mock exchanges, switches adapter to them, runs the rate ramp,
# and waits for it to finish. Assumes apply-all.sh has already been run.
#
# Does NOT tear anything down or switch adapter back automatically - do that
# deliberately once you're done capturing pprof/Grafana evidence:
#   ./infra/scripts/toggle-mock-feeds.sh off
set -euo pipefail
cd "$(dirname "$0")/../.."
source infra/scripts/lib.sh

TAG=$(current_image_tag)

echo "== scaling consumers to 0 so their groups go empty =="
kubectl scale deployment/normalizer deployment/aggregator deployment/orderbook deployment/persistence --replicas=0
kubectl wait --for=delete pod --selector='app in (normalizer,aggregator,orderbook,persistence)' --timeout=120s

echo "== resetting all pipeline consumer groups to latest (tag $TAG) =="
kubectl delete job reset-offsets --ignore-not-found
apply_with_current_tag infra/k8s/reset-offsets-job.yaml "$TAG"
kubectl wait --for=condition=complete --timeout=60s job/reset-offsets
kubectl logs job/reset-offsets

echo "== scaling consumers back up =="
kubectl scale deployment/normalizer deployment/aggregator deployment/orderbook deployment/persistence --replicas=1
kubectl rollout status deployment/normalizer --timeout=120s
kubectl rollout status deployment/aggregator --timeout=120s
kubectl rollout status deployment/orderbook --timeout=120s
kubectl rollout status deployment/persistence --timeout=120s

echo "== deploying mock exchanges (tag $TAG) =="
deploy_mocks "$TAG"

echo "== switching adapter to mock exchanges =="
./infra/scripts/toggle-mock-feeds.sh on

echo "== running the rate ramp =="
kubectl delete job loadtest-ramp --ignore-not-found
kubectl apply -f infra/k8s/loadtest-ramp-job.yaml
kubectl wait --for=condition=complete --timeout=600s job/loadtest-ramp
kubectl logs job/loadtest-ramp

echo "== ramp complete. Traffic keeps flowing at the last level until you"
echo "   run toggle-mock-feeds.sh off or scale the mocks to 0. =="
