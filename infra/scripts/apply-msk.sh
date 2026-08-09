#!/usr/bin/env bash
# Recovery path for when only MSK was torn down (destroy-msk.sh) to save cost
# overnight, while EKS/RDS/ElastiCache and the k8s objects kept running.
# Brings MSK back, points every pod at the new broker via infra-config (no
# per-service ConfigMap delete/recreate needed), recreates topics on the
# fresh cluster, and restarts the affected deployments.
#
# If you were in mock-exchange mode before tearing MSK down and want to stay
# there, export BINANCE_WS_URL / COINBASE_WS_URL / KRAKEN_WS_URL before
# running this - otherwise adapter reverts to real exchanges by default.
set -euo pipefail
cd "$(dirname "$0")/../.."
source infra/scripts/lib.sh

echo "== terraform apply (MSK only) =="
cd infra/terraform
terraform apply -auto-approve \
  -target=aws_msk_serverless_cluster.this \
  -target=aws_security_group.msk_sg \
  -target=aws_iam_policy.msk_client \
  -target=aws_iam_role.msk_irsa \
  -target=aws_iam_role_policy_attachment.msk_irsa
MSK_BROKER=$(terraform output -raw msk_bootstrap_brokers)
cd ../..

echo "== updating infra-config with the new broker address =="
apply_infra_config "$MSK_BROKER"

echo "== recreating topics on the fresh cluster =="
kubectl delete job bootstrap-topics --ignore-not-found
kubectl apply -f infra/k8s/bootstrap-topics-job.yaml
kubectl wait --for=condition=complete --timeout=60s job/bootstrap-topics

echo "== restarting services (and mocks, if scaled down) =="
kubectl scale deployment adapter normalizer aggregator orderbook persistence --replicas=1
kubectl scale deployment binance-mock coinbase-mock kraken-mock --replicas=1 2>/dev/null || true
for svc in adapter normalizer aggregator orderbook persistence; do
  kubectl rollout restart deployment/$svc
  kubectl rollout status deployment/$svc --timeout=120s
done

echo "== done =="
kubectl get pods
