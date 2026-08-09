#!/usr/bin/env bash
# Targeted teardown: only MSK + its IAM/IRSA wiring. EKS/RDS/ElastiCache and
# everything deployed on them stays up. Use this for a short pause (e.g.
# overnight) where you don't want MSK's usage-based billing running but
# don't want to lose the rest of the cluster state either.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "== scaling down Kafka-touching workloads (quiet while MSK is gone) =="
kubectl scale deployment adapter normalizer aggregator orderbook persistence --replicas=0 2>/dev/null || true
kubectl scale deployment binance-mock coinbase-mock kraken-mock --replicas=0 2>/dev/null || true

echo "== terraform destroy (MSK only) =="
cd infra/terraform
terraform destroy -auto-approve \
  -target=aws_msk_serverless_cluster.this \
  -target=aws_security_group.msk_sg \
  -target=aws_iam_role_policy_attachment.msk_irsa \
  -target=aws_iam_role.msk_irsa \
  -target=aws_iam_policy.msk_client

echo "== done. Bring it back with infra/scripts/apply-msk.sh =="
