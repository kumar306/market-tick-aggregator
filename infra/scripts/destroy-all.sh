#!/usr/bin/env bash
# Full teardown: everything except ECR (images) and Terraform config itself.
# ui/ui-backend aren't deployed by apply-all.sh right now, so there's no
# LoadBalancer to orphan by default - the delete below is just a safety net
# in case infra/k8s/ui.yaml was applied manually at some point.
set -euo pipefail
cd "$(dirname "$0")/../.."

echo "== deleting any LoadBalancer Services (ui/ui-backend), if present =="
kubectl delete svc ui ui-backend --ignore-not-found

echo "== terraform destroy (everything except ECR) =="
cd infra/terraform
terraform destroy -auto-approve \
  -target=module.eks \
  -target=module.vpc \
  -target=aws_msk_serverless_cluster.this \
  -target=aws_security_group.msk_sg \
  -target=aws_iam_role_policy_attachment.msk_irsa \
  -target=aws_iam_role.msk_irsa \
  -target=aws_iam_policy.msk_client \
  -target=aws_db_instance.postgres \
  -target=aws_db_subnet_group.postgres \
  -target=aws_security_group.postgres \
  -target=random_password.postgres \
  -target=aws_elasticache_cluster.redis \
  -target=aws_elasticache_subnet_group.redis \
  -target=aws_security_group.redis

echo "== done. Bring it all back with infra/scripts/apply-all.sh =="
