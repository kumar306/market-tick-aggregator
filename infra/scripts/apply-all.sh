#!/usr/bin/env bash
# Full fresh spin-up: terraform apply, then every k8s object needed for the
# 5 pipeline services (adapter/normalizer/aggregator/orderbook/persistence).
# ui/ui-backend are NOT deployed here - see infra/k8s/ui.yaml.
#
# Usage:
#   ./infra/scripts/apply-all.sh              reuse the current image tag, no rebuild
#   ./infra/scripts/apply-all.sh --rebuild     bump the tag by one and rebuild/push all images
set -euo pipefail
cd "$(dirname "$0")/../.."
source infra/scripts/lib.sh

if [ "${1:-}" == "--rebuild" ]; then
  TAG=$(bump_image_tag)
  echo "== bumped image tag to $TAG =="
else
  TAG=$(current_image_tag)
  echo "== reusing image tag $TAG (pass --rebuild to force a new build) =="
fi

echo "== terraform apply =="
cd infra/terraform
terraform apply -var image_tag="$TAG" -auto-approve
MSK_BROKER=$(terraform output -raw msk_bootstrap_brokers)
RDS_HOST=$(terraform output -raw postgres_endpoint | cut -d: -f1)
REDIS_HOST=$(terraform output -raw redis_endpoint)
PG_PASSWORD=$(terraform output -raw postgres_password)
cd ../..

echo "== kubeconfig =="
aws eks update-kubeconfig --region "$REGION" --name "$CLUSTER"
kubectl get nodes

echo "== ServiceAccount =="
kubectl apply -f infra/k8s/kafka-client-sa.yaml

echo "== db-credentials Secret =="
kubectl create secret generic db-credentials \
  --from-literal=POSTGRES_USER=markettick_admin \
  --from-literal=POSTGRES_PASSWORD="$PG_PASSWORD" \
  --from-literal=POSTGRES_DB=markettick \
  --from-literal=POSTGRES_HOST="$RDS_HOST" \
  --from-literal=POSTGRES_PORT=5432 \
  --from-literal=DATABASE_URL="postgres://markettick_admin:${PG_PASSWORD}@${RDS_HOST}:5432/markettick" \
  --from-literal=REDIS_ADDR="${REDIS_HOST}:6379" \
  --from-literal=REDIS_PASSWORD="" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "== infra-config ConfigMap (broker + feed URL overrides) =="
apply_infra_config "$MSK_BROKER"

echo "== per-service static ConfigMaps =="
apply_static_service_configs

echo "== Postgres schema =="
kubectl create configmap postgres-init-files \
  --from-file=persistence/db/schema/aggregated_ticks.sql \
  --from-file=persistence/db/schema/orderbook_flushes.sql \
  --from-file=scripts/init-postgres-container.sh \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl delete job postgres-init --ignore-not-found
kubectl apply -f infra/k8s/postgres-init-job.yaml
kubectl wait --for=condition=complete --timeout=120s job/postgres-init

echo "== Kafka topics =="
kubectl delete job bootstrap-topics --ignore-not-found
kubectl apply -f infra/k8s/bootstrap-topics-job.yaml
kubectl wait --for=condition=complete --timeout=60s job/bootstrap-topics

echo "== Prometheus + Grafana (Helm) =="
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts 2>/dev/null || true
helm repo update
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace monitoring --create-namespace \
  -f infra/helm/prometheus-values.yaml
kubectl apply -f infra/k8s/pod-monitors.yaml

echo "== deploy the 5 pipeline services =="
kubectl apply -f infra/k8s/services.yaml
set_service_images "$TAG"
for svc in adapter normalizer aggregator orderbook persistence; do
  kubectl rollout status deployment/$svc --timeout=120s
done

echo "== done =="
kubectl get pods
