#!/usr/bin/env bash
# Shared helpers sourced by the apply-*.sh scripts. Not meant to be run directly.
set -euo pipefail

REGION=us-east-1
CLUSTER=mta-eks
TAG_FILE="infra/terraform/.image_tag"

# Current image tag, defaults to v1 the very first time this repo is used.
current_image_tag() {
  if [ -f "$TAG_FILE" ]; then
    cat "$TAG_FILE"
  else
    echo "v1"
  fi
}

# Bumps the numeric suffix by one and persists it, so nobody has to remember
# to edit anything - this is the only place the tag ever changes.
bump_image_tag() {
  local current next new_tag
  current=$(current_image_tag)
  next=$(( ${current#v} + 1 ))
  new_tag="v${next}"
  echo "$new_tag" > "$TAG_FILE"
  echo "$new_tag"
}

# Regenerates the single shared ConfigMap every Kafka-touching pod reads via
# envFrom. Feed URL overrides default to empty (real exchanges) unless the
# caller has exported BINANCE_WS_URL / COINBASE_WS_URL / KRAKEN_WS_URL first.
apply_infra_config() {
  local msk_broker="$1"

  kubectl create configmap infra-config \
    --from-literal=KAFKA_BOOTSTRAP_SERVERS="$msk_broker" \
    --from-literal=KAFKA_ADDR="$msk_broker" \
    --from-literal=KAFKA_AUTH_MODE=iam \
    --from-literal=BINANCE_WS_URL="${BINANCE_WS_URL:-}" \
    --from-literal=COINBASE_WS_URL="${COINBASE_WS_URL:-}" \
    --from-literal=KRAKEN_WS_URL="${KRAKEN_WS_URL:-}" \
    --from-literal=BINANCE_REST_URL="${BINANCE_REST_URL:-}" \
    --dry-run=client -o yaml | kubectl apply -f -
}

# Static per-service YAML config (topics, worker counts, windows, batching -
# everything that doesn't change across environments). Created directly from
# the repo's own config files - no sed, no per-environment regeneration,
# since the broker address now comes from infra-config's env var instead.
apply_static_service_configs() {
  for svc in adapter normalizer aggregator orderbook persistence ui-backend; do
    kubectl create configmap "$svc-config" \
      --from-file=config.yaml="$svc/config/docker.config.yaml" \
      --dry-run=client -o yaml | kubectl apply -f -
  done
}

# Applies a manifest with every image tag rewritten to $tag in the same pass
# that hits the cluster - no separate "apply, then kubectl set image" step,
# so there's never a moment where the wrong tag is briefly (or permanently,
# if the follow-up step gets skipped) what's actually running. Works
# regardless of whatever tag happens to already be in the checked-in file,
# so those files never need manual editing again.
apply_with_current_tag() {
  local file="$1" tag="$2"
  sed -E "s#(market-tick-[a-z]+):v[0-9]+#\1:${tag}#g" "$file" | kubectl apply -f -
}

apply_services() {
  local tag="$1"
  apply_with_current_tag infra/k8s/services.yaml "$tag"
  for svc in adapter normalizer aggregator orderbook persistence; do
    kubectl rollout status deployment/$svc --timeout=120s
  done
}

deploy_mocks() {
  local tag="$1"
  apply_with_current_tag infra/k8s/mock-exchanges.yaml "$tag"
  for m in binance-mock coinbase-mock kraken-mock; do
    kubectl rollout status deployment/$m --timeout=120s
  done
}
