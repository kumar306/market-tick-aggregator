#!/usr/bin/env bash
# Regenerates every module's proto/generated/*.pb.go from its proto/*.proto
# sources. Each .proto declares its own `option go_package = "proto/generated"`,
# so protoc just needs --go_out=. run from inside the module directory, no
# extra path flags. Used both by developers after touching a .proto file and
# by CI (.github/workflows/ci.yml, proto-check job) to verify committed
# generated code isn't stale.
#
# Requires protoc and protoc-gen-go on PATH. Pinned versions this was last
# verified against: protoc 33.1, protoc-gen-go v1.36.10 - a different protoc
# version can produce a byte-different (but not necessarily wrong) output,
# since the version itself gets embedded in the generated file's header.
#
# Usage: ./scripts/generate-proto.sh
set -euo pipefail
cd "$(dirname "$0")/.."

MODULES=(aggregator normalizer orderbook persistence ui-backend)

for mod in "${MODULES[@]}"; do
  for proto_file in "$mod"/proto/*.proto; do
    [ -e "$proto_file" ] || continue
    rel="${proto_file#"$mod"/}"
    echo "  $mod: $rel"
    (cd "$mod" && protoc --go_out=. "$rel")
  done
done

echo "Done. Run 'git diff' to see what changed, if anything."
