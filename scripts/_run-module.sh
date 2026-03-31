#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <module>" >&2
  exit 1
fi

# ensure log dir is created, if not then create it
MODULE="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs"
mkdir -p "${LOG_DIR}"

# load env vars for postgres, redis config
if [ -f "${ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT}/.env"
  set +a
fi

# create the logfile
TS="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/${MODULE}-${TS}.log"

echo "[${MODULE}] starting; log=${LOG_FILE}"
exec > >(tee "${LOG_FILE}") 2>&1
cd "${ROOT}/${MODULE}"
if command -v stdbuf >/dev/null 2>&1; then
  exec stdbuf -oL -eL go run .
else
  exec go run .
fi
