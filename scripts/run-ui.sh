#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${ROOT}/logs"
mkdir -p "${LOG_DIR}"

if [ -f "${ROOT}/.env" ]; then
  set -a
  # shellcheck disable=SC1091
  . "${ROOT}/.env"
  set +a
fi

TS="$(date +%Y%m%d-%H%M%S)"
LOG_FILE="${LOG_DIR}/ui-${TS}.log"

echo "[ui] starting; log=${LOG_FILE}"
exec > >(tee "${LOG_FILE}") 2>&1
cd "${ROOT}/ui"
if command -v stdbuf >/dev/null 2>&1; then
  exec stdbuf -oL -eL npm run dev
else
  exec npm run dev
fi
