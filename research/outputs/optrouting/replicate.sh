#!/usr/bin/env bash
# Regenerate data/ from the commit0 traces. Usage: ./replicate.sh [optimal_isolated.py args...]
# Clones the pinned blis-catalog and blis-registry into research/outputs/deps/ if they are missing,
# unless BLIS_CATALOG and BLIS_REGISTRY are already set. Resumable: rerun after an interruption.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
deps=$(cd "$here/.." && pwd)/deps
if [[ -z "${BLIS_CATALOG:-}" || -z "${BLIS_REGISTRY:-}" ]]; then
  mkdir -p "$deps"
  [[ -d $deps/blis-catalog ]] || git -c advice.detachedHead=false clone -q --branch 0.3.0 --depth 1 \
    https://github.com/inference-sim/blis-catalog.git "$deps/blis-catalog"
  [[ -d $deps/blis-registry ]] || git -c advice.detachedHead=false clone -q --branch v0.2.0 --depth 1 \
    https://github.com/inference-sim/blis-registry.git "$deps/blis-registry"
  export BLIS_CATALOG=$deps/blis-catalog BLIS_REGISTRY=$deps/blis-registry
fi
exec python3 -I "$here/optimal_isolated.py" "$@"
