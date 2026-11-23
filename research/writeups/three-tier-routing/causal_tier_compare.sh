#!/usr/bin/env bash
# Tune on seed 42, then evaluate the selected causal tier policy on held-out seeds and 2x load.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
deps=$repo/research/outputs/deps
die() { echo "causal_tier_compare.sh: $*" >&2; exit 1; }

go version 2>/dev/null | grep -q 'go1\.27\.' || die "Go 1.27.x must be on PATH"
for tool in python3 zstd git patch; do command -v "$tool" >/dev/null || die "$tool is required"; done
mkdir -p "$deps"

if [[ -z "${BLIS_CATALOG:-}" || -z "${BLIS_REGISTRY:-}" ]]; then
  [[ -d $deps/blis-catalog ]] || git -c advice.detachedHead=false clone -q --branch 0.3.0 --depth 1 \
    https://github.com/inference-sim/blis-catalog.git "$deps/blis-catalog"
  [[ -d $deps/blis-registry ]] || git -c advice.detachedHead=false clone -q --branch v0.2.0 --depth 1 \
    https://github.com/inference-sim/blis-registry.git "$deps/blis-registry"
  export BLIS_CATALOG=$deps/blis-catalog BLIS_REGISTRY=$deps/blis-registry
fi

version=$(cd "$repo" && go list -m -f '{{if .Replace}}{{.Replace.Version}}{{end}}' github.com/llm-d/llm-d-router)
[[ $version == v1.0.0-blis.1 ]] || die "go.mod pins llm-d-router fork $version; expected v1.0.0-blis.1"
fork=$deps/llm-d-router-$version-header-routing
if [[ ! -f $fork/.patched ]]; then
  source_dir=$(cd "$repo" && go mod download -json "github.com/inference-sim/llm-d-router@$version" |
        python3 -I -c 'import json,sys; print(json.load(sys.stdin)["Dir"])')
  rm -rf "$fork"
  cp -r "$source_dir" "$fork"
  chmod -R u+w "$fork"
  patch -s -d "$fork" -p1 < "$here/llm-d-router-header-profile.patch"
  touch "$fork/.patched"
fi
cat > "$deps/three-tier-routing.go.work" <<EOF
go 1.27.2

use $repo

replace github.com/llm-d/llm-d-router => $fork
EOF
export GOWORK=$deps/three-tier-routing.go.work
exec python3 -I "$here/causal_tier_compare.py" "$@"
