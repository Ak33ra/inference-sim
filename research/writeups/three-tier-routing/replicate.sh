#!/usr/bin/env bash
# Replicate the three-tier routing result (README.md in this directory).
#   research/writeups/three-tier-routing/replicate.sh [replicate.py args...]
# Needs Go 1.27.2 on PATH, python3, zstd, git and patch. Sets up, under research/outputs/deps/
# (gitignored): the pinned blis-catalog and blis-registry clones (unless BLIS_CATALOG and
# BLIS_REGISTRY are set), and a copy of the llm-d router fork with llm-d-router-header-profile.patch
# applied, selected by a Go workspace file through GOWORK, so go.mod stays untouched. Resumable.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
repo=$(cd "$here/../../.." && pwd)
deps=$repo/research/outputs/deps
die() { echo "replicate.sh: $*" >&2; exit 1; }

go version 2>/dev/null | grep -q 'go1\.27\.' || die "Go 1.27.x must be on PATH (the llm-d router requires it)"
for t in python3 zstd git patch; do command -v $t >/dev/null || die "$t is required"; done
mkdir -p "$deps"

if [[ -z "${BLIS_CATALOG:-}" || -z "${BLIS_REGISTRY:-}" ]]; then
  [[ -d $deps/blis-catalog ]] || git -c advice.detachedHead=false clone -q --branch 0.3.0 --depth 1 \
    https://github.com/inference-sim/blis-catalog.git "$deps/blis-catalog"
  [[ -d $deps/blis-registry ]] || git -c advice.detachedHead=false clone -q --branch v0.2.0 --depth 1 \
    https://github.com/inference-sim/blis-registry.git "$deps/blis-registry"
  export BLIS_CATALOG=$deps/blis-catalog BLIS_REGISTRY=$deps/blis-registry
fi

# The pinned fork plus two pure header plugins in its audited list (profile selection and soft affinity).
version=$(cd "$repo" && go list -m -f '{{if .Replace}}{{.Replace.Version}}{{end}}' github.com/llm-d/llm-d-router)
[[ $version == v1.0.0-blis.1 ]] || die "go.mod pins llm-d-router fork $version; the patch is for v1.0.0-blis.1"
fork=$deps/llm-d-router-$version-header-routing
if [[ ! -f $fork/.patched ]]; then
  src=$(cd "$repo" && go mod download -json "github.com/inference-sim/llm-d-router@$version" |
        python3 -I -c 'import json,sys; print(json.load(sys.stdin)["Dir"])')
  rm -rf "$fork"; cp -r "$src" "$fork"; chmod -R u+w "$fork"
  patch -s -d "$fork" -p1 < "$here/llm-d-router-header-profile.patch"
  touch "$fork/.patched"
fi
cat > "$deps/three-tier-routing.go.work" <<EOF
go 1.27.2

use $repo

replace github.com/llm-d/llm-d-router => $fork
EOF
export GOWORK=$deps/three-tier-routing.go.work
exec python3 -I "$here/replicate.py" "$@"
