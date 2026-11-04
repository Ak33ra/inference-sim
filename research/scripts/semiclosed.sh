#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage: research/scripts/semiclosed.sh [smoke|checks|full] [OPTIONS]
       research/scripts/semiclosed.sh run CONFIG.json [--output PATH] [--python EXE] [--dry-run]
       research/scripts/semiclosed.sh verify RUN_DIR [--source DIR] [--python EXE] [--dry-run]

smoke   Convert, build, replay, and verify the documented 175-request selection (default).
checks  Run smoke plus workload, cluster, CLI, and original-source AgentX parity tests.
full    Replay all eligible selected-model families in every available weekly directory.
verify  Check a saved run against original Parquet and print JSON without changing it.
run     Run a JSON experiment with separate trace and serving models and numbered weeks.

  --output NEW_DIR       Fresh run directory (default: research/outputs/semiclosed/<UTC>-<PID>)
  --source DIR           FreeInference release directory (default: repository traces path)
  --week NAME            Restrict full to this week; repeatable
  --model NAME           Full model label (default: glm-5.2); other models require --scenario
  --scenario FILE        Compatible scenario YAML (default: GLM5.2, B200 TP8, two replicas)
  --num-instances N      Replica count (default: 2)
  --join-policy POLICY   timing-inferred (default) or none
  --python EXE           Python 3.11+ with PyArrow (also accepts the PYTHON environment variable)
  --catalog DIR          Override BLIS_CATALOG
  --registry DIR         Override BLIS_REGISTRY
  --dry-run              Print commands without building, downloading, or writing outputs
  -h, --help             Show this help

Paths supplied by the caller are relative to the caller's directory. The local
.inference-sim-env/activate is loaded when present. Caller BLIS_CATALOG,
BLIS_REGISTRY, and GOCACHE overrides are preserved. Without a suitable python3,
uv provisions Python and requirements in its cache. No system pip install is run.
Full retains converter exclusions in input/manifest.json and input/audit.jsonl.
EOF
}

die() { printf 'semiclosed: %s\n' "$*" >&2; exit 2; }
caller=$PWD
absolute() { case $1 in /*) printf '%s\n' "$1" ;; *) printf '%s/%s\n' "$caller" "$1" ;; esac; }
repo=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
if [[ ${1:-} == run ]]; then
    shift
    overrides=()
    for name in BLIS_CATALOG BLIS_REGISTRY GOCACHE PYTHON; do
        if [[ -v $name ]]; then
            value=${!name}
            if [[ -n $value && ( $name != PYTHON || $value == */* ) ]]; then value=$(absolute "$value"); fi
            overrides+=("$name=$value")
        fi
    done
    if [[ -f $repo/.inference-sim-env/activate ]]; then source "$repo/.inference-sim-env/activate"; fi
    for override in "${overrides[@]}"; do export "$override"; done
    exec python3 -B "$repo/research/semiclosed/run.py" "$@"
fi
mode=smoke
case ${1:-} in smoke|checks|full|verify) mode=$1; shift ;; esac
case ${1:-} in -h|--help) usage; exit 0 ;; esac
run_dir=
if [[ $mode == verify ]]; then
    [[ $# -gt 0 && $1 != -* ]] || die 'verify requires RUN_DIR'
    run_dir=$(absolute "$1")
    shift
fi
output=
source_dir=$repo/traces/freeinference_agentic_trace/release
scenario=$repo/research/configs/clusters/glm-5.2-b200-tp8-2replicas.yaml
scenario_set=0
model=glm-5.2
instances=2
join_policy=timing-inferred
python_exe=${PYTHON:-}
catalog=
registry=
dry_run=0
weeks=()
while (($#)); do
    case $1 in
        -h|--help) usage; exit 0 ;;
        --dry-run) dry_run=1; shift; continue ;;
        --output|--source|--week|--model|--scenario|--num-instances|--join-policy|--python|--catalog|--registry)
            [[ $# -ge 2 && -n $2 && $2 != --* ]] || die "$1 requires a value" ;;
        *) die "unknown argument: $1" ;;
    esac
    if [[ $mode == verify && $1 != --source && $1 != --python ]]; then
        die "$1 is not supported by verify"
    fi
    case $1 in
        --output) output=$(absolute "$2") ;;
        --source) source_dir=$(absolute "$2") ;;
        --week) [[ $mode == full ]] || die '--week is only supported by full'; weeks+=("$2") ;;
        --model) [[ $mode == full ]] || die '--model is only supported by full'; model=$2 ;;
        --scenario) scenario=$(absolute "$2"); scenario_set=1 ;;
        --num-instances) instances=$2 ;;
        --join-policy) join_policy=$2 ;;
        --python) python_exe=$2 ;;
        --catalog) catalog=$(absolute "$2") ;;
        --registry) registry=$(absolute "$2") ;;
    esac
    shift 2
done
[[ $instances =~ ^[1-9][0-9]*$ ]] || die '--num-instances must be a positive integer'
[[ $join_policy == timing-inferred || $join_policy == none ]] || die 'invalid --join-policy'
[[ $model == glm-5.2 || $scenario_set == 1 ]] || die 'a custom --model requires an explicit compatible --scenario'
if [[ $python_exe == */* ]]; then python_exe=$(absolute "$python_exe"); fi
overrides=()
for name in BLIS_CATALOG BLIS_REGISTRY GOCACHE; do
    if [[ -v $name ]]; then
        value=${!name}
        [[ -z $value ]] || value=$(absolute "$value")
        overrides+=("$name=$value")
    fi
done
cd -- "$repo"
if [[ -f .inference-sim-env/activate ]]; then source .inference-sim-env/activate; fi
for override in "${overrides[@]}"; do export "$override"; done
catalog=${catalog:-${BLIS_CATALOG:-}}
registry=${registry:-${BLIS_REGISTRY:-}}
if [[ $mode != verify && $dry_run == 0 ]]; then
    command -v go >/dev/null 2>&1 || die 'go is not on PATH; activate the project Go environment'
    [[ -f $scenario ]] || die "scenario file does not exist: $scenario"
    [[ -f $repo/defaults.yaml ]] || die "defaults file does not exist: $repo/defaults.yaml"
    [[ -n $catalog && -d $catalog ]] || die 'set --catalog DIR or BLIS_CATALOG to the BLIS catalog directory'
    [[ -n $registry && -d $registry ]] || die 'set --registry DIR or BLIS_REGISTRY to the BLIS registry directory'
fi

window=()
if [[ $mode == full ]]; then
    if ((${#weeks[@]} == 0)); then
        for dir in "$source_dir"/????-??-??_????-??-??; do
            [[ ! -d $dir ]] || weeks+=("${dir##*/}")
        done
    fi
    ((${#weeks[@]})) || die "no weekly directories in $source_dir"
elif [[ $mode != verify ]]; then
    weeks=(2026-07-12_2026-07-18)
    window=(--start-ms 1784081087725 --end-ms 1784086819492)
fi
week_args=()
for week in "${weeks[@]}"; do
    [[ $week != */* && $week != . && $week != .. ]] || die "invalid week: $week"
    week_args+=(--week "$week")
done
if [[ $mode != verify ]]; then
    output=${output:-$repo/research/outputs/semiclosed/$(date -u +%Y%m%dT%H%M%SZ)-$$}
    [[ ! -e $output && ! -L $output ]] || die "output already exists: $output"
    run_dir=$output
fi

python_check='import sys; assert sys.version_info >= (3, 11), "Python 3.11+ required"; import pyarrow'
if [[ -n $python_exe ]]; then
    python=("$python_exe")
    if (( ! dry_run )); then
        "$python_exe" -B -c "$python_check" || die '--python/PYTHON must provide Python 3.11+ and PyArrow'
    fi
elif (( ! dry_run )) && python3 -B -c "$python_check" >/dev/null 2>&1; then
    python=(python3)
elif command -v uv >/dev/null 2>&1; then
    python=(uv run --no-project --python '>=3.11' --with-requirements "$repo/research/freeinference_trace/requirements.txt" python)
elif ((dry_run)); then
    python=(python3)
else
    die 'install Python 3.11+ with PyArrow or uv, or pass --python EXE'
fi

run() {
    local command
    printf -v command '%q ' "$@"
    if ((dry_run)); then printf '%s\n' "$command"; return; fi
    printf '+ %s\n' "$command" >&2
    if [[ $mode != verify ]]; then printf '%s\n' "$command" >> "$output/commands.log"; fi
    "$@"
}
verify=("${python[@]}" "$repo/research/semiclosed/verify.py" --input "$run_dir/input"
    --report "$run_dir/workload.json" --metrics "$run_dir/metrics.json" --source "$source_dir")
if [[ $mode == verify ]]; then run "${verify[@]}"; exit 0; fi

if (( ! dry_run )); then
    mkdir -p -- "$(dirname -- "$output")"
    mkdir -- "$output"
    exec > >(tee "$output/run.log") 2>&1
    trap 'status=$?; printf "Run directory: %s\nExit status: %s\n" "$output" "$status"; exit "$status"' EXIT
    printf 'Mode: %s\nSource: %s\nModel: %s\nWeeks: %s\n' "$mode" "$source_dir" "$model" "${weeks[*]}"
    if [[ $mode == full ]]; then
        printf 'Full scope: all eligible selected-model families in these weeks, without a timestamp window.\n'
    fi
    printf 'Artifacts and logs: %s\n' "$output"
fi
run "${python[@]}" -m research.freeinference_trace --source "$source_dir" "${week_args[@]}" \
    --model "$model" "${window[@]}" --output "$output/input"
run "${python[@]}" -c 'import json,sys; m=json.load(open(sys.argv[1])); print(json.dumps({k:m[k] for k in ("selection", "counts", "included", "excluded_families_by_reason")}, indent=2))' "$output/input/manifest.json"
run go build -o "$output/blis" .
replay=("$output/blis" replay --semi-closed-input "$output/input" --semi-closed-report "$output/workload.json"
    --metrics-path "$output/metrics.json" --semi-closed-join-policy "$join_policy"
    --scenario "$(basename -- "$scenario")" --scenarios "$(dirname -- "$scenario")"
    --num-instances "$instances" --seed 42 --server-series-interval-s 0 --defaults-filepath "$repo/defaults.yaml")
[[ -z $catalog ]] || replay+=(--catalog "$catalog")
[[ -z $registry ]] || replay+=(--registry "$registry")
run "${replay[@]}"
if ((dry_run)); then
    run "${verify[@]}"
else
    run "${verify[@]}" | tee "$output/verification.json"
fi
if [[ $mode == checks ]]; then
    run "${python[@]}" -m research.freeinference_trace --source "$source_dir" --week 2026-07-12_2026-07-18 \
        --model glm-5.2 --start-ms 1784204303061 --end-ms 1784204303062 --output "$output/source-parity"
    test_env=(env -u BLIS_CATALOG -u BLIS_REGISTRY "BLIS_SEMICLOSED_SOURCE_FIXTURE=$output/source-parity")
    run "${test_env[@]}" go test ./sim/workload/semiclosed -count=1
    run "${test_env[@]}" go test ./sim/cluster -run '^TestSemiClosed' -count=1
    run "${test_env[@]}" go test ./cmd -run 'Test(SemiClosed|ReplaySemiClosed)' -count=1
fi
