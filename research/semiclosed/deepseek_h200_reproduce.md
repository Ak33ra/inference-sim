# Reproduce the DeepSeek H200 diagnostic

Run these commands from the repository root. They require the local FreeInference
release and the catalog and registry selected by `.inference-sim-env/activate`.
The [study report](deepseek_h200.md) records the exact versions and limitations.
The four-GPU scenario is a diagnostic, not a promoted configuration.

Create a new output directory and build the current semi-closed implementation:

```bash
source .inference-sim-env/activate
study_output=research/outputs/deepseek-h200-repeat
mkdir "$study_output"
go build -o "$study_output/blis" .
deepseek_python=(uv run --no-project --python '>=3.11' \
  --with-requirements research/freeinference_trace/requirements.txt python)
cp -R research/configs/diagnostics/deepseek-h200 "$study_output/scenarios"
```

Convert the two complete-family cohorts:

```bash
"${deepseek_python[@]}" -m research.freeinference_trace \
  --source traces/freeinference_agentic_trace/release \
  --week 2026-07-12_2026-07-18 --model deepseek-v4-flash \
  --start-ms 1784246400000 --end-ms 1784289600000 \
  --output "$study_output/discovery-input"

"${deepseek_python[@]}" -m research.freeinference_trace \
  --source traces/freeinference_agentic_trace/release \
  --week 2026-07-19_2026-07-25 --model deepseek-v4-flash \
  --start-ms 1784851200000 --end-ms 1784894400000 \
  --output "$study_output/holdout-input"
```

Expect 4,288 discovery requests and 1,202 holdout requests. The manifests record
all source hashes and exclusions. The source label stays `deepseek-v4-flash`.
The scenario selects the serving model `deepseek-v4.1-flash`.

Run both cohorts with five seeds and compare every result against the original
Parquet rows:

```bash
for seed in 42 43 44 45 46; do
  for cohort in discovery holdout; do
    run_dir="$study_output/tp4-k5-off-$cohort-seed$seed"
    mkdir "$run_dir"
    "$study_output/blis" replay \
      --semi-closed-input "$study_output/$cohort-input" \
      --semi-closed-report "$run_dir/workload.json" \
      --metrics-path "$run_dir/metrics.json" \
      --scenario deepseek-v4.1-flash-h200-tp4-dspark5-1m.yaml \
      --scenarios "$study_output/scenarios" \
      --catalog "$BLIS_CATALOG" --registry "$BLIS_REGISTRY" \
      --speculative-category coding --speculative-thinking off \
      --num-instances 1 --seed "$seed" --server-series-interval-s 0 \
      > "$run_dir/run.log" 2>&1 || break 2
    "${deepseek_python[@]}" research/scripts/compare_semiclosed.py \
      --input "$study_output/$cohort-input" \
      --report "$run_dir/workload.json" --metrics "$run_dir/metrics.json" \
      --source traces/freeinference_agentic_trace/release \
      --output "$run_dir/comparison.json" --pairs-csv "$run_dir/pairs.csv" \
      --serving-model deepseek-v4.1-flash || break 2
  done
done
```

For one run per cohort, use only seed `42`. Replay drains naturally, so omit
`--horizon` and `--timeout`. These controls are not supported by semi-closed
replay. The comparator verifies source hashes, membership, causal timing,
request/token conservation, and zero failed or unfinished work before writing
latency comparisons. Existing comparison files are not overwritten.

To repeat the TP2 failure, run the same replay command with the TP2 scenario
and fresh report paths:

```bash
"$study_output/blis" replay \
  --semi-closed-input "$study_output/holdout-input" \
  --semi-closed-report "$study_output/tp2-workload.json" \
  --metrics-path "$study_output/tp2-metrics.json" \
  --scenario deepseek-v4.1-flash-h200-tp2-dspark5-1m.yaml \
  --scenarios "$study_output/scenarios" \
  --catalog "$BLIS_CATALOG" --registry "$BLIS_REGISTRY" \
  --speculative-category coding --speculative-thinking off \
  --num-instances 1 --seed 42 --server-series-interval-s 0 \
  > "$study_output/tp2-startup.log" 2>&1
```

This command is expected to fail at engine startup. Use the TP1 scenario and
distinct output paths for the one-GPU check. The direct weight breakdown is
saved in the original study's `memory-breakdown.jsonl`, with `memory_probe.go`
beside it. The probe uses that study's scenario directory and the activated
catalog and registry:

```bash
go run research/outputs/deepseek-h200/memory_probe.go \
  deepseek-v4.1-flash-h200-tp1-dspark5-1m.yaml \
  deepseek-v4.1-flash-h200-tp2-dspark5-1m.yaml
```

Run the comparator's focused numeric and cohort checks:

```bash
python3 -m unittest discover -s research/scripts/tests -v
```
