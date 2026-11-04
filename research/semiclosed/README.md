# Finite semi-closed FreeInference replay

This replay admits every selected root family at its recorded start time. Later turns follow simulated response completion plus the recorded non-LLM delay. A family leaves when its root and every child session have drained. There is no family cap, sampling, warmup, recycling, or idle compression.

The input is the existing [FreeInference conversion](../freeinference_trace/README.md), containing `manifest.json`, `corpus.jsonl`, and `audit.jsonl`. The loader checks artifact sizes and SHA256 digests, then cross-checks family and session identities, steps, recorded times, token counts, prefix coverage, child placement, join candidates, and totals. Child order must agree with the audit's source tool-call order. Weka does not retain the original numerical tool-call index, so source verification checks that field against Parquet.

## Run from a config

Use one JSON file to select the source model, simulated model, cluster layout,
weeks, and output directory:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/glm52-smoke.json
```

`semi_closed.sh` is an alias for `semiclosed.sh`; both accept the same commands.
The config command converts the selection, builds BLIS, drains the semi-closed
workload, and verifies request and token accounting against the original Parquet.
The [config guide](../configs/semiclosed/README.md) describes the fields and paths.

Preview a config without building, converting, downloading dependencies, or
creating output files:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/glm52-all.json --dry-run
```

Available examples are [GLM smoke](../configs/semiclosed/glm52-smoke.json),
[all GLM weeks](../configs/semiclosed/glm52-all.json), and the
[DeepSeek H200 diagnostic](../configs/semiclosed/deepseek-h200-diagnostic.json).
The DeepSeek example uses four H200s and is not a calibrated trace match.
The earlier one- and two-H200 configurations failed memory feasibility.

Use `--output NEW_DIR` to override the config's output path for another run.
Existing output directories are never overwritten. The older commands below
retain their existing arguments and defaults.

## Run with the script

From the repository root, run the 175-request smoke selection and verify its
results against the original Parquet tables:

```bash
research/scripts/semiclosed.sh smoke
```

The script builds the current checkout, converts the selection, runs semi-closed
replay, and verifies the result. It prints a new run directory under
`research/outputs/semiclosed/`. That directory keeps the conversion in `input/`,
the workload report in `workload.json`, engine metrics in `metrics.json`, and
the verification result in `verification.json`, together with command logs.
Use `--output PATH` to choose a new run directory. Existing directories are not
overwritten.

Run the smoke selection and the semi-closed Go checks, including the original
source-family AgentX comparison:

```bash
research/scripts/semiclosed.sh checks
```

Replay every eligible GLM-5.2 family across all available source weeks, without
a root-arrival window or simulation horizon:

```bash
research/scripts/semiclosed.sh full
```

`full` preserves the converter's exclusions for mixed-model and unsupported
families. Inspect `input/manifest.json` for the selected counts and exclusions.
It does not combine different serving models into one simulation. To select
particular weeks, repeat `--week`:

```bash
research/scripts/semiclosed.sh full --week 2026-07-12_2026-07-18
```

Verify an existing run again without rebuilding or replaying:

```bash
research/scripts/semiclosed.sh verify research/outputs/semiclosed/YOUR_RUN
```

The default serving configuration is two B200 TP8 replicas with seed 42.
Use `--join-policy none` for the join sensitivity run. Use `--scenario FILE`
and `--num-instances N` together to select another serving configuration. A
different trace `--model` requires an explicit compatible scenario.

The script loads `.inference-sim-env/activate` when present. It uses a Python
3.11 or newer interpreter with PyArrow, or `uv` to provide that environment.
The first `uv` invocation can download Python or PyArrow. Set `--python PATH`
to use an existing interpreter. The source release defaults to
`traces/freeinference_agentic_trace/release/`; use `--source PATH` to relocate it.
Explicit command-line paths are relative to the directory where you invoke
the script. The script itself can be invoked from any directory.

Use `--help` to list options, or preview a full run without creating outputs:

```bash
research/scripts/semiclosed.sh full --dry-run
```

The [DeepSeek H200 study](deepseek_h200.md) records the V4-Flash to V4.1-Flash
substitution, hardware feasibility checks, and source-to-simulator comparisons.

Compare an existing run with its original request latencies after verification:

```bash
uv run --no-project --python '>=3.11' \
  --with-requirements research/freeinference_trace/requirements.txt \
  python research/scripts/compare_semiclosed.py \
  --input research/outputs/semiclosed/YOUR_RUN/input \
  --report research/outputs/semiclosed/YOUR_RUN/workload.json \
  --metrics research/outputs/semiclosed/YOUR_RUN/metrics.json \
  --source traces/freeinference_agentic_trace/release \
  --output research/outputs/semiclosed/YOUR_RUN/comparison.json \
  --pairs-csv research/outputs/semiclosed/YOUR_RUN/pairs.csv \
  --serving-model glm-5.2-fp8
```

The comparator requires new output paths. `--serving-model` records your scenario
declaration; it does not change the run or infer the serving model from request
tags. E2E includes every request. TTFT and TPOT use identical eligible source rows
on both sides, with eligibility counts in the output. Family and session makespans,
paired errors, and week/provider and token-length groups are included.

## Run the individual commands

Use the project's Go environment and Python 3.11 or newer with PyArrow. Run these commands from the repository root.

```bash
python -m research.freeinference_trace \
  --source traces/freeinference_agentic_trace/release \
  --week 2026-07-12_2026-07-18 --model glm-5.2 \
  --start-ms 1784081087725 --end-ms 1784086819492 \
  --output research/outputs/semiclosed-source-smoke

go build -o /tmp/blis-semiclosed .
/tmp/blis-semiclosed replay \
  --semi-closed-input research/outputs/semiclosed-source-smoke \
  --semi-closed-report research/outputs/semiclosed-workload.json \
  --metrics-path research/outputs/semiclosed-engine.json \
  --scenario glm-5.2-b200-tp8-2replicas.yaml \
  --scenarios research/configs/clusters \
  --num-instances 2 --seed 42 --server-series-interval-s 0 \
  --catalog "$BLIS_CATALOG" --registry "$BLIS_REGISTRY" \
  --defaults-filepath defaults.yaml

python research/semiclosed/verify.py \
  --input research/outputs/semiclosed-source-smoke \
  --report research/outputs/semiclosed-workload.json \
  --metrics research/outputs/semiclosed-engine.json \
  --source traces/freeinference_agentic_trace/release
```

The converter requires a new output directory. Keep the generated conversion and reports local. Do not commit third-party trace data.

This smoke selection contains two complete families, ten sessions, and 175 requests. Its original Parquet totals are 9,077,878 prompt tokens and 169,589 output tokens, including one zero-output request. `verify.py` reads the original Parquet request and session tables, checks their manifest hashes, and verifies the selected request set, source counts, engine totals, causal arrivals, and family lifetimes.

`--semi-closed-report` is required. `--metrics-path` retains the standard engine report and its per-request rows. `--results-path` is incompatible because the legacy SimResult format requires numeric TraceV2 IDs. Semi-closed request IDs preserve the source trace, session, and step identities.

The replay drains naturally with an unbounded simulation horizon. Explicit `--horizon`, TraceV2 controls, and AgentX workload controls are rejected. `--semi-closed-join-policy none` disables inferred join dependencies for a sensitivity run. It retains all requests and ordinary parent gaps.

## Timing and lifecycle

All simulation times use integer microseconds. Recorded source times remain absolute milliseconds in the report. The simulation epoch is the earliest selected root start.

| Work | Release rule |
| --- | --- |
| Root request | Recorded family origin minus the earliest origin |
| Ordinary turn | Previous actual completion plus recorded start-minus-previous-end |
| Child starting after the spawning response | Parent actual completion plus recorded spawn gap |
| Child starting during the spawning request | Parent actual send plus recorded child-start-minus-parent-start |
| Joined parent turn | Latest actual prerequisite completion plus the join residual |

For a join, prerequisites include the preceding parent request and the terminal request of every required child. The residual is the next recorded parent start minus the latest recorded prerequisite end. It replaces the ordinary parent gap. Adding both delays would count the recorded child wait twice.

Joins are timing-based assumptions, not observed barriers. The report retains each session's candidate join even when the policy is `none`. Overlapping turns within one session, nested delegation, and other converter exclusions remain excluded by the existing converter. Final tools after the last LLM request are not represented. Completeness is limited to the published weekly tables.

The runtime has one state per request. Requests move through pending, scheduled, issued, and completed states. A refusal, timeout, or token-count mismatch fails the request and blocks unsent dependent work. A child that already issued remains independent and drains its own continuation. Duplicate callbacks have no effect. Unfinished work becomes censored when the simulation stops. A successful run requires every selected request to complete and aggregate engine token totals to equal the original source totals.

Completion callbacks can announce a future step end. The driver schedules a terminal timer before changing lifecycle state, allowing earlier overlap arrivals to execute first. It resolves P/D copied parents by request ID and maps failed stage IDs back to the logical request.

## Token and report definitions

Each wire prompt has exactly the source prompt length. Each output slice has exactly the source output length, including zero. The client budget is at least one to avoid the engine's zero-budget autofill rule; it does not add an output token. There are no chat-template or cache-marker tokens.

The converter maps each fourth chained 16-token block identity to one 64-token block. The replay assigns collision-free positive token IDs within the corpus, preserving equal mapped blocks within each family-local namespace. Unknown suffixes use request-specific identities. Prefix sharing beyond mapped coverage is unknown. Tokens are materialized at dispatch, and the driver releases its request pointer after terminal bookkeeping.

The workload report contains the original manifest and its SHA256, selected totals, terminal state counts, source-token budgets by state, engine totals, request dependencies, and family/session lifetimes. A complete-family makespan runs from the root's send to the last completed descendant. Incomplete families have no completion or makespan value.

`arrival_us`, `completion_us`, and `e2e_us` describe the lifecycle used to schedule dependencies. `ttft_us`, `engine_e2e_us`, and `engine_completion_us` retain the actual engine metrics. The CLI uses the engine's normal response completion for colocated requests. P/D uses the existing copied-parent completion callback. Its projected engine latency can differ because token processing is accounted separately. The deterministic P/D fixture, for example, completes its first parent at 3,894 microseconds while the engine completion metric is 3,914 microseconds. Both values remain visible.

Ordinary refusals save both reports before returning a failure. An engine or gateway startup failure can prevent aggregate metrics from existing. That path saves the workload report with `engine_available: false` and returns the original error.

The existing P/D engine assumes a nonempty decode output. A zero-output P/D request can reach `KVCacheState.allocate` with an empty output slice and panic. Semi-closed CLI replay rejects P/D deployments when any selected request has zero output. Colocated replay preserves those requests, and positive-output P/D replay is covered through the real cluster. No serving-engine behavior is changed here.

## Reproduce the checks

```bash
go test ./sim/workload/semiclosed
go test ./sim/cluster -run '^TestSemiClosed' -count=1
go test ./cmd -run 'Test(SemiClosed|ReplaySemiClosed)' -count=1
```

The committed synthetic fixtures exercise staggered births, positive ordinary and join delays, fan-out, overlap, background children, failures, duplicate callbacks, and zero output. The cluster tests cover positive-output P/D copied completions and prefill refusal. The CLI tests check deterministic reports, flag conflicts, startup failure reporting, and artifact preservation after refused work.

The AgentX comparison fixes the population to one or two families, starts at `MinRatio=MaxRatio=0`, disables both idle caps, suppresses recycled tree IDs at the test boundary, and stops at the final selected completion. Both paths receive identical prompt and output arrays before service. Synthetic spawn/join residuals and response-processing overhead are zero so the shared timing semantics coincide. The test compares every request's arrival, TTFT, E2E, completion, engine counters, and the entire aggregate engine output.

An additional opt-in test uses a complete original three-request family. It retains its long recorded gaps and exact source counts, totaling 12,575 prompt tokens and 467 output tokens. Generate it with the unchanged converter, then run the same engine comparison:

```bash
python -m research.freeinference_trace \
  --source traces/freeinference_agentic_trace/release \
  --week 2026-07-12_2026-07-18 --model glm-5.2 \
  --start-ms 1784204303061 --end-ms 1784204303062 \
  --output research/outputs/semiclosed-source-parity

BLIS_SEMICLOSED_SOURCE_FIXTURE="$PWD/research/outputs/semiclosed-source-parity" \
  go test ./sim/cluster -run '^TestSemiClosedSourceFamilyAgentXMetrics$' -count=1
```

The source test skips when that environment variable is unset. The synthetic comparisons run in the default suite. Existing AgentX and TraceV2 code, serving engines, the converter, and research-launcher defaults remain unchanged.

For the full repository suite, use the vendored catalog and registry fixtures:

```bash
env -u BLIS_CATALOG -u BLIS_REGISTRY \
  BLIS_SEMICLOSED_SOURCE_FIXTURE="$PWD/research/outputs/semiclosed-source-parity" \
  go test ./...
```

Validation on 2026-11-03 passed the full suite, the source-family AgentX comparison, and the 175-request CLI replay under both join policies. The independent verifier matched the original Parquet totals and causal timing in both runs. It also rejected a changed engine token total, a missing engine request, and a coherently shifted request time that violated its source dependency.
