# Agent fan-out makespan research

Research context and next steps from the discussion through **2026-10-30**.
This document distinguishes measured trace statistics, simulator checks, and planned work.

## Objective and current status

Study how replica count, routing, and KV-cache placement affect the time to finish
parallel agent branches and their parent workflows. Request latency alone does not
describe the slowest branch, time spent waiting at joins, or whole-family completion.

This repository is the group's BLIS fork, separate from the earlier personal
simulator in `../llm-routing-sim-fast-alpha`. BLIS was selected after considering
SGLang's simulator: the required serving features are multiple replicas, routing
decisions, and tiered KV storage. BLIS provides these plus existing agentic workload
hooks. Use the fork for shared implementation and its CLI for experiments.

**Completed:** local trace setup, full model/fan-out survey, BLIS reference checks,
GLM-5.2 trace baselines, synthetic cluster sanity checks, the
[FreeInference to Weka converter](research/freeinference_trace/README.md), and the
[research replay launcher](research/freeinference_replay/README.md). Initial BLIS
replays passed: the full pilot loaded all 207 families; a targeted family check
dispatched four children and exercised two parent joins.
**Planned:** validate branch timing/sharing, add complete-family makespan metrics,
and run contention experiments with verified fan-out coverage.

The AgentX baseline uses closed-loop replay with fixed family concurrency.
The [semi-closed replay](research/semiclosed/README.md) adds recorded root arrivals,
completion-driven internal work, and departure after every selected family drains.
It preserves raw trace token counts and reports complete-family makespans separately
from standard engine metrics. Serving engines and existing AgentX defaults remain
unchanged. The [AgentX limitations](research/AGENTX_LIMITATIONS.md) still apply to
the fixed-concurrency baseline.

## Lessons from the earlier API traces

The earlier API export includes completion timestamps and `timestamp_arrival`.
Using completions as simulated arrivals changes interarrival gaps and can reorder
overlapping requests. A true dependency `completion(A) <= arrival(B)` preserves
the order of A and B under either clock, but that does not preserve their gap,
global arrival order, or dependencies after simulated service times change.

A controlled API-week-1 rerun in the earlier simulator processed 1,783,198 requests
with the arrival clock: mean TTFT decreased 10.84%, p99 TTFT 14.52%, and mean E2E
7.92% relative to the completion-clock baseline. This was one fixed vLLM/alpha
configuration, not a policy-ranking study. Earlier results remain results for
their stated arrival process; neither universal invalidation nor equivalence is
justified. Recent studies there already used arrival-derived traces.
Evidence: `../llm-routing-sim-fast-alpha/results/api_arrival_clock_week1_20261027/RESULTS.md`.

Observed timestamps support offline duration/overlap analysis when grouping is
credible. Counterfactual makespan experiments additionally need dependencies:
fixed recorded arrivals cannot make a parent wait longer when simulated children slow down.

## Dataset and portable storage

- Source: [FreeInference agentic trace](https://huggingface.co/datasets/harvardMadsys/freeinference_agentic_trace/tree/16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade),
  pinned revision `16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade`.
- Downloaded release: approximately 8.8 GB, 16 weeks spanning May 17–September 5,
  2026; 1,222,199 requests, 14,036 sessions, and 12,002 root families.
- All workflows should read `traces/freeinference_agentic_trace/release/<week>/`.
  This machine symlinks the dataset root to `/scratch/akira/freeinference_agentic_trace`.
  Collaborators may symlink their own download or store it directly at that path.
  See [traces/README](traces/README); datasets and machine-specific symlinks are ignored.
- Prefer local Parquet, selected columns, and one week/model at a time. Optional
  finer windows should preserve complete families or mark boundary truncation.
  Dataset bytes alone do not predict runtime: long prompts and materialized token
  arrays can dominate memory and simulation work.
- Remote streaming or downloading selected weekly Parquet files are alternatives.
  Prefer fetching/normalizing before simulation and pinning the input; online
  fetching inside the event loop is unnecessary and complicates reproducibility.

Relevant input tables under `processed/`:

| Table | Useful fields |
|---|---|
| `requests.parquet` | `session_id`, `step`, `link_reason`, `start_ms`, `end_ms`, `ttft_ms`, `model`, `provider`, `stream`, `step_trigger`, `waited_for`, `prompt_tokens`, `completion_tokens`, `cache_read_tokens`, `role_tokens`, `n_messages`, `tool_definition_names`, `finish_reason`, `transition`, `cause`, `block_ids` |
| `sessions.parquet` | Session identity, `parent_session_id`, `spawned_by_step`, `spawned_by_call_index`, user/harness metadata, start/end |
| `tool_calls.parquet` | Session/step/call index, tool category, outcomes, inferred latency |

## Population and fan-out definitions

The survey distinguishes requests that call tools from requests inside a child
session. A fan-out candidate has at least two children attached to the same parent
session and step. An overlapping fan-out requires actual positive-duration LLM
interval overlap across distinct child subtrees; overlapping session envelopes
alone are insufficient. A single-model family includes every descendant.

Across the release: 35,617 subagent requests, 642 families with children, 288
same-step fan-out candidates, 271 overlapping groups, and 212 overlapping groups
inside entirely single-model families.

| Model | Requests | Requests calling tools | Subagent requests | Overlapping groups in single-model families |
|---|---:|---:|---:|---:|
| DeepSeek-V4-Flash | 528,165 | 488,298 | 14,037 | 62 |
| GLM-5.2 | 170,676 | 158,913 | 6,863 | 41 |
| GLM-5.1 | 153,360 | 143,129 | 5,279 | 74 |
| MiniMax-M3 | 105,519 | 98,433 | 1,334 | 3 |
| Qwen3.6-35B | 87,201 | 84,582 | 2,242 | 1 |
| GLM-5.3-Flash | 66,439 | 61,708 | 1,838 | 20 |
| Kimi-K2.7-Code | 51,608 | 49,242 | 2,351 | 7 |

**Selected initial model: GLM-5.2**, balancing traffic, subagent activity, and
usable fan-outs. GLM-5.1 is an attractive later expansion for more groups.
Treat each model as its own serving cluster initially. Filtering a mixed-model
family loses work: measure complete single-model families, or later coordinate
multiple model clusters for end-to-end mixed-model makespan.

Spawn links are publisher reconstructions using prompt/time matching, not
instrumented orchestration ground truth. The matching permits a child to start
before its spawning request ends. Parent-child links and `waited_for=subagent`
do not identify an exact join barrier. Same-step grouping also misses asynchronous
spawns across different parent steps. Audit intra-session overlap before assuming
every session is a sequential chain.

Evidence and reproducible survey: [scripts/survey_freeinference_models.py](scripts/survey_freeinference_models.py),
[survey outputs](results/freeinference_model_survey_20261027/summary.json).
The survey uses PyArrow and deliberately skips the large `block_ids` column.

## GLM-5.2 pilot and observed baselines

Initial week: **`2026-07-12_2026-07-18`**. It contains 20,013 GLM-5.2 requests,
736 subagent requests, and five entirely single-model families containing nine
overlapping fan-outs (978 requests in those families). Seven groups additionally
have nonnegative spawn delays and a subsequent parent request after all child
subtrees finish. These are initial fork/join candidates, not proven barriers.

- Prompt tokens: median 101,317; p95 584,707; maximum 928,285.
  Maximum prompt plus output is 936,742; median output is 170 tokens.
- Mean arrival rate 0.0331 requests/s; peak aligned second/minute 4/27 arrivals;
  observed peak in-flight requests 6. Retain surrounding model traffic when
  studying contention; simulating only the fan-out families removes background load.
- Recorded E2E: median 10.085 s, p95 48.760 s. Provider-reported token-weighted
  cache-read fraction is 96.62% among 19,462 requests reporting that counter.
- TTFT quality rule from the [publisher's analysis](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/925a718eed7cc01255e7dcea024e70389cf021e5/paper/metrics/latency.py):
  `0 < TTFT < 0.95 * E2E`. Only 6,151 requests pass; trusted-subset median TTFT
  is 7.308 s and TPOT 13.056 ms. Apply this heuristic only to TTFT/TPOT validation,
  not to workload inclusion or E2E. It can exclude legitimate prefill-heavy calls.

An offline branch duration is **last LLM completion minus first LLM arrival across
the entire child subtree**, including tool/idle gaps but excluding spawn delay.
Median per-group slowest/fastest ratios are **2.186** for the nine pilot groups
and **1.684** for all 41 GLM-5.2 groups; slowest/median ratios are **1.372** and
**1.306**, respectively. These are our measurements, not a verified reproduction
of a particular published “2x” figure. Common-start completion ratios are also saved.

The provider's hardware, competing traffic, and gateway overheads are unknown;
provider labels should not be assumed stable across weeks. These observations
are plausibility checks, not calibration targets that this cluster must exactly match.
Evidence: [trace_baseline.json](results/glm52_cluster_probe_20261027/trace_baseline.json).

## Agreed initial serving cluster

Exploratory configuration inspired by the [vLLM GLM-5.2 recipe](https://recipes.vllm.ai/zai-org/GLM-5.2),
not a reconstruction of the provider deployment.

| Component | Baseline |
|---|---|
| Model / engine | `glm-5.2-fp8`; engine version `0.31.0` |
| Placement | Two nodes, eight B200 GPUs/node; two replicas (`--num-instances 2`), one per node |
| Parallelism | TP=8, PP=1, DP=1 per replica; expert parallelism off |
| Serving | Colocated prefill/decode; FCFS, tail preemption; no autoscaling or MTP |
| Precision / cache | FP8 weights and KV; prefix caching enabled; 64-token blocks |
| Limits | Context 1,048,576; 8,192 batched tokens; 32 sequences; PIECEWISE CUDA graphs |
| GPU memory | 178.351 GiB/GPU catalog capacity; utilization 0.9; 160.516 GiB budget, 88.377 GiB fixed, 72.139 GiB allocatable KV |
| Logical KV capacity | Reported 26,938 blocks = 1,724,032 tokens per replica; account for the reserved null block; do not multiply logical capacity by TP=8 |
| Other cache tiers | **GPU-only initially.** CPU/NVMe/shared storage off; earlier CPU-offload discussion was a future experiment |
| Fabric | B200 NVLink nominal 900 GB/s/GPU/direction; `ib-400g`, nominal 50 GB/s/rail/direction |
| Frontend / telemetry | One API server and render worker per replica; gateway delay 0; metrics refresh 50 ms, jitter 0; KV-event delay 0; routing consumes zero simulated time |

The 32-sequence cap does not mean 32 simultaneous full-context requests fit.
Inter-node bandwidth has limited influence in this colocated baseline without
cross-node TP or prefill/decode handoff. Tier capacity, transfer bandwidth/latency,
and placement policy must be specified when offload is introduced.

Keep the existing **llm-d router**: score weights queue=2, KV utilization=2,
prefix match=3, each normalized to favor a less loaded or more cache-friendly
endpoint. Ties use deterministic round robin in the pinned fork. Prefix estimates
come from routing history, not exact cache inventory. Routing is per request and
has no fan-out objective; siblings may be placed together for prefix reuse.

Scenario: [glm-5.2-b200-tp8-2replicas.yaml](research/configs/clusters/glm-5.2-b200-tp8-2replicas.yaml).
Named experiments and cluster/workload controls live in
[research/configs](research/configs/README.md). One-, two- and four-replica
candidates are available; load calibration is deferred.
It uses the registry's `cost-model-{primitives,collectives,host-overheads,attention,recurrent,memory}` sets.
An H200 TP8 variant failed the full-context memory check, motivating B200.
Five B200 synthetic probes passed conservation with no drops: cold 131k/524k/936k
prompts plus 512 output tokens took about 9.865/25.658/45.530 s E2E; a four-request
131k burst improved median E2E from 23.970 s on one replica to 14.583 s on two.
These are simulator sanity checks, not FreeInference replay or hardware validation.

## Existing metrics and implementation path

BLIS already reports TTFT/E2E/ITL distributions, throughput, queueing, cache hits,
KV capacity, preemptions, and request outcomes. Save `--metrics-path` for full
aggregate/per-request output and provenance. Server time series require
`--server-series-interval-s`; routing diagnostics are opt-in (`--trace-level decisions`,
`--summarize-trace`). Do not assume stdout contains every metric or raw routing score.

Existing `SessionDuration` is latest recorded completion minus round-0 arrival
for one session ID. It neither groups a parent family nor verifies all expected
rounds completed. AgentX already supports fork/join execution, but its Weka/AIPerf
format, warmup, recycling, and prompt semantics are not a FreeInference adapter.
Implementation references: [session driver](sim/workload/session.go),
[AgentX joins](sim/workload/agentx_replay.go), [CLI wiring](cmd/replay.go),
[cluster hooks](sim/cluster/arrival_gate.go), and [session metrics](sim/cluster/metrics.go).

Earlier implementation proposal. The semi-closed replay implements the finite
driver and family accounting; its README defines the current scope and exclusions.
The broader proposal below remains research context:

1. **Python normalizer:** Parquet to compact JSONL plus a manifest, selectable by
   model/week/window. Preserve IDs, complete family structure, parent/spawn step,
   explicit or inferred join targets, absolute token counts, prefix identities,
   observed timings, and residual non-LLM delays. Report exclusions and ambiguity.
   Standard TraceV2 alone lacks parent/spawn/join metadata.
2. **Small Go driver in `sim/workload`:** recorded root arrivals; completion-driven
   session continuation, child spawning, and joins. Reuse the cluster completion
   callback and unserved hook; consult AgentX's counters and arrival gate. Count
   completed branches, not individual LLM calls. Support nested required joins,
   deterministic ordering, exactly-once release, failures, and horizon interruption.
3. **Makespan recorder:** exact simulation-clock fork/branch/join/family timestamps,
   branch durations and slowest/fastest/median ratios, completion spread, parent
   wait, and complete/failed/censored counts. Define fan-out makespan as fork
   release to last required branch completion; define whole-family completion
   separately. Boundaries initially remain LLM-visible, potentially omitting final tools.

Translation requirements for evaluating the AgentX approximations:

- **Timing:** sequential gaps exclude preceding LLM execution; join residuals
  exclude time already spent executing children. If children originally finish at
  10 s and the parent resumes at 12 s, preserve 2 s after the simulated join, not
  the original full wait. Do not silently clamp negative gaps or force background
  children into barriers. Fixed-arrival replay remains a useful control, not causal replay.
- **Prefix identity:** `block_ids` encode chained 16-token blocks using an
  `o200k` tokenizer, while provider prompt lengths and BLIS's 64-token blocks have
  different semantics. Design and document an approximation preserving sharing
  and serving lengths; random independent prompts lose the routing/cache signal.
  Keep absolute prompt lengths and compaction, rather than blindly accumulating turns.

## Validation, reproducibility, and next experiments

Completed BLIS checks: 14 selected reference tests passed, including golden,
AgentX/AIPerf, prefix-sharing, and offload cases. A four-replica, 200-request CLI
run/replay produced byte-identical stdout and metrics. This establishes execution
consistency, not reproduction of published GPU measurements; external measured
corpora were not available. See [validation summary](results/blis_validation_20261027/summary.json).

Local environment: `source .inference-sim-env/activate` selects Go 1.27.2 and
scratch-hosted dependencies. Checked BLIS revision:
`6a821db3cbe807e6dbcf26dffad2564936b891a6` (upstream `29f783c7126dc7425b0dece342f8edcb0f4e7168`);
catalog `0.3.0`, registry `v0.2.0`, latency kernel `v0.3.0`, schemas `v0.7.0`,
llm-d router fork `v1.0.0-blis.1`. Exact dependency SHAs are in the validation summary.
Every run requires catalog, registry, scenario directory, and scenario; the catalog
is authoritative and read-only. Pin inputs, code, seed, scenario, routing, horizon,
window selection, cache warmup, and timing/grouping assumptions in each result manifest.

Next work, in order:

1. Audit converted joins, branch membership, shared prefixes and changed delays.
   The converter retains complete eligible families and reports exclusions.
2. Add complete-family and fan-out metrics, accounting for sampled starts,
   recycling, failures and window censoring. Short runs select roots in corpus
   order and may miss fan-out families; verify branch coverage before analysis.
3. Run the two-replica baseline with surrounding model traffic; compare matching
   subsets and metric definitions against the offline baselines. Report selection
   counts and unfinished work; do not tune solely to obtain the remembered 2x ratio.
4. Then vary replica count/load, routing, and cache tiers/bandwidth individually;
   expand weeks/models once correctness is established. Any load scaling must
   state whether it changes only root arrivals or also internal delays.

Current analysis/probe outputs and helper scripts under `results/` are ignored,
local evidence. Before shared experiments, move reusable scenarios and runners
into tracked locations. The survey script is currently untracked. Do not commit
third-party trace data; retain portable `traces/` paths in shared workflows.
