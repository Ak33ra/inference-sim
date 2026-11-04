# DeepSeek H200 comparison

The 2026-11-03 study tests whether one or two individual H200 GPUs with a draft
model reproduce FreeInference's DeepSeek timings. The requested substitution
selects `deepseek-v4-flash` requests and serves them with `deepseek-v4.1-flash`.
BLIS supports H200 and DSpark, but neither requested GPU count fits this catalog
model. No configuration is promoted as a research baseline.

A four-H200 diagnostic completes both selected cohorts and conserves every
request and token. Its latency distributions fail the agreement rule. This
diagnostic cannot establish the hardware used for the original V4-Flash trace.

## Memory feasibility

All memory figures below are GiB per GPU. The catalog supplies 139.80072021 GiB
of device memory per H200. The kernel uses native checkpoint precision with
CPU Engram offload enabled.

| GPUs per replica | Target weights | Target and DSpark weights | Result |
| --- | ---: | ---: | --- |
| 1, TP1 | 279.556 | 287.054 | Weights exceed device memory |
| 2, TP2 | 140.139 | 144.008 | Weights exceed device memory |

The DSpark weight totals are identical for draft lengths 1, 3, and 5. Enabling
expert parallelism gives the same weight bound at TP1 and TP2. Even target-only
TP2 exceeds physical memory before activations, CUDA graphs, communication,
or KV storage. Reducing context, batch size, or draft length cannot remove that
weight deficit. The configured TP1 and TP2 startup checks fail before replay.

This is a bound for the substituted V4.1 model and its modeled deployment. It
does not prove that the original V4 model, another checkpoint precision, or
another offload implementation could not run on one or two H200s. Native
Hopper expert weights remain MXFP4 with Marlin W4A16 compute in this kernel.

The [feasibility logs](../outputs/deepseek-h200/feasibility.json) and
[direct kernel weight probe](../outputs/deepseek-h200/memory-breakdown.jsonl)
retain the evidence. The
[scenario files](../configs/diagnostics/deepseek-h200/) are diagnostic artifacts.
TP1 and TP2 intentionally represent infeasible deployments.

## Fixed selections and accounting

Each selection includes every eligible single-model family whose root starts
in the stated half-day UTC window. Every selected family drains, including
descendants after the window. Root arrivals, token lengths, known prefix
identities, and recorded non-LLM delays are preserved. Later requests follow
simulated completion. There is no family recycling or fixed concurrency cap.

| Cohort | Root window, UTC | Families | Sessions | Requests | Prompt tokens | Output tokens |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Discovery | July 17, 00:00 to 12:00 | 30 | 36 | 4,288 | 1,109,392,536 | 1,417,394 |
| Holdout | July 24, 00:00 to 12:00 | 33 | 36 | 1,202 | 89,999,931 | 341,748 |

The total is 5,490 requests, 1,199,392,467 prompt tokens, and 1,759,142 output
tokens. Source Parquet rows, replay reports, and engine counters agree exactly.
There are no zero-output requests in these cohorts. Prompt counters count
logical input tokens, including cached tokens; they are not uncached prefill
compute counts or draft verification work.

Both cohorts contain provider `A` within their respective weeks. Provider letters
are anonymous and scoped by week. They do not establish a common server.
Mixed-model families and roots outside each window remain excluded. Unselected
traffic, prior cache state, and provider queueing are not reconstructed.
The initial July 14 window had no eligible families. The switch to July 17 and
July 24 happened before simulator latency results were inspected.

The [selection record](../outputs/deepseek-h200/actual-selection.json) includes
window boundaries, source revisions, hashes, branch coverage, and exclusions.

## Four-H200 diagnostic

The scenario uses one colocated replica with TP4, PP1, DP1, DSpark K5,
CPU Engram offload, 1,048,576-token context, 8,192 batched tokens, 1,024 sequence
slots, 64-token blocks, FP8 cache, and GPU memory utilization 0.8. The registry's
`coding`, `thinking-off` profile supplies mean acceptance length 4.07.
That acceptance profile was measured on B200 TP4, not this H200 workload.

BLIS advances decode by a deterministic mean with fractional carry. Changing
the seed does not sample alternative draft acceptances. Repetition therefore
checks reproducibility; it does not quantify real-server or acceptance-model
uncertainty. All figures below are simulated time, not host execution time.

Seeds 42 through 46 complete five runs per cohort. All ten runs pass the original
source and engine verifier with zero failed or unfinished requests. Within each
cohort, the full engine metric objects are identical across seeds. The table
therefore gives both the median across runs and each range's identical minimum
and maximum. The [aggregate results](../outputs/deepseek-h200/results-summary.json)
retain the run count, ranges, verification status, and agreement decisions.

| Cohort | Metric | Trace p50 | Simulator p50 | Trace p95 | Simulator p95 |
| --- | --- | ---: | ---: | ---: | ---: |
| Discovery | E2E, ms | 2,331.50 | 2,913.79 | 15,888.40 | 11,549.64 |
| Discovery | TTFT, ms | 1,308.00 | 1,978.80 | 3,476.20 | 9,736.62 |
| Discovery | TPOT, ms/token | 4.176 | 2.556 | 7.406 | 4.872 |
| Holdout | E2E, ms | 1,440.00 | 1,091.70 | 20,713.45 | 4,691.63 |
| Holdout | TTFT, ms | 364.00 | 618.59 | 5,951.50 | 2,285.23 |
| Holdout | TPOT, ms/token | 5.406 | 2.541 | 11.623 | 2.690 |

E2E includes all 4,288 discovery and 1,202 holdout requests. Trusted TTFT and
TPOT include 3,495 and 1,186 requests respectively. The comparator keeps
streaming rows with finite `0 < TTFT < 0.95 * E2E`, following the publisher's
[TTFT quality heuristic](https://github.com/HarvardMadSys/freeinference_agentic_trace/blob/925a718eed7cc01255e7dcea024e70389cf021e5/paper/metrics/latency.py).
TPOT additionally requires more than one output token and uses
`(E2E - TTFT) / (output_tokens - 1)` on both sides. It does not use engine ITL,
whose speculative-step interpretation can differ. Percentiles interpolate
linearly over identical paired request sets. The TTFT heuristic can exclude
legitimate prefill-heavy requests; those requests remain in E2E comparisons.

The decision rule was recorded before the simulations. A candidate must be
feasible, conserve all work, and have p50 and p95 E2E, TTFT, and TPOT ratios
between 0.8 and 1.2 on both cohorts. Median paired absolute relative E2E error
must be at most 30%. TP4 fails multiple distribution checks. Its paired E2E
error is 50.63% on discovery and 28.69% on holdout. No latency coefficients,
acceptance overrides, or capacity overrides were fitted to these observations.

## Interpreting the difference

Family makespans appear closer than request latencies. Discovery median family
makespan is 22.01 minutes in the trace and 20.15 minutes simulated. Holdout is
8.15 versus 7.64 minutes. The replay copies non-LLM delays, so this agreement
does not establish an accurate serving cost model.

Source token-weighted cache reads are 99.36% and 92.91% of prompt tokens.
The simulated cache-hit rates are 90.10% and 82.99%. These measures have
different provenance and cache semantics. About 9.02% and 8.27% of source
prompt tokens have no mapped prefix identity in the converted cohorts.
Family-local prefix namespaces and a cold selected replay also restrict reuse.
These observed differences make cache reconstruction a material confounder
for TTFT. Their causal contribution has not been isolated.

The diagnostic has zero preemptions and zero gateway queue delay. Its maximum
in-flight request counts are six and three. Neither the 1,024 sequence cap nor
KV-capacity pressure limits these runs. Sparse arrivals and recorded delays
limit aggregate throughput; per-request serving cost and engine scheduling
affect latency. The original server's bottleneck cannot be inferred from these
traces alone.

Kernel provenance contains 117 measured, 49 assumed, 14 vendor-spec, and two
uncharged resolved parameters. These counts cover the loaded parameter set,
not necessarily parameters exercised by TP4. H200 support therefore does not
mean every part of this model/hardware combination has direct calibration.
The V4 to V4.1 substitution, unknown original draft configuration, transferred
acceptance profile, and missing external traffic prevent hardware identification
from an accidental aggregate match.

A useful next comparison needs the original serving model, checkpoint precision,
offload policy, and draft configuration, or a V4-Flash graph matching those
facts. The present evidence supports retaining this as a negative feasibility
result, with TP4 as a diagnostic only.

## Reproduce and inspect

The [run instructions](deepseek_h200_reproduce.md) provide conversion, replay,
comparison, and repetition commands. Each successful run includes its exact argv,
logs, workload report, engine metrics, comparison JSON, and paired request CSV.
The initial `tp4-k5-off-holdout-seed42/` directory contains a rejected CLI
invocation with an unsupported `--timeout` flag. The corrected successful run
is `tp4-k5-off-holdout-seed42-r1/`. That CLI error is not a latency observation.
The study uses these versions:

| Input | Version |
| --- | --- |
| BLIS base | `6a821db3cbe807e6dbcf26dffad2564936b891a6`, plus this checkout's semi-closed changes |
| Catalog | `676326ec6c708e91ec4981bd6b03922a7fe60512`, release 0.3.0 |
| Registry | `7f6662d7188dc3dc873a37623e1f7bbfe1d6081b`, release 0.2.0 |
| Latency kernel | 0.3.0 |
| Schemas | 0.7.0 |
| FreeInference release | `16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade` |
| Python / PyArrow | 3.12.13 / 24.0.0 |

The [discovery comparison](../outputs/deepseek-h200/tp4-k5-off-discovery-seed42-r1/comparison.json)
and [holdout comparison](../outputs/deepseek-h200/tp4-k5-off-holdout-seed42-r1/comparison.json)
retain paired errors, makespans, eligibility counts, and token-length groups.
Outputs are local research artifacts and remain ignored by Git. Scenario and
comparison code live separately from serving engines and existing replay paths.
The comparator's eight focused tests pass. An independent reviewer reran both
seed-42 comparisons and reproduced their saved JSON exactly.

The Explain the Number principle required the memory proof and a declared
agreement rule before interpreting latency. Exact work accounting prevents
incomplete or rejected requests from appearing to improve performance.
