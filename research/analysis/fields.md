# Saved data and statistic definitions

This reference describes the emitters at BLIS revision `47cffc8` and the report
format in this directory. Optional sections depend on the engine and recording
settings. `summary.json` records which fields are present in each supplied run.

## Request metrics

`metrics.json.requests[]` contains engine observations for registered requests.
Rows can include incomplete work whose missing latency values are serialized as
zero. Semi-closed analysis uses `workload.json` state as the completion authority.
Without that file, the parser uses positive `completion_index`, or positive E2E
for older rows lacking that field.

| Fields | Units or meaning |
| --- | --- |
| `requestID` | Stable engine request identity |
| `arrived_at` | Simulation seconds, despite neighboring latency fields using milliseconds |
| `ttft_ms` | Simulator time to first token |
| `scheduling_delay_ms` | Engine arrival to recorded scheduling; includes admission work and waiting |
| `e2e_ms` | Raw engine E2E, including host completion overhead |
| `itl_ms` | Engine inter-step timing observation; not necessarily per-token latency under speculative decoding |
| `num_prefill_tokens`, `num_decode_tokens` | Input length and requested output length |
| `output_tokens` | Emitted output count, when present; may differ from requested length after length capping |
| `handled_by`, `model` | Serving instance and request model tag; the tag is the source trace model in semi-closed runs |
| `session_id`, `round_index` | Session identity and turn number |
| `phase`, `completion_index` | Benchmark phase and completion order |
| `gateway_queue_delay_ms` | Optional gateway queue delay observation |
| `slo_class`, `tenant_id`, `adapter`, `length_capped`, `agentic` | Optional classification, outcome, and AgentX metadata |

The definitions are in [RequestMetrics](../../sim/metrics_utils.go).
The normalized CSV uses `request_id`, `trace_model`, `engine_e2e_ms`,
`engine_itl_ms`, and simulation-second arrival/completion columns. Token counts
come from the source workload when available. Generic metrics-only rows use
engine counts. Unemitted optional observations remain missing.

## Aggregate engine metrics

| Group | Available fields |
| --- | --- |
| Latency | TTFT/E2E mean, p50, p90, p95, p99; scheduling-delay p99 |
| Decode timing | `itl_*`, `step_interval_*`, `request_itl_*`, interactivity, output tokens per user |
| Work and throughput | Injected/completed/queued/running requests; input/output tokens; requests/s and tokens/s; measured-window duration and source |
| Outcomes | Failed, timed-out, cancelled, context-overflow, length-capped, and dropped-unservable counts |
| Cache and pressure | Cache-hit rate, CPU-tier hit rate, preemptions, KV allocation failures when emitted, in-flight mean/max, KV capacity tokens per replica |
| Optional SLO output | Goodput, attainment, and per-class summaries |
| Optional P/D output | Transfer count and p50/p90 time; transfer-queue mean/p90; peak held KV; per-link peak concurrency; per-pool CPU-tier hit rate |
| Provenance | Catalog revision, BLIS/kernel/schema versions, scenario overrides, kernel evidence, speculative acceptance, prefix-cache configuration |

See [MetricsOutput and PDOutput](../../sim/metrics_utils.go) and
[MeasuredMetrics](../../sim/metrics_measured.go). The report preserves aggregate
blocks in `summary.json` under `emitted_aggregates`.

Top-level measured `e2e_*` removes host completion overhead. Request rows and
`whole_run.e2e_*` retain it. That overhead is not emitted per request, so exact
measured `request_itl_*` cannot be reconstructed from the raw request rows.
Measured aggregate latency also excludes warmup and respects an explicit
measurement completion window. The row export does not expose all window bounds.

The report's request plots use completed, non-warmup rows. They are explicitly
separate from the measured aggregate population. Zero-output requests retain
simulator TTFT when the engine records it, matching the existing simulator
convention. Each statistic records its available sample count. Missing and
negative latency values do not become zero-duration observations.

## Semi-closed lifecycle data

`workload.json` has the following records. Simulation timestamps use
microseconds; original trace timestamps use epoch milliseconds.

| Record | Available fields and interpretation |
| --- | --- |
| Run | `complete`, `accounting_match`, `engine_available`, selected totals, engine totals, request states, tokens by state, source manifest/hash, origin, end, join policy/assumption |
| Request | Request/family/session identity, step, source start/end and tokens, state/reason, arrival/completion/terminal, TTFT, lifecycle E2E, engine E2E/completion, recorded delay, dependency prerequisites |
| Session | Family/session identity, arrival/completion, states, spawn request, inferred join request |
| Family | Family identity, birth/arrival/completion, `makespan_us`, states |

The existing [semi-closed report](../../sim/workload/semiclosed/report.go) already
provides complete-family makespan. It starts at the root's actual arrival and
ends at the last completion across all descendant sessions. Session lifetime is
that session's completion minus its first arrival. Null completion times remain
missing. These durations include recorded non-LLM delays and dependency waits.
They are not sums of request latencies, since branches can overlap.

These fields do not yet constitute a full fork/branch/join makespan recorder.
Observed causal barriers, final tools after the last recorded LLM request, and a
decomposition of the critical path are not available as independent measurements.
The [makespan research notes](../../MAKESPAN_RESEARCH.md) describe those extensions.

## TTFT bar decomposition

For a run with an explicitly recorded zero preemption count and valid request
measurements, the current report splits TTFT into:

1. `scheduling_delay_ms`, labeled **admission + scheduling delay**.
2. TTFT minus that delay, labeled **remaining time to first token**.

The first segment includes host preprocessing and queue waiting. The remainder
can include prefill steps, chunk scheduling gaps, mixed-batch work, first-token
processing, and P/D handoff/decode work. Neither segment measures pure queue or
prefill compute. Gateway delay is not assumed to be a third disjoint component.

The [scheduling recorder](../../sim/simulator.go) can overwrite scheduling delay
when a preempted request is admitted again. TTFT keeps its first value. The
current format has no per-request preemption count, so the report suppresses the
split for the entire run when preemptions are nonzero or unknown. Missing,
negative, or larger-than-TTFT scheduling delays also produce unsplit total bars.

For the mean, the component means add to mean TTFT. For a quantile, requests are
sorted by total TTFT, with request ID breaking ties. The same interpolation
weights are applied to each component of the two rows bracketing the total's
quantile rank. The segments therefore add to total TTFT p50/p95/p99; they are not
the individual components' p50/p95/p99 values. Percentiles follow the
[NumPy linear method](https://numpy.org/doc/stable/reference/generated/numpy.quantile.html),
which BLIS also uses.

Future queue/prefill/KV bars need per-request measurements with aligned
boundaries and an explicit rule for overlaps. They also need first-admission
timestamps that survive preemption. Aggregate transfer percentiles, cache-hit
rates, and transferred token counts cannot supply a KV-fetch time component.

## Optional server series

`metrics.json.server_series` contains an interval, clock origin, and per-engine
arrays. It is absent when `run.series_interval_s` is zero.

| Fields | Interpretation |
| --- | --- |
| `num_requests_waiting`, `num_requests_running`, `kv_cache_usage` | Time-weighted mean gauges per bin; KV occupancy is a fraction |
| Corresponding `_last` fields | Last published gauge values at the bin endpoint |
| `prefix_cache_queries`, `prefix_cache_hits` | Per-bin counter increments |
| `prompt_tokens`, `generation_tokens` | Per-bin token increments |
| `prompt_tokens_local_compute`, `prompt_tokens_local_cache_hit`, `prompt_tokens_external_kv_transfer` | Prompt token accounting by source, not elapsed time |

The [series recorder](../../sim/server_series.go) defines the bin semantics.

## Policy pairing

The report pairs complete families by identity only when their source work also
matches. The signature includes every request ID, session/step, original
timestamps, input/output token counts, recorded delay, and prerequisite edge.
It also includes the trace model, recorded source-file hashes for that week,
converter code hash, and conversion rules, including prefix mapping. Missing
provenance excludes a pair rather than establishing identical work.
Coverage reports unmatched families, incompatible work, incomplete matches, and
`insufficient_provenance`.
Normalized change is `100 * (candidate / baseline - 1)`; a zero baseline duration
has no percentage change. Each run remains separate, including repeated seeds.
