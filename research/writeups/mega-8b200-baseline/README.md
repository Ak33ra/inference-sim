# Mega trace: eight-B200 medium-load baseline

The baseline combines all eight Commit0 buckets and Oolong once each. Poisson
root arrivals with a **12-second mean gap** give a median **70.1% request-active
time** across five arrival seeds (range **64.2–71.6%**). This measures time with
at least one scheduled request at a replica, including host overhead; it is not
GPU/SM utilization or a percentage of maximum throughput.

Run the prepared trace from the repository root:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/mega-8b200-medium.json
```

The config writes to `research/outputs/semiclosed/mega-8b200-medium`. Use
`--output PATH` with a fresh directory for repeats. The measured runs are saved
separately as `research/outputs/semiclosed/mega-8b200-poisson-12s-seed{42..46}`.
The final config resolves to the measured seed-42 settings, with a new output
location.

The input is [input.jsonl.zst](../../../traces/mega-commit0-oolong/input.jsonl.zst).
Commit0 paths are `traces/commit0-fanout-buckets/<bucket>/traces.jsonl.zst`;
Oolong is at the repository root. FreeInference is excluded.

| Source | Root families | Sessions, including roots | Requests |
| --- | ---: | ---: | ---: |
| w00-04 | 203 | 590 | 16,011 |
| w05-09 | 26 | 314 | 11,277 |
| w10-14 | 16 | 293 | 10,123 |
| w15-19 | 6 | 168 | 6,256 |
| w20-24 | 4 | 111 | 3,900 |
| w25-29 | 3 | 96 | 2,971 |
| w30-34 | 3 | 108 | 3,986 |
| w65-69 | 1 | 72 | 2,609 |
| natural-oolong-fanout.jsonl.zst | 25 | 633 | 1,608 |
| **Total** | **287** | **2,385** | **58,741** |

Every run processes **722,128,759 prompt tokens** and **10,121,700 output
tokens**. All sources declare GPT-OSS-120B. Family/session IDs are namespaced;
request data, dependencies, token counts and non-LLM delays are preserved.

Whole families are shuffled once with order seed 42 and pinned in the input.
This spreads the unequal source populations across the run instead of ending
with 177 consecutive families from the smallest fanout bucket. Source weights
remain their original counts; the mixture is not balanced across buckets.
The manifest records the source hashes and exact permutation. Its uncompressed
input SHA-256 is `e5aaa2f4370f77f95dda1d635cfac7c33b63f611d2ff571ccc42922488cd02c0`.

| Serving setting | Value |
| --- | --- |
| Model | GPT-OSS-120B, `mxfp4` |
| Cluster | Eight nodes, one B200 and one TP1 replica per node; PP1, DP1; `ib-400g` |
| Router | JSQ, counting waiting + running requests; flow control and implicit utilization filtering off |
| Prefix caching | Off; measured cache hit rate zero |
| Batch limits | 16,384 tokens, 1,024 sequences |
| Context / memory | 131,072-token context, GPU memory utilization setting 0.85 |
| Engine | vLLM model version 0.31.0; `FULL_AND_PIECEWISE` CUDA graphs |

Only root families receive generated arrival times. The first arrives at zero;
subsequent gaps are independent exponential draws at **0.08333333333333333
roots/second**, or five roots per minute on average. Within a family, requests
wait for simulated parent completions plus the recorded non-LLM delay. Joins
wait for all parents. Every family drains once; there is no concurrency cap or
recycling. Router seed and trace-order seed stay 42; arrival seeds vary 42–46.

For smoother arrivals, replace `run.arrivals` with
`{"mode":"fixed","interval_s":12}`. That changes the experiment and needs a new
load check. Increasing Poisson `rate_per_s` raises offered load. Keep the same
input and arrival seeds/rate across policy comparisons so every policy faces
the same root schedule.

We selected the rate using these full-trace seed-42 pilots. The target was
roughly 65–75% request-active time with modest scheduling delay; this was an
arrival-rate calibration, not a router or engine performance comparison.

| Mean gap | Roots/s | Request-active time | Scheduling p95 (ms) | Family median makespan (s) |
| --- | ---: | ---: | ---: | ---: |
| 16s | 0.06250 | 56.6% | 79.5 | 66.0 |
| 12s | 0.08333 | 70.4% | 84.1 | 68.2 |
| 8s | 0.12500 | 88.3% | 98.2 | 72.9 |
| 4s | 0.25000 | 99.9% | 129.9 | 108.9 |

Load is measured over the central 80% of the root-arrival interval, rounded
inward to complete one-second bins. For each replica, we take the union of
intervals from initial scheduling to client completion, clip to that window,
and average the eight fractions. All runs have zero preemptions, which is
required to reconstruct these intervals from saved scheduling delays.
Scheduling percentiles use requests arriving inside the window and include
admission overhead. Mean scheduler waiting count is only 0.00169–0.00209 per
replica across baseline seeds, so this is a moderate-activity baseline with
little admission queueing, not a queue-heavy stress test.

Family makespan includes all 287 families, measured from root birth until all
family work completes, including time outside the load-measurement window.

| Arrival seed | Request-active time | Scheduling p95 (ms) | Family median (s) | Family p90 (s) | Peak active families |
| --- | ---: | ---: | ---: | ---: | ---: |
| 42 | 70.4% | 84.1 | 68.2 | 420.2 | 23 |
| 43 | 69.5% | 85.9 | 68.8 | 408.2 | 26 |
| 44 | 71.6% | 82.6 | 68.2 | 418.8 | 20 |
| 45 | 64.2% | 84.7 | 68.6 | 420.0 | 23 |
| 46 | 70.1% | 83.7 | 65.7 | 413.4 | 21 |

Across seeds, median family makespan is 68.2s [65.7, 68.8], mean family makespan is
154.6s [153.9, 155.3], and family p90 is 418.8s [408.2, 420.2]. Each number is the median of the five
per-run statistics; brackets show the seed range. This finite heterogeneous
workload varies over time: seed-42 quarter activity ranges from 60.2% to 86.4%.
Its final root arrives at 3,220.0s and all work drains at 4,716.0s. Including
startup and final drain lowers that run's request-active fraction to 52.9%.

The fork analysis includes **161 multi-child forks** sharing a spawn and join.
It excludes 498 single-child groups and one terminal child with no parent join
from fork statistics only; their work remains in replay and family statistics.
Child finish times share the earliest child arrival as origin, so staggered
starts and recorded delays count. Fork makespan is the maximum child finish.
Ratios are calculated within each fork before taking percentiles. Table cells
are medians across five seeds of the stated percentile across forks.

| Fork statistic | Median | p90 |
| --- | ---: | ---: |
| Fork makespan | 36.69s | 94.07s |
| Mean child finish | 26.00s | 52.54s |
| Median child finish | 24.72s | 44.93s |
| Makespan / mean child finish | 1.58× | 3.49× |
| Makespan / median child finish | 1.61× | 5.20× |

All eight distinct runs (four pilot rates, then four additional baseline seeds)
completed all requests with exact token accounting, verified all 60,551 DAG
edges, and had zero failures, drops, truncations, timeouts, cancellations or
preemptions. No simulator execution path changed. The standalone preparation
module has three new tests; those, ten mixer tests and 17 config tests pass.
The Prove It Works check compares the prepared trace against the original
sources and the replay against the prepared trace; it does not infer correctness
from successful process exits alone.

The experiment used BLIS commit `4bd36cd75994b732be39e865395f2853b55b1ddc`
with this research-only working-tree addition, kernel v0.3.0, schemas v0.7.0,
and catalog revision `676326ec6c708e91ec4981bd6b03922a7fe60512`.
Saved run directories retain resolved configs, exact commands, input snapshots,
router snapshots, metrics and independent verification reports. Timings are
simulated; host process wall time is not an experiment result.

Artifacts: [calibration plot](../../outputs/analysis/mega-8b200-baseline/calibration.png),
[seed-42 timeline](../../outputs/analysis/mega-8b200-baseline/baseline-timeline.png),
[full baseline statistics](../../outputs/analysis/mega-8b200-baseline/baseline-summary.json),
[per-seed CSV](../../outputs/analysis/mega-8b200-baseline/baseline.csv), and
[pilot statistics](../../outputs/analysis/mega-8b200-baseline/pilot-summary.json).

To rebuild the trace, choose a new output directory or run these commands on a
checkout without the prepared directory. Source order is part of reproducibility:

```bash
sources=()
for bucket in w00-04 w05-09 w10-14 w15-19 w20-24 w25-29 w30-34 w65-69; do
  sources+=(--source "traces/commit0-fanout-buckets/$bucket/traces.jsonl.zst")
done
python3 -B -m research.semiclosed.prepare_mega build \
  "${sources[@]}" --source natural-oolong-fanout.jsonl.zst \
  --seed 42 --output traces/mega-commit0-oolong
```

Verify provenance, replay, or regenerate the analysis:

```bash
python3 -B -m research.semiclosed.prepare_mega verify traces/mega-commit0-oolong
research/scripts/semi_closed.sh verify research/outputs/semiclosed/mega-8b200-poisson-12s-seed42
python3 -B research/writeups/mega-8b200-baseline/analyze.py \
  research/outputs/semiclosed/mega-8b200-poisson-12s-seed{42,43,44,45,46} \
  --output research/outputs/analysis/mega-8b200-baseline/baseline-summary.json
uv run --with-requirements research/analysis/requirements.txt \
  python -B research/writeups/mega-8b200-baseline/plot.py
```

Full provenance verification needs the originals at the paths recorded in
`composition.json`. The compressed mega trace is otherwise a standalone input.

The [optrouting slowdown comparison](slowdown.md) measures loaded family and
fork durations against the available optimal isolated baselines.

The [eight-second arrival comparison](../mega-8b200-8s/README.md) keeps this
trace and serving setup fixed while increasing the root arrival rate by 50%.

The [prompt-length histogram and CDF](../../outputs/analysis/mega-trace-prompt-lengths/prompt-lengths.png)
describe all 58,741 requests using their exact `in` counts. Mean input length is
12,293.4 tokens, median 10,566, and p99 48,558. The
[summary](../../outputs/analysis/mega-trace-prompt-lengths/summary.json) includes
source-level statistics; the [request CSV](../../outputs/analysis/mega-trace-prompt-lengths/requests.csv)
contains every request's input length. Reproduce these with:

```bash
uv run --with-requirements research/analysis/requirements.txt \
  python -B research/writeups/mega-8b200-baseline/prompt_lengths.py
```
