# Central session SPLIT versus JSQ

The SPLIT-inspired adaptation is slightly slower than JSQ/FCFS in all five paired seeds. Mean session elapsed time increases by 2.2% and mean family makespan by 2.1%; mean, median and p99 slowdown also increase.

The workload combines all eight Commit0 fanout buckets and natural Oolong, excluding FreeInference: **287 families, 2,385 sessions and 58,741 requests**. Eight replicas each run GPT-OSS-120B MXFP4 on one B200, TP=1, with prefix caching off. Native limits remain 16,384 batched tokens, 1,024 sequences, 131,072 model tokens, 16-token KV blocks and 85% GPU memory utilization.

Family roots arrive with independent exponential gaps averaging **4 seconds** (0.25 families/s), starting at time zero in fixed family order. Arrival seeds 42–46 are matched between policies; simulator seed is 42. Requests follow recorded DAG dependencies and tool delays, and every family drains.

One job is one sequential session. All sessions can use all replicas: instance 0 chooses the largest remaining ready session; the other seven choose the smallest. A request waits centrally until native batch admission succeeds, then runs without preemption. Sessions can move between requests. Remaining size is the selected reference session’s completion time minus the current request’s reference arrival time; loaded queueing never counts as progress. This is an adaptation of [SPLIT](https://arxiv.org/pdf/2605.13749), whose original model has preemptive SRPT and an LJF server ranked by original size, without LLM batching.

Each cell below is **mean / median / p99**. Each number is the median of that statistic across five runs; elapsed times are seconds. A session ends when its last request completes; family makespan ends when its entire DAG finishes.

| Population / metric | JSQ/FCFS | Central SPLIT |
|---|---:|---:|
| Session elapsed (s) | 60.81 / 35.92 / 643.28 | 62.16 / 36.80 / 652.51 |
| Session slowdown | 1.812 / 1.712 / 3.465 | 1.846 / 1.749 / 3.647 |
| Family makespan (s) | 203.82 / 108.56 / 974.13 | 208.15 / 110.95 / 980.97 |
| Family slowdown | 1.845 / 1.662 / 3.902 | 1.917 / 1.713 / 3.975 |

Changes below are computed within each seed, then summarized as **median [minimum, maximum] percent change**. Positive means slower. These ranges are seed variation, not confidence intervals; paired changes need not equal the ratio of table medians.

| Population / metric | Mean change (%) | Median change (%) | p99 change (%) |
|---|---:|---:|---:|
| Session elapsed | +2.2 [2.0, 2.5] | +1.9 [1.5, 2.8] | +1.0 [0.7, 1.4] |
| Session slowdown | +2.1 [1.8, 3.0] | +2.3 [1.1, 3.0] | +5.2 [1.3, 5.8] |
| Family makespan | +2.1 [2.0, 2.3] | +2.4 [1.8, 4.5] | +0.7 [0.6, 0.9] |
| Family slowdown | +3.4 [2.7, 4.1] | +4.0 [2.6, 5.1] | +4.5 [1.7, 9.9] |

Slowdown divides each object’s elapsed time by the same saved best-observed reference from 35 cache-off isolated-family runs. Sibling sessions compete in those runs, and tool/dependency waits remain included. These are not proven session-alone optima; ratios below one are retained.

Diagnostic point values are medians of the five per-run means or fractions; brackets are seed minima and maxima. Mean request TTFT falls **128.94 → 107.23 ms**, while the mean span from first token to completion rises **1,015.08 → 1,075.76 ms**. Mean request latency rises **1,144.02 → 1,182.99 ms**. Only **4.64% [4.62%, 4.94%]** of SPLIT admissions have multiple ready requests to rank; mean central wait is **0.875 ms [0.836, 0.917]**. Priorities therefore have limited opportunities to change admission order. This comparison does not isolate the effect of ranking from central dispatch and batching.

Preparation retains eight native one-worker endpoints and identical kernel service (44.30 ms mean). SPLIT sends preparation round-robin before choosing a GPU; JSQ couples preparation and GPU placement. Mean preparation wait is **10.48 → 1.32 ms**. JSQ preparation waits were reconstructed from its ordered dispatch audit; SPLIT waits were checked against native renderer counters. This architectural difference favors SPLIT’s queueing result and is part of the measured configuration.

Every run completes **722,128,759 prompt tokens and 10,121,700 output tokens**, including matching physical local-compute totals and all **60,551 DAG edges**. All five new JSQ controls reproduce the frozen outputs exactly after normalizing only `catalog.path`. SPLIT’s audit verifies every min/max claim, native admission time and renderer queue. It has zero preemptions, allocation failures, capacity refusals or final KV/promise leaks; maximum batches at admission are 25–28. The independent calculation matches all 120 per-run statistics and 60 paired changes.

The affected Go checks and 71 Python tests pass, except a preexisting randomized P/D saturation failure reproduced identically before SPLIT (3/6 requests complete for the saved failing case). No unrelated P/D change is included. New code lives primarily in `sim/cluster/session_split*.go`, `sim/admission.go`, `sim/kv/lifecycle.go` and `research/session_split`; existing helpers and defaults remain unchanged.

- [Run instructions and policy scope](../../session_split/README.md).
- [Frozen configs, binary/source hashes and run matrix](../../outputs/mega-session-split-4s/study.json).
- [Sessions](../../outputs/mega-session-split-4s/analysis/sessions.csv), [families](../../outputs/mega-session-split-4s/analysis/families.csv), [paired sessions](../../outputs/mega-session-split-4s/analysis/paired-sessions.csv) and [paired families](../../outputs/mega-session-split-4s/analysis/paired-families.csv) for plotting.
- [Per-run statistics](../../outputs/mega-session-split-4s/analysis/per-run.csv), [aggregate ranges](../../outputs/mega-session-split-4s/analysis/aggregate.csv), [paired changes](../../outputs/mega-session-split-4s/analysis/paired-deltas.csv), and [independent numerical/latency audit](../../outputs/mega-session-split-4s/independent-numerical-audit.json).

Regenerate the tables and verify them from the repository root:

```bash
python3 -B -m research.session_split.analysis research/outputs/mega-session-split-4s
python3 -B research/outputs/mega-session-split-4s/analysis/independent_check.py research/outputs/mega-session-split-4s
```
