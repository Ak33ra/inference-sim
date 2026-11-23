**Request SPLIT at 2-second arrivals.** Median paired mean family makespan changes by **+0.84%** versus JSQ/FCFS, with a five-seed range of +0.83% to +0.95%. Mean session elapsed time changes by **+0.85%**.
The mean-family gap shrinks from +2.11% at 4-second arrivals to +0.84% at 2 seconds. JSQ has the lower mean family makespan in all five new seed pairs.

This repeats the existing comparison with the fully aggregated mega trace. It includes all eight Commit0 fanout buckets and natural Oolong, excludes FreeInference, and contains **287 families, 2,385 sessions and 58,741 requests**. Each source appears once.

The cluster has eight replicas, each with one B200 running GPT-OSS-120B MXFP4 at TP=1. Prefix caching is off. Native limits remain 16,384 batched tokens, 1,024 sequences, 131,072 model tokens and 85% GPU memory utilization.

The only workload change is a **2-second mean Poisson family-root interarrival**, or 0.5 families/s. The first root arrives at zero. Family order, arrival seeds 42–46 and simulator seed 42 are unchanged. Fresh root schedules match exactly between policies within each seed. Compared with the 4-second runs, root birth times halve to microsecond rounding. Tool delays and DAG dependencies remain unchanged, and every family drains.

SPLIT uses the same shared ready queue, seven replicas taking the shortest request and one taking the longest. Requests finish without preemption and retain native batching. Request size uses the unchanged isolated-service oracle for exact input/output lengths. Both policies use the exact frozen binary and slowdown references from the 4-second study.

Each cell is **mean / median / p99**. Each number is the median of that statistic across five runs. Elapsed times are seconds.

| Metric | JSQ/FCFS | Request SPLIT |
|---|---:|---:|
| Session elapsed (s) | 88.75 / 54.71 / 794.88 | 89.51 / 54.91 / 798.92 |
| Session slowdown | 2.946 / 2.774 / 6.202 | 2.967 / 2.793 / 6.254 |
| Family makespan (s) | 286.09 / 197.49 / 1052.42 | 288.50 / 195.74 / 1054.03 |
| Family slowdown | 3.173 / 2.919 / 6.513 | 3.203 / 2.940 / 6.628 |

Paired changes are **median [minimum, maximum] percent change** across seeds. Positive means slower. These ranges describe seed variation, not confidence intervals. Paired changes need not equal ratios of the displayed cross-seed medians.

| Metric | Mean change (%) | Median change (%) | p99 change (%) |
|---|---:|---:|---:|
| Session elapsed (s) | +0.85 [+0.83, +1.09] | +0.63 [+0.12, +0.85] | +0.63 [-0.05, +1.05] |
| Session slowdown | +0.48 [+0.25, +1.36] | +0.55 [-0.07, +1.00] | +0.37 [-0.55, +3.79] |
| Family makespan (s) | +0.84 [+0.83, +0.95] | +0.86 [-1.77, +3.68] | +0.32 [+0.15, +0.92] |
| Family slowdown | +1.33 [+0.58, +1.68] | +1.66 [+0.12, +2.98] | +0.38 [-0.56, +2.31] |

Slowdown divides each object’s elapsed time by the same saved best-observed isolated-family reference used before. Sibling sessions compete in those references and tool delays remain included. These are distinct from the request-size oracle and are not proven session-alone optima.

The following diagnostics show medians of five per-run request means, or admission fractions. GPU admission wait excludes preparation service and its queue.

| Diagnostic | JSQ 4s | JSQ 2s | SPLIT 4s | SPLIT 2s |
|---|---:|---:|---:|---:|
| Request latency (ms) | 1144.02 | 1984.37 | 1182.13 | 2006.21 |
| TTFT (ms) | 128.94 | 144.95 | 107.40 | 111.86 |
| First token to completion (ms) | 1015.08 | 1839.42 | 1074.65 | 1894.29 |
| Preparation wait (ms) | 10.48 | 16.45 | 1.33 | 1.91 |
| GPU admission wait (ms) | 9.313 | 15.423 | 0.818 | 1.455 |
| Multiple ready requests at admission (%) | n/a | n/a | 4.73 | 9.34 |

As in the earlier comparison, SPLIT sends preparation round-robin before GPU assignment, while JSQ couples preparation and GPU placement. This experiment measures the complete policies, including central dispatch and batching. It does not isolate priority ranking.

Every run completes all **722,128,759 input tokens and 10,121,700 output tokens** with matching physical engine totals. All **60,551 dependency edges** and recorded delays are preserved. No requests fail, time out, drop, hit length caps or preempt. SPLIT admission priorities, renderer timing, native capacity and final KV/reservation drain are audited. Independent calculations match 120 per-run statistics and 60 paired changes. The new configs differ from the 4-second configs only in arrival rate and output directory.

- [Study/configurations and frozen provenance](../../outputs/mega-request-split-2s/study.json).
- Plotting data: [sessions](../../outputs/mega-request-split-2s/analysis/sessions.csv), [families](../../outputs/mega-request-split-2s/analysis/families.csv), [requests](../../outputs/mega-request-split-2s/analysis/requests.csv), [paired changes](../../outputs/mega-request-split-2s/analysis/paired-deltas.csv), [per-run statistics at both arrival rates](../../outputs/mega-request-split-2s/analysis/comparison-by-arrival.csv).
- [Independent numerical and arrival audit](../../outputs/mega-request-split-2s/independent-numerical-audit.json) and [previous 4-second report](../mega-request-split-4s/README.md).

Rerun or reuse the verified experiments and regenerate the tables from the repository root:

```bash
python3 -B research/scripts/request_split_arrivals.py run research/outputs/mega-request-split-2s --jobs 4
python3 -B research/scripts/request_split_arrivals.py analyze research/outputs/mega-request-split-2s
python3 -B research/outputs/mega-request-split-2s/review/check_numerical_results.py research/outputs/mega-request-split-2s
```

Raw outputs live in `/scratch/akira/research-outputs/mega-request-split-2s`, linked from `research/outputs/mega-request-split-2s`.
