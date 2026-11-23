**Request-level SPLIT versus JSQ, 4-second arrivals.** Request-level SPLIT increases mean session elapsed time by **2.17%** and mean family makespan by **2.11%**. Both means increase in all five paired seeds. It reduces request TTFT, but does not improve makespan on this workload.

The mega trace combines all eight Commit0 fanout buckets and natural Oolong, excluding FreeInference: **287 families, 2,385 sessions and 58,741 requests**. Eight replicas each run GPT-OSS-120B MXFP4 on one B200, TP=1, with prefix caching off. Native limits are 16,384 batched tokens, 1,024 sequences, 131,072 model tokens, 16-token KV blocks and 85% GPU memory utilization.

Family roots arrive in fixed family order with exponential gaps averaging **4 seconds** (0.25 families/s), starting at time zero. Arrival seeds 42–46 are matched between policies; simulator seed is 42. Requests retain their recorded DAG dependencies and tool delays, and every family drains.

SPLIT uses one shared ready queue. Replica 0 takes the longest ready request; replicas 1–7 take the shortest. Each claim occurs at native batch admission; admitted requests stay on that replica and finish without preemption. All replicas retain native chunked prefill and batching. Blocked dependencies are absent from the ready queue. Request size is the native isolated client-visible service time for its exact input/output lengths, including preparation and output host service. Priorities remain fixed. Calibration covers all **56,134 distinct shapes**, with representative shapes also checked in reverse order and standalone runs.

Each cell below is **mean / median / p99**. Each number is the median of that statistic across five runs. A session ends when its last request completes; family makespan ends when its entire DAG finishes.

| Metric | JSQ/FCFS | Request SPLIT |
|---|---:|---:|
| Session elapsed (s) | 60.81 / 35.92 / 643.28 | 62.13 / 36.53 / 649.87 |
| Session slowdown | 1.812 / 1.712 / 3.465 | 1.840 / 1.742 / 3.595 |
| Family makespan (s) | 203.82 / 108.56 / 974.13 | 207.93 / 112.53 / 978.82 |
| Family slowdown | 1.845 / 1.662 / 3.902 | 1.915 / 1.722 / 3.920 |

Paired changes are computed within each seed, then summarized as **median [minimum, maximum] percent change**. Positive means slower. These ranges describe seed variation, not confidence intervals; paired changes need not equal ratios of the table medians.

| Metric | Mean change (%) | Median change (%) | p99 change (%) |
|---|---:|---:|---:|
| Session elapsed (s) | +2.17 [+1.96, +2.26] | +1.70 [+1.05, +2.57] | +1.02 [+0.53, +1.40] |
| Session slowdown | +1.67 [+1.21, +2.44] | +1.94 [+1.42, +2.75] | +2.09 [-0.72, +5.68] |
| Family makespan (s) | +2.11 [+2.01, +2.25] | +3.65 [+2.00, +7.11] | +0.56 [-0.01, +0.83] |
| Family slowdown | +3.75 [+2.85, +3.94] | +3.51 [+2.78, +5.31] | +5.23 [-0.89, +8.67] |

Slowdown uses the same saved best-observed references from 35 cache-off isolated-family runs for both policies. Sibling sessions compete in those references, and tool/dependency waits remain included. These denominators are distinct from the single-request sizing oracle and are not proven session-alone optima; ratios below one remain valid.

Diagnostic values below are medians of the five per-run request means:

| Request metric (ms) | JSQ/FCFS | Request SPLIT |
|---|---:|---:|
| Scheduling delay | 63.94 | 46.45 |
| TTFT | 128.94 | 107.40 |
| First token to completion | 1015.08 | 1074.65 |
| Total latency | 1144.02 | 1182.13 |

Only **4.73% [4.43%, 4.88%]** of SPLIT admissions have multiple ready requests to rank. Mean central wait is **0.818 ms [0.758, 0.888]**. Ranking therefore has limited opportunity to change admission order. The longer first-token-to-completion span outweighs the TTFT reduction. This comparison changes central dispatch and batching as well as ranking; it does not isolate their individual effects.

Preparation retains eight native one-worker endpoints with identical mean service, 44.30 ms. SPLIT sends preparation round-robin before GPU assignment; JSQ couples preparation and GPU placement. Mean preparation wait falls **10.48 → 1.33 ms**. This ingress difference is part of the measured configuration.

All ten runs process **722,128,759 input tokens and 10,121,700 output tokens**, matching physical engine accounting, with all **60,551 DAG edges** preserved. There are no drops, caps or preemptions. SPLIT has no allocation failures, admission refusals or final KV/reservation leaks; maximum batches at admission range from 23 to 32. Every ready-set min/max claim is audited. An independent calculation matches all 120 per-run statistics and 60 paired changes.

All five JSQ controls reproduce the frozen baseline after normalizing only `catalog.path`; the previous session-SPLIT seed-42 workload, metrics and routing audit are byte-identical. The affected Go checks and 75 Python tests pass. The broad Go run excludes one preexisting randomized P/D saturation failure, reproduced before these changes. New policy and study code live in `sim/cluster/request_split.go` and `research/request_split/`, sharing the validated central lifecycle with session-SPLIT.

- [Run instructions](../../request_split/README.md) and [frozen run matrix/configurations](../../outputs/mega-request-split-4s/study.json).
- Plotting rows: [sessions](../../outputs/mega-request-split-4s/analysis/sessions.csv), [families](../../outputs/mega-request-split-4s/analysis/families.csv), [requests](../../outputs/mega-request-split-4s/analysis/requests.csv), [paired sessions](../../outputs/mega-request-split-4s/analysis/paired-sessions.csv), [paired families](../../outputs/mega-request-split-4s/analysis/paired-families.csv).
- [Per-run statistics](../../outputs/mega-request-split-4s/analysis/per-run.csv), [aggregate ranges](../../outputs/mega-request-split-4s/analysis/aggregate.csv), [paired changes](../../outputs/mega-request-split-4s/analysis/paired-deltas.csv), [independent numerical audit](../../outputs/mega-request-split-4s/independent-numerical-audit.json), and [verification logs](../../outputs/mega-request-split-4s/review).

Regenerate the tables and verify them from the repository root:

```bash
python3 -B -m research.request_split.analysis research/outputs/mega-request-split-4s
python3 -B research/outputs/mega-request-split-4s/review/check_numerical_results.py research/outputs/mega-request-split-4s
```

The output directory is a symlink to `/scratch/akira/research-outputs/mega-request-split-4s` after the home quota interrupted four initial control artifact writes. All files were hash-verified during migration; incomplete controls were archived and rerun successfully. The linked research paths remain usable.
