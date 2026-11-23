# JSQ versus least-token routing

JSQ has lower mean, median, and p99 slowdown in every matched seed, for all sessions, both session length groups, and family makespan. This comparison uses unrestricted routing across all eight replicas.

The results reuse ten verified runs from `mega-policy-study-final`; no new simulations were needed. Both policies use the full Commit0/Oolong mega trace, eight one-B200 GPT-OSS-120B MXFP4 replicas, FCFS, prefix caching off, and Poisson family-root arrivals averaging four seconds. Arrival seeds 42–46 are matched; simulator seed is 42. Dependencies and recorded tool delays are preserved, and all families drain.

JSQ minimizes the count of unfinished requests. Least-token minimizes the sum of **original prompt tokens** of unfinished requests. Each request retains its full prompt weight until it finishes. This implementation does not include predicted output tokens or estimate remaining work. Both break ties with seeded random choice.

## Slowdown

Arrows show **JSQ → least-token**. Each number is the median of five per-run statistics. The short/long labels reuse the reservation study’s fixed 40%/60% split, with 954 short and 1,431 long sessions; those labels impose no routing restrictions here.

| Population | Mean slowdown | Median slowdown | p99 slowdown |
|---|---:|---:|---:|
| All sessions | 1.812 → 1.910 | 1.712 → 1.786 | 3.465 → 3.951 |
| Short sessions | 1.959 → 2.118 | 1.866 → 1.995 | 3.888 → 4.276 |
| Long sessions | 1.707 → 1.751 | 1.609 → 1.656 | 2.987 → 3.091 |
| Families, makespan | 1.845 → 1.916 | 1.662 → 1.713 | 3.902 → 4.005 |

Changes are computed separately within each seed, then summarized by their median [minimum, maximum]. Positive values mean least-token is worse. These are seed ranges, not confidence intervals; paired changes can differ from the ratio of table medians.

| Population | Mean change (%) | Median change (%) | p99 change (%) |
|---|---:|---:|---:|
| All sessions | +5.4 [4.4, 6.2] | +4.3 [3.4, 5.8] | +13.6 [9.0, 15.8] |
| Short sessions | +8.9 [7.0, 10.6] | +7.8 [6.3, 8.7] | +12.1 [8.1, 18.2] |
| Long sessions | +2.6 [2.5, 2.8] | +3.2 [2.8, 3.8] | +2.9 [1.3, 3.5] |
| Families, makespan | +3.9 [3.0, 4.3] | +4.2 [1.4, 4.4] | +6.5 [2.5, 8.4] |

Slowdown uses the same per-object minimum elapsed time across 35 cache-off isolated-family runs as the reservation study. Sibling sessions can compete in those references, so these are best-observed references rather than proven session-alone optima. Session elapsed time includes intervening tool and dependency waits.

## Family makespan in seconds

| Router | Mean | Median | p99 |
|---|---:|---:|---:|
| jsq | 203.82 | 108.56 | 974.13 |
| least-token | 206.20 | 109.08 | 980.19 |

Request timing also worsens. The median across seeds of mean request scheduling delay is 63.94 → 77.57 ms, TTFT is 128.94 → 140.56 ms, and end-to-end request latency is 1,144.02 → 1,181.12 ms. These observations support the measured regression, but do not establish a complete causal explanation or a universal advantage for JSQ.

## Verification and data

All ten saved runs pass provenance and complete-work checks. Each processes 58,741 requests, 722,128,759 prompt tokens, and 10,121,700 output tokens, with 60,551 DAG edges and no failures or stranded work. Within each seed, configuration differs only in router and output path; root arrivals and logical work match. An independent calculation from raw outputs matches all 120 slowdown statistics and their summaries.

- [Comparison summary and provenance](../../outputs/mega-jsq-vs-least-token-4s/summary.json).
- [Individual session/family data](../../outputs/mega-jsq-vs-least-token-4s/objects.csv), [per-run statistics](../../outputs/mega-jsq-vs-least-token-4s/per-run.csv), and [paired changes](../../outputs/mega-jsq-vs-least-token-4s/paired-deltas.csv).
- [Independent numerical check](../../outputs/mega-jsq-vs-least-token-4s/independent-audit.json) and [request timing](../../outputs/mega-jsq-vs-least-token-4s/request-stage-diagnostic.json).

To regenerate the comparison tables and plotting data from the saved runs, execute from the repository root:

```sh
python3 -B research/outputs/mega-jsq-vs-least-token-4s/compare.py
```
