# Mega-trace routing and scheduling study

We compared how arrival rate, routing and scheduling change family makespan
and slowdown on the eight-B200 mega-trace configuration. All 225 runs passed
verification: 150 loaded runs and 75 isolated reference runs.

The workload combines all eight Commit0 buckets and natural Oolong, with no
FreeInference data. It has 287 families, 2,385 sessions and 58,741 requests.
Eight nodes each serve one GPT-OSS-120B mxfp4 replica on one B200, with TP=1.
Root arrivals have exponential gaps averaging 4 or 8 seconds, with five matched
arrival seeds. Recorded tool delays and DAG dependencies are preserved.
See [methods and reproduction](methods.md) for the full configuration.

## Findings

- **Doubling the arrival rate increases makespan.** JSQ/FCFS mean family
  makespan rises from 162.6s to 203.8s with caching off, and from 153.4s to
  175.4s with caching on. Mean slowdown rises from 1.24× to 1.85× off and
  from 1.16× to 1.50× on.
- **Agentix affinity/FCFS helps at 4s arrivals with caching on.** Its mean is
  170.4s, versus JSQ/FCFS's 175.4s. The paired per-seed mean improvement is
  2.6–3.4%. At 8s it is 0.9–2.1% slower by mean, although its reported p99
  is lower. Affinity/ATLAS does not improve mean makespan over affinity/FCFS,
  but has slightly better mean and p99 normalized slowdown at 4s.
  Affinity/FCFS has slower multi-child forks: its mean is 80.4s versus
  JSQ/FCFS's 76.3s at 4s, and 74.4s versus 67.6s at 8s.
- **ATLAS gives no consistent mean-makespan improvement here.** With JSQ and
  caching off it is 1.6–2.2% slower at 4s and about 0.35% slower at 8s.
  With caching on it is nearly tied with FCFS. Input-length SJF also stays
  near FCFS; random routing is worse at both rates.
- **Request MLFQ is costly with this KV-recompute backend.** At 4s its mean
  family makespan is 996.5s and mean slowdown is 14.55×. The median run has
  236,086 displacements and 3.90 billion recomputed tokens. At 8s these fall
  to 52,665 displacements and 0.93 billion tokens, but mean makespan remains
  210.9s. This measures the scoped port and its parameters, not FastServe's
  or Agentix's published system performance.
- **The Preble E2 cost port is slower in this experiment.** Its mean is
  228.7s at 4s and 185.8s at 8s, versus cache-on JSQ/FCFS's 175.4s and
  153.4s. At 4s both Preble and Agentix affinity reach about 90% prefix-token
  cache hits, versus JSQ's 62%; cache-hit rate alone does not predict makespan.

## Reading the tables

Each cell is **mean / median / p99**. Each number is the median of that
statistic across five seeds. Makespan is in seconds; slowdown is a per-family
ratio to the best observed isolated duration for that same cache mode.
These references cover all 287 families and are not proven optima.
Slowdowns are calculated per family before aggregation. Cache-on and cache-off
references differ, so use absolute makespan to compare caching's effect.

The [plots](../../outputs/mega-policy-study-final/analysis/plots) show the full
seed range. [Multi-child fork results](forks.md) use the same conventions.

## Prefix caching off

| Router / scheduler | 4s makespan (s) | 8s makespan (s) | 4s slowdown | 8s slowdown |
| --- | ---: | ---: | ---: | ---: |
| llm-d JSQ / FCFS | 203.7 / 107.7 / 974.2 | 162.6 / 74.1 / 892.7 | 1.85 / 1.65 / 3.97 | 1.24 / 1.15 / 2.39 |
| JSQ / FCFS | 203.8 / 108.6 / 974.1 | 162.6 / 74.8 / 892.9 | 1.85 / 1.66 / 3.90 | 1.24 / 1.15 / 2.44 |
| Least token / FCFS | 206.2 / 109.1 / 980.2 | 163.4 / 73.3 / 897.7 | 1.92 / 1.71 / 4.01 | 1.28 / 1.17 / 2.59 |
| Random / FCFS | 220.5 / 121.4 / 1,003.4 | 175.1 / 84.8 / 935.8 | 2.06 / 1.91 / 4.25 | 1.40 / 1.30 / 2.90 |
| JSQ / SJF | 203.7 / 107.9 / 976.2 | 162.6 / 73.7 / 893.0 | 1.84 / 1.67 / 3.76 | 1.24 / 1.15 / 2.47 |
| JSQ / MLFQ | 996.5 / 545.3 / 2,829.7 | 210.9 / 106.3 / 965.2 | 14.55 / 9.87 / 54.24 | 2.00 / 1.44 / 8.07 |
| JSQ / ATLAS | 207.7 / 111.9 / 980.9 | 163.2 / 74.1 / 897.1 | 1.92 / 1.71 / 3.95 | 1.25 / 1.16 / 2.41 |

## Prefix caching on

| Router / scheduler | 4s makespan (s) | 8s makespan (s) | 4s slowdown | 8s slowdown |
| --- | ---: | ---: | ---: | ---: |
| llm-d JSQ / FCFS | 175.6 / 86.1 / 939.5 | 153.3 / 67.2 / 866.9 | 1.50 / 1.39 / 2.76 | 1.16 / 1.10 / 1.87 |
| JSQ / FCFS | 175.4 / 87.9 / 938.3 | 153.4 / 67.4 / 865.4 | 1.50 / 1.39 / 2.74 | 1.16 / 1.11 / 1.91 |
| Least token / FCFS | 175.8 / 85.2 / 941.5 | 153.4 / 67.1 / 866.2 | 1.53 / 1.43 / 2.88 | 1.18 / 1.11 / 2.02 |
| Random / FCFS | 190.0 / 95.9 / 956.0 | 162.2 / 73.8 / 894.3 | 1.71 / 1.61 / 3.28 | 1.29 / 1.22 / 2.17 |
| Agentix affinity / FCFS | 170.4 / 81.9 / 896.5 | 156.0 / 69.2 / 856.3 | 1.33 / 1.26 / 2.28 | 1.14 / 1.08 / 1.69 |
| Preble E2 cost / FCFS | 228.7 / 120.8 / 1,015.3 | 185.8 / 95.2 / 937.7 | 2.59 / 2.00 / 7.68 | 1.73 / 1.48 / 4.46 |
| JSQ / ATLAS | 175.6 / 89.1 / 937.2 | 153.3 / 67.4 / 866.3 | 1.50 / 1.39 / 2.76 | 1.16 / 1.11 / 1.91 |
| Agentix affinity / ATLAS | 171.3 / 82.1 / 914.7 | 156.2 / 70.7 / 861.4 | 1.31 / 1.26 / 2.16 | 1.14 / 1.08 / 1.74 |

## Scope and saved results

These are scoped policy implementations. MLFQ and ATLAS use BLIS KV
recomputation rather than CPU swap. Research scheduling also publishes new
prefix blocks after batch execution, while native FCFS keeps its existing
eager publication within a batch. Cache-on scheduler comparisons include this
difference. The Preble port implements E2 placement cost with observed block
prefixes and approximate eviction costs; it omits the full system's local
scheduling, replication and rebalancing. See [methods](methods.md).

All runs complete the same 722,128,759 logical prompt tokens, 10,121,700 output
tokens and 60,551 dependency edges. Recomputed tokens are additional work.
The existing default cache-off path matches pre-change results at both arrival rates.
The full Go suite and 68 Python research tests pass. The
[independent audit](../../outputs/mega-policy-study-final/review/full225-review.md)
checks every run, artifact hashes, arrival pairing and isolated nonoverlap.

For later plotting, use [aggregate.csv](../../outputs/mega-policy-study-final/analysis/aggregate.csv)
for table statistics and seed ranges, [per-run.json](../../outputs/mega-policy-study-final/analysis/per-run.json)
for statistics and policy activity, [families.csv](../../outputs/mega-policy-study-final/analysis/families.csv)
and [forks.csv](../../outputs/mega-policy-study-final/analysis/forks.csv) for
individual objects, and [requests.csv.gz](../../outputs/mega-policy-study-final/analysis/requests.csv.gz)
for request latency data. [Paired comparisons](../../outputs/mega-policy-study-final/analysis/paired_deltas.csv),
[isolated-reference identities](../../outputs/mega-policy-study-final/analysis/families.csv),
[the earlier optrouting comparison](../../outputs/mega-policy-study-final/analysis/optrouting_slowdown.json)
and all raw runs are retained. PNG and PDF plots are in the linked plot directory.
