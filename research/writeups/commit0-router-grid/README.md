# Commit0 routing and replica count

We compared JSQ and least-token routing with 4, 8, and 16 replicas in the
semi-closed simulator, using five seeds per configuration. Increasing from 4 to
16 replicas roughly halved median family makespan. Router choice had a much
smaller effect.

The workload used these two Weka DAG files from `traces/commit0-fanout-buckets`:

| Source | Original families | Original requests | Copies |
| --- | ---: | ---: | ---: |
| [w25-29/traces.jsonl.zst](../../../traces/commit0-fanout-buckets/w25-29/traces.jsonl.zst) | 3 | 2,971 | 2 |
| [w30-34/traces.jsonl.zst](../../../traces/commit0-fanout-buckets/w30-34/traces.jsonl.zst) | 3 | 3,986 | 2 |

Two copies of the six original families produced 12 independent root families,
408 sessions, and 13,914 requests. Each run contained 162,892,506 prompt tokens
and 2,340,834 output tokens. Copies received distinct IDs while preserving request
dependencies, token counts, and recorded non-LLM delays.

The composer alternated families from the two sources, placing both copies
consecutively. Roots arrived at fixed one-second intervals, at 0 through 11
seconds. Requests within each family followed simulated parent completions plus
recorded delays. Fork branches could run concurrently; joins waited for all
parents. Families left when all their work finished, with no recycling or family
concurrency cap. All 12 overlapped at peak load. Only root arrivals were fixed;
request arrivals depended on simulated progress.

Every replica had the same configuration:

| Setting | Value |
| --- | --- |
| Model | GPT-OSS-120B, `mxfp4` quantization |
| Cluster | 4, 8, or 16 nodes; one B200 GPU and one replica per node |
| Parallelism and fabric | TP1, PP1, DP1; `ib-400g` |
| Prefix caching | Off |
| Batch limits | 16,384 tokens and 1,024 sequences |
| Context and memory | 131,072-token context; GPU memory utilization 0.85 |

JSQ minimized waiting plus running request counts. Least-token minimized
outstanding prompt tokens, held until completion of these nonstreaming requests.
Output-token estimates were disabled. Both profiles disabled flow control and
the implicit utilization filter, so all ready replicas participated in scoring.
Earlier runs with that filter are excluded.

All 30 runs used the same workload and arrivals. Router seeds were 42 through 46;
the arrival seed remained 42.

Family makespan runs from root arrival to completion of all family work. Forks
group child sessions with a shared spawn and join request. We analyzed 18
multi-child forks, excluding 28 single-child groups. Child finish times share the
earliest child arrival as their origin, including staggered starts and non-LLM
delays. Fork makespan is the maximum child finish time.

Times below are medians across families or forks, then medians across five
seeds, in seconds. Brackets show the seed range for the family statistic.

| Router | Replicas | Family makespan [range] | Fork makespan | Mean child finish | Median child finish |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSQ | 4 | 377.5 [376.9, 380.4] | 150.9 | 90.5 | 90.0 |
| Least-token | 4 | 380.1 [380.0, 380.2] | 153.8 | 91.2 | 91.5 |
| JSQ | 8 | 254.3 [254.3, 255.1] | 91.4 | 60.1 | 58.7 |
| Least-token | 8 | 255.2 [254.7, 256.1] | 92.9 | 59.2 | 59.0 |
| JSQ | 16 | 191.2 [190.8, 191.5] | 78.2 | 37.9 | 38.3 |
| Least-token | 16 | 191.5 [190.6, 191.7] | 74.0 | 37.7 | 38.9 |

Ratios below are calculated within each fork, followed by the median and p90
across forks, then the median across seeds. Percentiles use linear interpolation.
Dividing columns in the preceding table gives a different statistic.

| Router | Replicas | Makespan/mean: median | Makespan/mean: p90 | Makespan/median: median | Makespan/median: p90 |
| --- | ---: | ---: | ---: | ---: | ---: |
| JSQ | 4 | 1.66x | 2.31x | 1.67x | 2.50x |
| Least-token | 4 | 1.63x | 2.22x | 1.66x | 2.36x |
| JSQ | 8 | 1.55x | 2.29x | 1.53x | 2.51x |
| Least-token | 8 | 1.58x | 2.22x | 1.56x | 2.43x |
| JSQ | 16 | 1.62x | 2.40x | 1.56x | 2.64x |
| Least-token | 16 | 1.60x | 2.36x | 1.54x | 2.54x |

Neither router consistently won on family makespan across seeds. Least-token
had lower p90 ratios at every replica count and about 5.5% shorter median fork
makespan at 16 replicas. More replicas reduced absolute time, but did not
consistently reduce the relative gap between the slowest and typical children.
These results describe this workload, including its duplicate families.

All runs completed with exact token accounting and no failures or truncations.
All 400,987 audited routing decisions matched the minimum-load policy. The audit
excluded simultaneous events because saved metrics lack their ordering.

Saved artifacts include the [plot](../../outputs/analysis/commit0-router-grid/makespan-grid.png),
[full statistics and ranges](../../outputs/analysis/commit0-router-grid/summary.json),
[per-fork CSV](../../outputs/analysis/commit0-router-grid/forks.csv), and
[analysis script](../../outputs/analysis/commit0-router-grid/analyze.py).
The [config guide](../../configs/semiclosed/README.md) links all six configs.
Reported run directories end in `-pure`.
