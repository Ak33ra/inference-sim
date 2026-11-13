# Mega trace: eight-second mean root arrivals

Increasing the mean Poisson arrival rate from 1/12 to 1/8 root families per second
raises replica request-active time from **70.1% to 88.3%**. Median family makespan
rises from **68.2s to 74.1s**. Mean request TTFT rises from **112.6ms to 116.7ms**,
while p99 multi-child fork slowdown increases from **1.730× to 2.107×** relative
to the published optrouting reference.

Run the eight-second configuration from the repository root:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/mega-8b200-8s.json
```

It writes to `research/outputs/semiclosed/mega-8b200-8s`. Use `--output PATH` with
a fresh directory for a repeat. The measured directories are
`research/outputs/semiclosed/mega-8b200-poisson-8s-seed{42..46}`. Seed 42 reuses the
verified calibration pilot; seeds 43–46 were run for this comparison. The config
matches measured seed-42 settings and selects a new output location.

The [same mega trace](../mega-8b200-baseline/README.md) contains all eight Commit0
bucket files and Oolong once each: 287 families, 2,385 sessions, and 58,741
requests. Every run processes 722,128,759 prompt tokens and 10,121,700 output
tokens. Serving remains eight B200 TP1 replicas of GPT-OSS-120B mxfp4, JSQ routing
(waiting plus running requests), and prefix caching off. Engine settings and
model, catalog, registry, and router configuration match the twelve-second runs.

Only the root arrival rate changes, from five to 7.5 roots/minute on average.
Arrival seeds 42–46 are paired between rates; trace-order and router seeds stay
42. Each new root schedule is the corresponding old schedule compressed to
two-thirds of its duration, verified to within the simulator's microsecond
rounding. Internal parent dependencies and recorded delays are preserved.
All families drain, with no family concurrency cap or recycling.

Each table cell below is the median of its per-run statistic across five arrival
seeds. Brackets show the minimum and maximum across seeds. Latency and makespan
statistics include the full workload. Slowdown uses only matching references.

| Metric | 12s mean arrival gap | 8s mean arrival gap |
| --- | ---: | ---: |
| Request-active time | 70.1% [64.2, 71.6] | 88.3% [84.3, 90.5] |
| TTFT mean | 112.6 ms [112.1, 113.3] | 116.7 ms [116.1, 117.3] |
| TTFT median | 91.6 ms [91.4, 92.1] | 94.2 ms [93.8, 94.6] |
| TTFT p99 | 412.4 ms [405.8, 414.5] | 432.1 ms [427.5, 432.8] |
| Scheduling delay p95 | 84.1 ms [82.6, 85.9] | 96.1 ms [92.4, 98.2] |
| Family makespan mean | 154.6 s [153.9, 155.3] | 162.6 s [161.9, 163.7] |
| Family makespan median | 68.2 s [65.7, 68.8] | 74.1 s [71.7, 75.4] |
| Family makespan p90 | 418.8 s [408.2, 420.2] | 437.9 s [432.6, 446.9] |
| Family makespan p99 | 878.6 s [874.8, 881.1] | 892.7 s [888.0, 906.2] |
| Peak active families | 23 [20, 26] | 31 [30, 38] |

Request-active time is the fraction of time each replica has at least one
scheduled, unfinished request, averaged across eight replicas. It includes host
overhead and is not GPU utilization or fraction of maximum throughput. The
measurement window is the central 80% of each run's root-arrival interval,
rounded inward to whole one-second bins. Scheduling p95 uses requests arriving
inside that window and includes admission overhead. Other latency statistics
use all requests. All runs have zero preemptions.

The mean scheduler waiting count per replica is 0.00442 for the 8s experiment
(seed range 0.00354–0.00468), compared with 0.00200 at 12s. Both have little
admission queueing. Including startup and the entire final drain gives a
median request-active fraction of 50.4% at 8s versus 53.6% at 12s; those figures
include different drain periods and are separate from the load-window metric.

Slowdown is loaded duration divided by the published `optimal_s`, computed per
object before taking percentiles. Family time runs from root arrival to the
last family completion. Fork time starts at spawn-request completion and ends
at the last child completion, matching optrouting. The references cover 189
families and 613 forks, including 160 multi-child forks. The 98 SWE-bench
families without a published reference are excluded from slowdown statistics.
Some published optima are best-found estimates; this comparison uses them as
requested. The [baseline slowdown notes](../mega-8b200-baseline/slowdown.md)
describe the reference coverage and definitions.

| Cohort | Mean root gap | Objects/run | Mean slowdown | Median | p90 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Families | 12s | 189 | 1.089× | 1.057× | 1.189× | 1.453× |
| Families | 8s | 189 | 1.170× | 1.131× | 1.357× | 1.787× |
| Multi-child forks | 12s | 160 | 1.160× | 1.100× | 1.368× | 1.730× |
| Multi-child forks | 8s | 160 | 1.284× | 1.212× | 1.647× | 2.107× |
| All forks | 12s | 613 | 1.094× | 1.042× | 1.257× | 1.580× |
| All forks | 8s | 613 | 1.180× | 1.116× | 1.458× | 1.998× |

All five 8s runs completed every request with exact token accounting, verified
all 60,551 dependency edges, and had zero failures, drops, truncations, timeouts,
cancellations or preemptions. The comparison checks matching input hashes,
serving settings, router settings, simulation versions, arrival seeds and
published reference hashes. Durations use simulation time; host wall time is
not part of the measurement.

Saved artifacts:

- [Full 8s run summaries](../../outputs/analysis/mega-8b200-8s/summary.json)
- [Comparison with seed ranges](../../outputs/analysis/mega-8b200-8s/comparison.json)
- [Per-seed comparison CSV](../../outputs/analysis/mega-8b200-8s/comparison.csv)
- [Slowdown summary](../../outputs/analysis/mega-8b200-8s/slowdown/summary.json)
- [Individual family slowdowns](../../outputs/analysis/mega-8b200-8s/slowdown/family-slowdowns.csv)
- [Individual fork slowdowns](../../outputs/analysis/mega-8b200-8s/slowdown/fork-slowdowns.csv)

Regenerate the analysis from saved runs:

```bash
python3 -B research/writeups/mega-8b200-baseline/analyze.py \
  research/outputs/semiclosed/mega-8b200-poisson-8s-seed{42,43,44,45,46} \
  --output research/outputs/analysis/mega-8b200-8s/summary.json
python3 -B research/writeups/mega-8b200-baseline/slowdown.py \
  research/outputs/semiclosed/mega-8b200-poisson-8s-seed{42,43,44,45,46} \
  --output research/outputs/analysis/mega-8b200-8s/slowdown
python3 -B research/writeups/mega-8b200-8s/compare.py
```
