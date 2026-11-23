# Multi-child fork results

We compare fork makespan and slowdown under the same policy and arrival-rate
conditions as the [family study](README.md). These tables cover the 161 forks
with at least two direct child sessions. The raw data also contains all 660
forks, including single-child forks.

A fork starts when the root spawn request finishes and ends when its last
direct child session finishes. Each cell is **mean / median / p99**, with each
number the median of that statistic across five seeds. Makespan is in seconds.
Slowdown uses the smallest matching-cache isolated duration observed for the
same fork across the tested policies and seeds. It is not a proven optimum.

For JSQ/FCFS, reducing the mean arrival gap from 8s to 4s raises mean fork
makespan from 71.8s to 87.8s with caching off, and from 67.6s to 76.3s with
caching on. Fork p99 changes much less than the median in these runs.
Cache-on Agentix affinity/FCFS has slower mean forks than JSQ/FCFS at both
rates, despite its lower mean family makespan at 4s. Its mean fork slowdown
is 1.80× at 4s and 1.62× at 8s, versus JSQ's 1.53× and 1.21×.

## Prefix caching off

| Router / scheduler | 4s makespan (s) | 8s makespan (s) | 4s slowdown | 8s slowdown |
| --- | ---: | ---: | ---: | ---: |
| llm-d JSQ / FCFS | 88.0 / 61.7 / 646.0 | 71.8 / 41.1 / 633.2 | 1.82 / 1.73 / 3.16 | 1.27 / 1.19 / 2.06 |
| JSQ / FCFS | 87.8 / 61.9 / 645.7 | 71.8 / 41.6 / 631.9 | 1.82 / 1.75 / 3.23 | 1.27 / 1.19 / 2.05 |
| Least token / FCFS | 87.9 / 62.8 / 646.4 | 71.8 / 42.4 / 634.6 | 1.80 / 1.71 / 3.21 | 1.27 / 1.19 / 2.03 |
| Random / FCFS | 94.6 / 69.9 / 652.0 | 76.6 / 46.6 / 636.1 | 2.08 / 2.01 / 3.91 | 1.45 / 1.37 / 2.54 |
| JSQ / SJF | 87.7 / 62.2 / 645.8 | 71.8 / 41.0 / 632.5 | 1.80 / 1.73 / 3.07 | 1.27 / 1.19 / 2.07 |
| JSQ / MLFQ | 480.8 / 308.0 / 1,556.3 | 96.1 / 61.4 / 649.4 | 15.15 / 11.76 / 50.00 | 2.25 / 1.71 / 6.48 |
| JSQ / ATLAS | 89.6 / 64.7 / 646.4 | 71.9 / 41.5 / 633.3 | 1.90 / 1.83 / 3.47 | 1.29 / 1.20 / 2.28 |

## Prefix caching on

| Router / scheduler | 4s makespan (s) | 8s makespan (s) | 4s slowdown | 8s slowdown |
| --- | ---: | ---: | ---: | ---: |
| llm-d JSQ / FCFS | 76.3 / 47.0 / 634.7 | 67.5 / 36.0 / 628.1 | 1.53 / 1.49 / 2.61 | 1.21 / 1.14 / 1.88 |
| JSQ / FCFS | 76.3 / 46.5 / 635.7 | 67.6 / 35.9 / 628.3 | 1.53 / 1.48 / 2.53 | 1.21 / 1.16 / 1.84 |
| Least token / FCFS | 76.0 / 47.6 / 635.0 | 67.4 / 36.2 / 629.5 | 1.51 / 1.45 / 2.47 | 1.21 / 1.15 / 1.83 |
| Random / FCFS | 82.3 / 55.2 / 640.3 | 71.2 / 40.1 / 631.4 | 1.80 / 1.74 / 2.95 | 1.39 / 1.32 / 2.39 |
| Agentix affinity / FCFS | 80.4 / 48.5 / 647.5 | 74.4 / 42.1 / 639.5 | 1.80 / 1.55 / 4.35 | 1.62 / 1.37 / 4.26 |
| Preble E2 cost / FCFS | 100.9 / 72.5 / 668.0 | 84.7 / 54.5 / 651.3 | 2.75 / 2.36 / 7.83 | 2.06 / 1.78 / 5.43 |
| JSQ / ATLAS | 76.3 / 46.5 / 635.0 | 67.5 / 35.9 / 628.9 | 1.53 / 1.49 / 2.47 | 1.21 / 1.16 / 1.84 |
| Agentix affinity / ATLAS | 80.0 / 49.0 / 648.1 | 74.5 / 42.2 / 640.8 | 1.83 / 1.56 / 4.55 | 1.64 / 1.35 / 4.27 |

See [methods](methods.md) for policy limitations and metric definitions.
[forks.csv](../../outputs/mega-policy-study-final/analysis/forks.csv) retains
per-fork makespan, mean and median child finish times, source identity,
and isolated-reference identity. Makespan/mean and makespan/median child-finish
ratios can be computed from those columns. The
[fork makespan plot](../../outputs/mega-policy-study-final/analysis/plots/fork-makespan.png)
and [fork slowdown plot](../../outputs/mega-policy-study-final/analysis/plots/fork-slowdown.png)
show the seed ranges. All-fork aggregate statistics remain in
[aggregate.csv](../../outputs/mega-policy-study-final/analysis/aggregate.csv).
