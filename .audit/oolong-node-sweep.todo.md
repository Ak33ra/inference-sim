# Oolong node scaling study

- [x] Read current config and prior run settings. Current config has prefix caching off; prior reported results had it on.
- [x] Define comparison. Repeat the same 47 multi-child fork groups with 1, 2, 4, and 8 independent B200 TP1 replicas, one GPU per node. Keep fixed 1s arrivals, source work, engine settings, and seed42 unchanged.
- [x] Save independent research configs, with distinct output directories.
- [x] Run all node counts and verify exact source work, edges, and tokens.
- [x] Interleave five same-seed repetitions per node count to check simulator repeatability. Compare simulated times, not host wall time.
- [x] Report per-fork makespan/mean and makespan/median ratios, p50/p90 ratios, median extra seconds, threshold counts, and largest gap.
- [x] Explain absolute duration changes alongside ratios, with request timing evidence and replica utilization.
- [x] Save machine-readable results and plotting artifacts.

The benchmark-checklist and Explain the Number principles require a fresh cache-off
baseline, because comparison against the earlier cache-on baseline would change two
variables. The intended claim is descriptive scaling under this fixed configuration,
not an optimum topology or a measurement of real B200 hardware performance.

Host load at start was 5.28 on 256 available CPUs. Experiment artifacts are separate;
existing user outputs and config edits are preserved. No inference engine edits.

All 20 replays completed and passed the independent source/report/engine verifier:
25 families, 633 sessions, 1,608 requests, 2,191 dependency edges, 40,606,279 prompt
tokens and 420,697 output tokens. Every configured replica served requests; cache
hit rate was zero. All five repetitions per configuration produced byte-identical
workload and metrics reports, so min = median = max for the reported statistics.
This tests deterministic replay with seed 42; it does not estimate uncertainty
across arrival schedules or seeds.

Median fork makespan for 1/2/4/8 nodes: 50.76/19.04/13.05/8.06 seconds. Median
within-fork makespan / median-child-finish ratio: 2.07/2.20/2.00/1.60. Compare the
ratios after computing each fork's statistics from a shared origin (earliest child
arrival). Do not divide independently aggregated medians. Single-child groups
remain in the replay but are excluded from this comparison.

Request timing evidence: mean scheduling delay falls from 7.28 seconds on one node
to 0.77 seconds on two. In the largest-ratio two-node fork, child output counts
are 110/105/108/176/4,984 tokens. The slow child's LLM time is 114.29 seconds, with
only 1.32 seconds of total TTFT and 0.001 seconds of intervening non-LLM delay.
The median child finishes in 2.82 seconds. More replicas reduce contention but
cannot parallelize token generation within that individual child. At eight nodes
the same fork takes 34.49 seconds versus a 1.64-second median child finish.

Reproducible scripts and results are under
`research/outputs/analysis/oolong-node-sweep/`: `analyze.py`, `plot.py`, `repeat.py`,
`study.json`, `summary.json`, `summary.csv`, `forks.json`, `forks.csv`,
`repeatability.json`, `node-sweep.png`, and `node-sweep.pdf`.
