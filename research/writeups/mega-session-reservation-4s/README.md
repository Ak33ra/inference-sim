# Clairvoyant session reservation on the mega trace

We tested the proposed reservation rule against JSQ. It helps the typical short session but worsens short-session tail slowdown and all three long-session statistics. Overall session slowdown increases while overall family makespan slowdown decreases. Different weighting explains the opposing direction of their means; the diagnostic below does not establish the cause of the family quantile changes.

## Experiment

- The mega trace contains all eight nonempty Commit0 fanout buckets (`w00-04` through `w30-34`, plus `w65-69`) and `natural-oolong-fanout.jsonl.zst`. It excludes FreeInference. Each source appears once. There are 287 families, 2,385 sessions, and 58,741 requests.
- Eight replicas each use one B200 and GPT-OSS-120B with MXFP4 weights, TP=1, FCFS, and prefix caching off. JSQ counts waiting plus running requests.
- Family roots arrive with exponential gaps averaging 4 seconds, using matched arrival seeds 42–46 and simulator seed 42. The first root starts at zero; family order stays fixed. Child sessions and requests follow the DAG and recorded tool delays. Every family drains.
- One job is one session. Rank sessions by reference elapsed time, from first request arrival to last request completion, including intervening tool and dependency waits. The shortest 954 sessions (40%) may use any of the eight replicas. The remaining 1,431 cannot use `instance_0` or `instance_1`. Each request uses JSQ within its eligible set; sessions are not pinned.
- The short cutoff is 15.760141 seconds; the shortest long session is 15.766307 seconds. Classes remain fixed for both policies and every seed.

Slowdown is loaded elapsed time divided by that object’s minimum elapsed time across the 35 saved cache-off isolated-family runs. This reference still permits competition among sibling sessions. It is a shared best-observed reference, not a proven session-alone optimum. No reservation measurements were used to fit the reference.

## Results

Each value below is the median of five per-run statistics. Arrows show **JSQ → reservation**. Mean and quantiles are computed over individual slowdown ratios, not ratios of aggregate elapsed times.

| Population | Mean slowdown | Median slowdown | p99 slowdown |
|---|---:|---:|---:|
| All sessions | 1.812 → 1.955 | 1.712 → 1.760 | 3.465 → 4.070 |
| Short sessions | 1.959 → 1.862 | 1.866 → 1.580 | 3.888 → 5.292 |
| Long sessions | 1.707 → 2.020 | 1.609 → 1.919 | 2.987 → 3.728 |
| All families (makespan) | 1.845 → 1.631 | 1.662 → 1.513 | 3.902 → 3.204 |

Percentage changes below are computed within each matched seed, then their median is taken. Consequently, they can differ from the ratio of the displayed medians. Negative values mean lower slowdown.

| Population | Mean change | Median change | p99 change |
|---|---:|---:|---:|
| All sessions | +7.5% | +2.6% | +27.9% |
| Short sessions | -5.0% | -14.2% | +40.1% |
| Long sessions | +17.8% | +18.9% | +22.5% |
| All families (makespan) | -12.0% | -8.3% | -16.9% |

The direction of every reported change is consistent across all five seed pairs. The [full statistical report](../../outputs/mega-session-reservation-4s/analysis/report.md) includes each minimum and maximum. These ranges describe arrival-seed variation, not confidence intervals.

Family results differ because 102 families contain only short sessions but contribute just 216 of the 2,385 sessions. Their mean makespan slowdown falls from 2.355 to 1.337. The 113 mixed families and 72 long-only families worsen on that metric. Giving each family equal weight therefore emphasizes a different population than giving each session equal weight. These are diagnostic composition groups, not additional routing classes.

The short group supplies 14.0% of requests and 10.9% of output tokens. Under reservation, 93.2% of its requests use the two reserved replicas (median across seeds). Short requests can still use the other six. This measured concentration is relevant to the tail regression; it does not by itself establish its full cause.

## Verification and saved data

All ten runs completed all work with no failed, cancelled, timed-out, or stranded requests. Each processed exactly 722,128,759 prompt tokens and 10,121,700 output tokens, preserving all 60,551 DAG edges. Every long request stayed off the two reserved replicas. All five new JSQ controls reproduced the previous controls. Independent recomputation matched all 120 per-run statistics. The 68 existing Python tests, nine new Python tests, and full native command/router Go suites passed.

- [Study manifest and configurations](../../outputs/mega-session-reservation-4s/study.json).
- [Per-session data](../../outputs/mega-session-reservation-4s/analysis/sessions.csv), [per-family data](../../outputs/mega-session-reservation-4s/analysis/families.csv), and [paired session changes](../../outputs/mega-session-reservation-4s/analysis/paired-sessions.csv).
- [Per-run statistics](../../outputs/mega-session-reservation-4s/analysis/per-run.csv), [paired percentage changes](../../outputs/mega-session-reservation-4s/analysis/paired-deltas.csv), and [independent audit](../../outputs/mega-session-reservation-4s/analysis/independent-audit.json).
- [Runner documentation](../../session_reservation/README.md). The isolated references depend on the retained `mega-policy-study-final` directory.

To verify/reuse the completed runs and regenerate the analysis from the repository root:

```sh
source .inference-sim-env/activate
python3 -B -m research.session_reservation.run run research/outputs/mega-session-reservation-4s --jobs 8
python3 -B -m research.session_reservation.analysis research/outputs/mega-session-reservation-4s
python3 -B research/outputs/mega-session-reservation-4s/analysis/independent-check.py research/outputs/mega-session-reservation-4s
python3 -B research/outputs/mega-session-reservation-4s/analysis/reproduce-diagnostics.py research/outputs/mega-session-reservation-4s
```

This tests the specified policy. The partner’s numerical results were not supplied, so agreement with those results has not been established.
