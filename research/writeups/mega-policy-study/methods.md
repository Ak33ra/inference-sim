# Mega-trace policy study methods

The study compares family and fork makespan on eight B200 replicas at mean
Poisson root interarrival times of 4 and 8 seconds. The main results are in
[README.md](README.md). Raw runs and frozen inputs are in
[the study directory](../../outputs/mega-policy-study-final).

## Workload and arrivals

The input is `traces/mega-commit0-oolong/input.jsonl.zst`. It contains one copy of
each Commit0 bucket `w00-04`, `w05-09`, `w10-14`, `w15-19`, `w20-24`, `w25-29`,
`w30-34`, and `w65-69`, plus the natural Oolong fanout trace. There is no
FreeInference data. The first bucket also contains 98 SWE-bench families.
The combined input has 287 families, 2,385 sessions, 58,741 requests,
722,128,759 prompt tokens, 10,121,700 output tokens and 60,551 dependency edges.

Family order is shuffled once with seed 42. Root arrivals use independent
exponential gaps with mean 4 or 8 seconds, starting the first family at zero.
Arrival seeds 42–46 are paired across every policy and cache setting. Loaded
runs keep the simulator seed at 42. The analyzer checks actual family-arrival
schedule equality, rather than relying only on matching configuration seeds.
Later requests are released by simulated parent completions and recorded tool
delays. Forks and joins retain their explicit DAG dependencies. There is no
family concurrency cap or recycling. Every selected family drains completely.

## Cluster and policies

Each of eight nodes serves one `gpt-oss-120b` mxfp4 replica on one B200.
TP, PP and DP are all one; the fabric is `ib-400g`. Engine version is 0.31.0,
block size 16, maximum batch tokens 16,384, maximum sequences 1,024, maximum
model length 131,072, GPU memory utilization 0.85 and CUDA graph mode
`FULL_AND_PIECEWISE`. Prefix caching is varied explicitly.

Cache-off conditions are llm-d JSQ/FCFS, research JSQ/FCFS,
least-token/FCFS, random/FCFS, JSQ/input-length SJF, JSQ/request MLFQ and
JSQ/ATLAS. Cache-on conditions are llm-d JSQ/FCFS, research JSQ/FCFS,
least-token/FCFS, random/FCFS, Agentix affinity/FCFS, Preble E2 cost/FCFS,
JSQ/ATLAS and Agentix affinity/ATLAS. There are 150 loaded runs.

Research routers share one dispatch path and an outstanding-request ledger.
JSQ counts waiting plus running requests through completion. Least-token sums
their original prompt lengths. Random samples uniformly. The existing llm-d
JSQ path is a separate control because it uses its own signals and tie-breaking.

The ATLAS port inherits completed family service when each request reaches an
engine. A shared process table covers the entire family, including child
sessions. Service receipts become visible at their simulated completion time.
Request MLFQ starts each call at the highest priority. Both use per-level
quanta of 100, 200, 400, 800, 1,600, 3,200, 6,400 and 12,800 ms, with cumulative
service boundaries [0,100), [100,300), [300,700), etc., and starvation ratio 10.
These are research parameters; the paper does not specify them.
See [scheduler semantics](../../../docs/guide/research-scheduler.md).

Agentix affinity routes prompts of at most 2,048 tokens by JSQ and pins longer
calls from the same family to one replica. Preble E2 cost uses a 180-second
history, observed block-prefix caches, an LRU eviction-cost approximation and
completed output lengths. Its cold-start output prior is 128 tokens.
Separate isolated synthetic probes produced prices of 6.7090406336 μs per
prefill token and 2,362.0455729 μs per decode token. The routing policy never
reads target requests' future output lengths. See
[router semantics](../../../docs/guide/research-router.md) and
[calibration](../../outputs/mega-policy-study-final/provenance/router-calibration.json).

These are scoped policy ports. ATLAS uses BLIS KV recomputation instead of
Agentix's CPU swap kernel and multi-step engine. Bottom-band renewal retains
FIFO position to preserve progress under recomputation. Research scheduling
publishes new cache blocks after batch execution; native FCFS retains its
existing same-batch eager publication. Cache-on scheduler comparisons include
that difference. Preble E2 cost excludes the complete system's prefill/decode
balancing shortcut, replication/rebalancing and local scheduling. Request MLFQ
does not implement FastServe's complete skip-join system.

Primary references are [Agentix, NSDI 2026](https://www.usenix.org/system/files/nsdi26-luo.pdf),
[Preble Algorithms 1–2](https://arxiv.org/html/2407.00023v2) and
[FastServe](https://arxiv.org/html/2305.05920).

## Measurements and references

Family makespan is elapsed time from root arrival to the last completion in
the family. Fork makespan is time from the root spawn request's completion to
the last direct child-session completion. Fork summaries distinguish all forks
from those with at least two children. Per-fork rows also retain mean and median
child finish time from the same origin.

For each cache setting, the denominator for slowdown is the smallest duration
observed for that same family or fork across all matching-cache isolated runs.
Isolation uses 12,000-second fixed root gaps and simulation seeds 42–46 for
every policy, producing 75 reference runs. Analysis rejects overlapping
families. Each isolated family retains access to all eight replicas; only
cross-family overlap is removed. The common denominator permits fair comparisons among loaded
policies; different objects may select different reference runs. It is a
best-observed isolated reference, not a proven optimum. Ratios below one are
retained. Cache-on and cache-off denominators differ, so absolute makespan is
the appropriate measure of caching's effect.

The earlier optrouting reference is retained separately for its 189 covered
families and 613 forks, with cache-off runs only. It is not mixed with the new
reference. The new reference covers the entire selected workload.

Slowdown is calculated per object before aggregation. Quantiles use linear
interpolation. Tables report the median of five per-run statistics; plots show
the range across seeds, not confidence intervals. All family makespan results
include startup, tool delays and the final drain. Load diagnostics use complete
one-second bins in the central 80% of root arrivals. The running/waiting request
diagnostics are not GPU utilization. Request scheduling delay measures first admission; the
research scheduler separately records cumulative waiting across readmissions.

## Reproduction and artifacts

The [frozen manifest](../../outputs/mega-policy-study-final/study.json) records
the binary, source archive, trace, catalog, registry, defaults, calibration,
configurations and hashes. Resume checks their integrity. Git optional locking
is disabled in worker processes so provenance inspection cannot rewrite the
frozen catalog index. Each run saves exact accounting and DAG verification,
metrics and workload reports. Research-router runs also save routing decisions;
MLFQ and ATLAS runs save scheduler activity.

```bash
source .inference-sim-env/activate
python3 -B -m research.policy_study.run run research/outputs/mega-policy-study-final --jobs 48
python3 -B -m research.policy_study.analysis research/outputs/mega-policy-study-final --requests
uv run --with-requirements research/analysis/requirements.txt python -B \
  -m research.policy_study.plot research/outputs/mega-policy-study-final
```

The runner's [README](../../policy_study/README.md) explains how to freeze a new
study. Saved `families.csv`, `forks.csv`, `per-run.csv`, `aggregate.csv`,
`paired_deltas.csv` and `requests.csv.gz` support later plotting without
rerunning simulations. Failed and superseded pilots remain in the earlier
`mega-policy-study` and `mega-policy-study-v2` output directories; they do not
contribute to the final tables.
