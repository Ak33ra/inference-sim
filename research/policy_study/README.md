# Mega-trace policy study

Compare family and fork makespan at mean Poisson interarrival times of four and
eight seconds on eight B200 TP1 replicas. All runs drain the same 287 families,
2,385 sessions and 58,741 requests. Source tool delays and DAG dependencies are
preserved. There is no family concurrency cap.

The runner uses five matched arrival seeds, 42 through 46. Loaded runs hold the
simulation seed at 42. Isolated runs vary the simulation seed from 42 through 46
and space families 12,000 seconds apart. The analyzer rejects overlapping
isolated families. Prefix identities are local to each family.

| Prefix caching | Router and scheduler conditions |
| --- | --- |
| Off | llm-d JSQ/FCFS; research JSQ/FCFS; least-token/FCFS; random/FCFS; JSQ/SJF; JSQ/MLFQ; JSQ/ATLAS |
| On | llm-d JSQ/FCFS; research JSQ/FCFS; least-token/FCFS; random/FCFS; Agentix affinity/FCFS; Preble E2 placement/FCFS; JSQ/ATLAS; Agentix affinity/ATLAS |

There are 150 loaded and 75 isolated runs. The common slowdown denominator is
the minimum isolated duration for the same object and cache setting across the
tested policies and simulation seeds. This is a best-observed reference, not a
proven optimum. The previously published cache-off optrouting results remain a
separate analysis with their original coverage.

## Run or resume

From the repository root, activate the local Go and catalog environment, build
a binary, and freeze a new study. Supply a JSON calibration artifact containing
a `parameters` object accepted by `serving.router.parameters`.

```bash
source .inference-sim-env/activate
go build -o /tmp/mega-study-blis .
python3 -B -m research.policy_study.run init research/outputs/my-policy-study \
  --binary /tmp/mega-study-blis \
  --calibration research/outputs/mega-policy-study/router-calibration.json
python3 -B -m research.policy_study.run run research/outputs/my-policy-study --jobs 8
python3 -B -m research.policy_study.analysis research/outputs/my-policy-study --requests
uv run --with-requirements research/analysis/requirements.txt python -B \
  -m research.policy_study.plot research/outputs/my-policy-study
```

The initialized study saves the binary, source archive, trace, catalog, registry,
defaults, router profile, calibration and all configurations. Resume checks their
hashes, including the Python helpers that execute and verify runs. A verified run
is reused only when its saved artifact hashes still match. An incomplete run is
retained for inspection; `--retry-failed` archives it before retrying.

Use `--kind loaded` or `--kind isolated` to run one portion. Use `--match` with a
run-ID substring and `--limit` for a pilot. The analyzer requires the full matrix
by default. `--allow-partial` produces explicitly incomplete interim summaries.

## Saved plotting data

The `analysis` directory contains family and fork rows, per-run statistics,
aggregates across seeds, paired policy/rate comparisons, isolated-reference
identities and the separate published-reference analysis. `--requests` adds a
compressed request CSV. Raw metrics, workload reports, policy counters, router
decisions and verification results remain in each run directory.

Family makespan runs from root arrival to the last completion in the family.
Fork duration runs from the root spawn request's completion to the last child
session completion. Fork rows also contain mean and median child finish times
from that same origin. Multi-child forks are summarized separately.

Quantiles use linear interpolation. Aggregates report the median and range of
the five per-run statistics. Slowdown is calculated per object before taking
means or quantiles. Ratios below one are retained. Queue diagnostics use complete
one-second bins in the central 80% of root arrivals; they are not GPU utilization.
