# Three-tier priority routing for agentic families

Splitting the eight replicas into three priority tiers, by each family's isolated optimal latency,
lowers family slowdown below JSQ. Pooled over five seeds, the mean drops **8.6%**, the median
**11.4%** and the p99 **19.7%**. The policy beats JSQ on every seed. It stays within the
stock llm-d router plugins; classes reach the router as a request header.

The policy is clairvoyant. Each family's class comes from its optimal isolated latency, computed
offline in [`research/outputs/optrouting/`](../../outputs/optrouting/README.md). This experiment
measures what the class information is worth, not a deployable classifier.

## Result

189 families (164 commit0 and 25 Oolong), seeds 42–46 pooled. After trimming, n = 765.

| Policy | Median | Mean | p99 |
| --- | --- | --- | --- |
| JSQ | 2.069 | 2.144 | 4.494 |
| Three-tier 40 / 20 / 40 | **1.834** | **1.960** | **3.608** |
| Change | −11.4% | −8.6% | −19.7% |

| Seed | 42 | 43 | 44 | 45 | 46 |
| --- | --- | --- | --- | --- | --- |
| Mean, JSQ | 2.044 | 2.238 | 2.129 | 1.980 | 2.331 |
| Mean, three-tier | 1.955 | 2.017 | 1.893 | 1.816 | 2.120 |
| Change | −4.4% | −9.9% | −11.1% | −8.3% | −9.0% |

By class (mean slowdown, three-tier against JSQ for the same families): P1 1.726 against 2.452
(−29.6%), P2 2.044 against 2.205 (−7.3%), P3 2.150 against 1.812 (+18.6%). Short families gain
and the longest pay. Because slowdown divides by the isolated optimum, a fixed delay costs a long
family less slowdown than a short one.

Time-averaged active requests per server (arrival to completion), over the trimmed window. JSQ
spreads load evenly, at 9.8–12.5 per server depending on the seed.

| Seed | P1-only servers | P1 + P2 servers | Shared servers |
| --- | --- | --- | --- |
| 42 | 5.6 | 9.8 | 13.6 |
| 43 | 3.2 | 9.2 | 16.0 |
| 44 | 4.9 | 8.0 | 19.5 |
| 45 | 5.0 | 8.2 | 16.1 |
| 46 | 6.2 | 12.2 | 16.6 |

## Setup

| Item | Value |
| --- | --- |
| Workload | Every family in optrouting's commit0 and Oolong sets (`traces/commit0-fanout-buckets/`, `natural-oolong-fanout.jsonl.zst`), each run exactly once. The 189 families are concatenated, commit0 first, and shuffled per seed with `random.Random(seed)`. |
| Serving | GPT-OSS-120B mxfp4 on 8 × B200 TP1 replicas, prefix caching off. Serving comes from `research/configs/semiclosed/commit0-overlap-jsq-8replicas.json`; Oolong's template has the same serving block. |
| Mode | Semi-closed replay (`research/scripts/semi_closed.sh`). Roots arrive by Poisson; children and later requests follow each family's DAG. |
| Load | λ = 0.223837 root families/s, seeded per seed. JSQ was calibrated to a mean slowdown of 2.0 on commit0 alone; with Oolong added, JSQ's mean is 2.144. At this load nothing queues: every request joins a batch on arrival, and slowdown comes from sharing a server. |
| Seeds | 42–46. Each seed sets the shuffle, the arrival times and the router's tie-breaking. |
| Metric | Family slowdown = family makespan under load (`makespan_us` in `workload.json`) ÷ `optimal_s` in optrouting's `root_e2e.csv`. |
| Trim | Within each run, families are ranked by arrival. The first and last 10% (18 each) are dropped as warmup and drain, leaving 153 per seed. |
| Statistics | Median, mean, and p99 (`statistics.quantiles(n=100, method="inclusive")[98]`), pooled over the seeds. |
| JSQ | `research/configs/routers/jsq.yaml`: `active-request-scorer` (running plus waiting requests) and `max-score-picker`. |

## Policy

Families are sorted by optimal isolated latency (ties broken by id) and cut into three classes:
P1 is the shortest 40% (76 families, optimum ≤ 100.3 s), P2 the next 20% (37), and P3 the rest
(76). Every request of a family, root and subagents alike, carries the family's class.

| Servers | Who may use them |
| --- | --- |
| `instance_0`, `instance_1` | P1 only |
| `instance_2`, `instance_3` | P1 and P2 |
| `instance_4` – `instance_7` | Every class |

Each class routes by JSQ over the servers it may use: P1 over 8, P2 over 6, P3 over 4. There is
no queueing and no preemption. The policy changes only where a request is placed.

### Implementation

| Piece | Where | What |
| --- | --- | --- |
| Class on the request | `sim/workload/semiclosed/{dag,corpus,replay}.go` | A weka-dag family line may carry `"slo_class"`. Replay sets it as each request's `SLOClass`, which the gateway already sends as the `x-llm-d-inference-objective` header. With no `InferenceObjective` manifests every class has priority 0, and the engine scheduler stays FCFS. |
| Profile by class | `research/configs/routers/jsq-3tier.yaml` | `header-profile-handler` on that header selects profile P1, P2 or P3. P2 adds `label-selector-filter` dropping `research.blis/tier=p1` pods. P3 adds a filter keeping only pods without that label. |
| Plugin opt-in, BLIS | `sim/llmd/support.go` | `header-profile-handler` is allowed only when `BLIS_RESEARCH_ALLOW_HEADER_PROFILE=1`. Without it, BLIS refuses the plugin as before. |
| Plugin opt-in, fork | `llm-d-router-header-profile.patch` | The fork's audited-plugin list gains `header-profile-handler`. The handler is a pure header lookup with no clock, randomness or background work. `replicate.sh` applies the patch to a copy of the pinned fork and selects it with a Go workspace (`GOWORK`), so `go.mod` is unchanged. |
| Alpha flag | `research/semiclosed/run.py` | `serving.router.epp_flags` passes router flags through. The tiered runs send BLIS's default flags plus `--allow-experimental-plugins=true`, since the handler is an Alpha plugin. |
| Server labels | `cmd/router.go` | `BLIS_RESEARCH_INSTANCE_LABELS`, a JSON map from instance id to labels, adds pod labels to individual servers. Pool labels in BLIS apply per role, not per server. |

All four switches are off by default, so a plain checkout behaves as `main` does.

## Replicating

```bash
# Go 1.27.x on PATH, plus python3, zstd, git and patch.
research/writeups/three-tier-routing/replicate.sh          # [--jobs N] [--out DIR]
```

- **Setup.** `replicate.sh` clones blis-catalog `0.3.0` and blis-registry `v0.2.0` into
  `research/outputs/deps/`, unless `BLIS_CATALOG` and `BLIS_REGISTRY` are already set. It then
  builds the patched fork copy and its workspace file, and runs `replicate.py`.
- **Runs.** `replicate.py` makes 10 semi-closed runs: JSQ and the three-tier policy on seeds
  42–46. Each run builds `blis`, then replays the 189 families. With the default `--jobs 10` it
  takes about 15 minutes on a many-core machine; each run needs a few GB of RAM.
- **Outputs.** Runs are cached in `research/outputs/three-tier-routing/work/runs/`, so a rerun
  reuses finished runs. Results go to `research/outputs/three-tier-routing/results.json` and
  `families.csv` (one row per family, seed and policy, with a `kept` flag for the trim).
- **Success criterion.** The last line reads `REPLICATED: matches the recorded result`. The
  pooled median, mean and p99 and every per-seed mean must match the recorded values to 12
  significant digits, and no request may run on a server its class is barred from. BLIS is
  deterministic, so any difference means the code, inputs or pins differ.

Expected output:

```
policy      median   mean     p99      (pooled, seeds 42-46, n = 765)
JSQ         2.069    2.144    4.494
3-tier      1.834    1.960    3.608
3-tier vs JSQ: median -11.4%, mean -8.6%, p99 -19.7%
by class (mean, 3-tier vs JSQ): P1 1.726 vs 2.452 (-29.6%), P2 2.044 vs 2.205 (-7.3%), P3 2.150 vs 1.812 (+18.6%)
seed 42: mean 1.955 vs JSQ 2.044 (-4.4%); active per server p1 5.6 / p12 9.8 / shared 13.6; violations 0
...
REPLICATED: matches the recorded result
```

**Notes for an automated harness:**

- **Don't set the research switches by hand.** `replicate.py` sets
  `BLIS_RESEARCH_INSTANCE_LABELS` and `BLIS_RESEARCH_ALLOW_HEADER_PROFILE` for the tiered runs
  only, and clears them for JSQ. Don't export them globally.
- **The tiered runs need `GOWORK`.** Without the workspace the stock fork refuses
  `header-profile-handler` at router startup, and those runs fail. JSQ runs don't need it.
- **Unrelated test failures.** `go test ./cmd/` fails three `TestCatalogProvenance*` tests when
  `BLIS_CATALOG` points at a git clone, on this branch and elsewhere. Run tests with it unset.

## How the partition was chosen

The partition (40%, 60%) came from a coarse-to-fine sweep on seed 42 only. The coarse pass cut
P1 at 10–50% of families and P1 + P2 at 20–80%, in 10% steps. A fine pass then searched in 5%
steps around the best point.

- **P1's share.** P1 must be large. With P1 at 20% or less, the P1-only servers sit nearly idle,
  and every partition was 7–60% worse than JSQ on the mean.
- **The ridge.** The best region is P1 at 35–45% with P1 + P2 at 55–60%, which gave −4.4% to
  −4.7% on seed 42. Seed 42 is the weakest seed for every policy tested.
- **Five seeds.** Three ridge points were rerun on all five seeds. 40/20/40 was best on the pooled
  mean and p99:

| Partition (P1 / P2 / P3) | Median / mean / p99 | Mean vs JSQ | p99 vs JSQ |
| --- | --- | --- | --- |
| 40 / 20 / 40 | 1.834 / 1.960 / 3.608 | −8.6% | −19.7% |
| 35 / 20 / 45 | 1.839 / 1.993 / 3.958 | −7.1% | −11.9% |
| 45 / 10 / 45 | 1.837 / 1.990 / 3.833 | −7.2% | −14.7% |

  A larger P3 crowds the four shared servers, and that sets the p99.

For context, the best two-tier policy was 2 servers reserved for the shortest 40% of families,
with JSQ over all 8 for those families and over the other 6 for the rest. On the same five
seeds it reached a median of 1.904, a mean of 2.017 (−6.0%) and a p99 of 3.796 (−15.5%). The
three-tier policy beat it on every seed, by 0.9 to 5.8 points of mean. The two-tier runs used
the same mechanism with one reserved label and two profiles, and are not scripted here.

## Caveats

- **Clairvoyant classes.** Classes use each family's optimal isolated latency, which a router
  would not know in advance.
- **One load, one workload.** There is one load point and one mix of workloads. Gains should
  shrink as load falls, since nothing needs sheltering then. The partition was tuned on this
  mix, using one seed.
- **One optimum is an upper bound.** For the commit0 family `moviepy` (a 67-child fork), the
  isolated optimum is an upper bound, not proven optimal. Its search stopped at 53.74 s against a
  48.44 s lower bound, so its slowdown is slightly understated.
- **The longest families pay.** P3 families are 18.6% slower than under JSQ. A metric weighted
  toward long jobs would rank the policies differently.
