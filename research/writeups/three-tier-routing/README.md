# Three-tier priority routing for agentic families

Splitting the eight replicas into three priority tiers, by each family's isolated optimal latency,
lowers family slowdown below JSQ. Pooled over five seeds, the mean drops **8.6%**, the median
**11.4%** and the p99 **19.7%**. The policy beats JSQ on every seed. It stays within the
stock llm-d router plugins; classes reach the router as a request header.

The policy is clairvoyant. Each family's class comes from its optimal isolated latency, computed
offline in [`research/outputs/optrouting/`](../../outputs/optrouting/README.md). This experiment
measures what the class information is worth, not a deployable classifier.

A causal observed-work variant closes part of that gap without future output lengths or unreleased
DAG information. With soft reservations it lowers mean slowdown **2.3%** at the primary rate and
**1.0%** at twice the arrival rate, beating JSQ on every seed at both loads.

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

### Non-clairvoyant causal observed-work tiers

An online classifier can also beat JSQ in this setting. It keeps one causal counter per family:

`observed work = admitted prompt tokens + 64 × completed output tokens`.

The current request's prompt charge is added at admission, when its length is known. Its output is
charged from `GeneratedTokens` only after completion. The classifier never reads the request's
simulator-known `OutputTokens`, an unreleased DAG node, an isolated optimum, or any other future
information. All root and subagent requests share their family's counter. After adding the current
prompt, work through 2.4M is P1, work through 4.6M is P2, and higher work is P3.

The best version uses **soft reservations**. P1 uses ordinary JSQ over all eight replicas. P2
prefers instances 2–7 and P3 prefers instances 4–7, but every replica remains eligible. The
router's normalized JSQ score is in [0, 1]; a preferred replica receives a 0.5 bonus. Thus an old
family borrows a reserved replica only when its JSQ score is more than 0.5 better than every home
replica's score. Routing remains nonpreemptive, and engine scheduling and batching are unchanged.

At the primary rate, soft causal tiers beat JSQ on every seed and improve every pooled statistic:

| Policy | Median | Mean | p99 | Median vs JSQ | Mean vs JSQ | p99 vs JSQ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| JSQ | 2.069 | 2.144 | 4.494 | — | — | — |
| Hard causal tiers | 1.985 | 2.114 | 4.400 | −4.1% | −1.4% | −2.1% |
| **Soft causal tiers** | **1.996** | **2.094** | **4.356** | **−3.5%** | **−2.3%** | **−3.1%** |
| Clairvoyant three-tier | 1.834 | 1.960 | 3.608 | −11.4% | −8.6% | −19.7% |

The bolding compares the two causal policies on the mean, the tuning objective; hard tiers have a
slightly lower median. The clairvoyant row is an information-rich reference, not a deployable policy.

| Seed | 42 (tuning) | 43 | 44 | 45 | 46 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mean, JSQ | 2.044 | 2.238 | 2.129 | 1.980 | 2.331 |
| Mean, hard causal | 2.011 | 2.235 | 2.081 | 1.942 | 2.299 |
| Mean, soft causal | **1.999** | **2.185** | **2.078** | **1.931** | **2.277** |
| Soft causal vs JSQ | −2.2% | −2.3% | −2.4% | −2.5% | −2.3% |

The same fixed parameters also beat JSQ when the arrival rate doubles to 0.447674 families/s.
No 2× result was used for selection:

| Policy, 2× arrival rate | Median | Mean | p99 | Median vs JSQ | Mean vs JSQ | p99 vs JSQ |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| JSQ | 2.892 | 3.105 | 7.258 | — | — | — |
| Hard causal tiers | 2.932 | 3.131 | 7.044 | +1.4% | +0.8% | −2.9% |
| **Soft causal tiers** | **2.844** | **3.075** | **7.224** | **−1.7%** | **−1.0%** | **−0.5%** |
| Clairvoyant three-tier | 3.021 | 3.068 | 6.980 | +4.4% | −1.2% | −3.8% |

| Seed, 2× | 42 | 43 | 44 | 45 | 46 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mean, JSQ | 3.152 | 2.914 | 3.119 | 3.053 | 3.285 |
| Mean, hard causal | 3.187 | 2.961 | 3.143 | 3.045 | 3.320 |
| Mean, soft causal | **3.109** | **2.882** | **3.097** | **3.019** | **3.265** |
| Soft causal vs JSQ | −1.4% | −1.1% | −0.7% | −1.1% | −0.6% |

The soft policy does not merely transfer slowdown between the clairvoyant length groups. At the
primary rate its mean changes against JSQ are −4.1% for oracle-P1 families, −2.0% for oracle-P2,
and −0.1% for oracle-P3. At 2× they are −1.1%, −1.2%, and −0.6%. These oracle classes are used
only after the runs for analysis. P2 borrowed outside its home pool for 0.78% of requests at the
primary rate and 0.42% at 2×; P3 borrowed for 10.94% and 13.48%, respectively.

#### Selection and audit

Selection used only seed 42 at the primary rate, with trimmed mean family slowdown as the stated
objective. A first-stage 3 × 3 sweep tested completed-output weights 32, 64, and 128 and three
threshold pairs around the final-work 35/55, 40/60, and 45/65 percentiles. All nine hard-tier
candidates beat seed-42 JSQ; weight 64 with thresholds 2.4M/4.6M was best at 2.011. Holding those
fixed, a second-stage sweep tested soft-affinity bonuses 0.1, 0.25, 0.5, and 1.0. Their seed-42
means were 2.024, 2.010, 1.999, and 2.014, selecting 0.5. Seeds 43–46 and all doubled-rate runs
were then evaluated without retuning.

All 31 causal runs—18 hard and 13 soft—completed 56,619 requests with exact source-to-engine
accounting. For every request, the analysis checks its recorded threshold class, prompt charge,
completed-output receipt, final family work, and objective header. It also checks hard-tier
placement. Unit tests pin the timing invariant: output is absent from the classifier at admission
and is charged in the completion callback. There were zero class, charge, final-work, header, or
hard-placement mismatches.

Reproduce the baselines, two-stage tuning, and held-out runs with:

```bash
research/writeups/three-tier-routing/replicate.sh --jobs 10
research/writeups/three-tier-routing/replicate.sh --jobs 10 \
  --out research/outputs/three-tier-routing-2x --rate 0.447674
research/writeups/three-tier-routing/causal_tier_compare.sh --jobs 6
research/writeups/three-tier-routing/causal_soft_compare.sh --jobs 6
```

Hard results are in `research/outputs/three-tier-routing-causal/`; soft results are in
`research/outputs/three-tier-routing-causal-soft/`. Each directory contains `results.json`,
`families.csv`, and resumable raw runs under `work/runs/`.

### Matched session-SPLIT comparison

We also ran the session-SPLIT implementation from the `akira` branch in this exact setting. One
sequential session is one job. Instance 0 claims the largest remaining ready session and the other
seven instances claim the shortest. Requests remain nonpreemptive and retain native batching. The
remaining-size oracle selects each session's fastest complete timeline across five isolated,
cache-off JSQ runs on the same 189-family population. Like the three-tier classes, this is
clairvoyant policy input.

Session SPLIT is slower than JSQ on every seed and slower than three-tier by a wider margin:

| Policy | Median | Mean | p99 |
| --- | ---: | ---: | ---: |
| JSQ | 2.069 | 2.144 | 4.494 |
| Three-tier 40 / 20 / 40 | **1.834** | **1.960** | **3.608** |
| Session SPLIT | 2.106 | 2.178 | 4.696 |
| Session SPLIT vs JSQ | +1.8% | +1.6% | +4.5% |
| Session SPLIT vs three-tier | +14.8% | +11.1% | +30.2% |

| Seed | 42 | 43 | 44 | 45 | 46 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mean, JSQ | 2.044 | 2.238 | 2.129 | 1.980 | 2.331 |
| Mean, three-tier | 1.955 | 2.017 | 1.893 | 1.816 | 2.120 |
| Mean, session SPLIT | 2.079 | 2.269 | 2.169 | 2.022 | 2.353 |
| Session SPLIT vs JSQ | +1.7% | +1.4% | +1.9% | +2.1% | +1.0% |

The slowdown increase is similar in every three-tier length class: +1.6% for P1, +1.7% for P2,
and +1.5% for P3 against JSQ. Only 6.0–8.7% of SPLIT claims had more than one ready session to
rank, so the size rule usually had no choice to make. This experiment measures the complete
implementation: SPLIT uses a central admission queue and round-robin preparation, while JSQ
couples preparation to its selected replica. It does not isolate priority ranking from those
dispatch and batching changes.

All five SPLIT runs completed 56,619 requests with exact accounting. The audit independently
replayed all 226,476 lifecycle events per seed, checked every shortest/longest choice, and found
zero preemptions, admission refusals, allocation failures, or final KV reservations. Fresh JSQ
controls built from the SPLIT-capable branch reproduced every recorded pooled and per-seed value
to 12 significant digits.

Reproduce the matched comparison after provisioning the dependencies with `replicate.sh`:

```bash
git worktree add --detach /tmp/inference-sim-session-split akira
python3 -B research/writeups/three-tier-routing/session_split_compare.py \
  --split-repo /tmp/inference-sim-session-split --jobs 10
```

The generated results are in `research/outputs/three-tier-routing-session-split/results.json` and
`families.csv`. The raw runs are cached under its `work/runs/` directory.

#### Double arrival rate

At λ = 0.447674 families/s, exactly twice the primary experiment's arrival rate, session SPLIT
still loses to JSQ on all five seeds, but by less than at the original rate. Three-tier has the
best pooled mean and p99, while JSQ has the best median:

| Policy | Median | Mean | p99 |
| --- | ---: | ---: | ---: |
| JSQ | **2.892** | 3.105 | 7.258 |
| Three-tier 40 / 20 / 40 | 3.021 | **3.068** | **6.980** |
| Session SPLIT | 2.915 | 3.126 | 7.364 |
| Three-tier vs JSQ | +4.4% | −1.2% | −3.8% |
| Session SPLIT vs JSQ | +0.8% | +0.7% | +1.5% |

| Seed | 42 | 43 | 44 | 45 | 46 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Mean, JSQ | 3.152 | 2.914 | 3.119 | 3.053 | 3.285 |
| Mean, three-tier | 3.074 | 2.792 | 3.098 | 3.079 | 3.296 |
| Mean, session SPLIT | 3.169 | 2.939 | 3.137 | 3.079 | 3.308 |

SPLIT had multiple ready sessions on 11.6–12.9% of claims, about twice the opportunity at the
primary rate, but its mean slowdown remains about 0.7% above JSQ in every length class. Three-tier
continues to redistribute latency: against JSQ, P1 improves 9.4%, P2 improves 2.9%, and P3 worsens
14.1%. Its pooled mean improves, but it loses on mean in seeds 45 and 46 and worsens the pooled
median. The fixed 40 / 20 / 40 partition is therefore less robust at the higher load.

The doubled-rate runs reuse the same isolated session oracle and change only the Poisson rate.
Fresh JSQ controls from the three-tier and SPLIT-capable branches again agree exactly. All 20
loaded runs complete with exact accounting; the tier audit has zero placement violations and the
SPLIT audit checks every shortest/longest selection with zero refusals, preemptions, allocation
failures, or final KV reservations.

```bash
research/writeups/three-tier-routing/replicate.sh --jobs 10 \
  --out research/outputs/three-tier-routing-2x --rate 0.447674

python3 -B research/writeups/three-tier-routing/session_split_compare.py \
  --split-repo /tmp/inference-sim-session-split --jobs 10 --rate 0.447674 \
  --out research/outputs/three-tier-routing-session-split-2x \
  --three-tier-results research/outputs/three-tier-routing-2x/results.json \
  --remaining-manifest research/outputs/three-tier-routing-session-split/remaining.json
```

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
| Causal class | `sim/workload/semiclosed/{replay,report}.go` | Optional causal tiers charge each admitted prompt and only completed output receipts to family state, assign P1/P2/P3 before routing, and record every decision and charge in `workload.json`. Static trace classes and causal tiers are mutually exclusive. |
| Causal config | `cmd/replay_semiclosed.go`, `research/semiclosed/run.py` | Strict `run.causal_tiers` configuration carries two thresholds and the completed-output weight into semi-closed replay. It is off by default. |
| Profile by class | `research/configs/routers/jsq-3tier.yaml` | `header-profile-handler` on that header selects profile P1, P2 or P3. P2 adds `label-selector-filter` dropping `research.blis/tier=p1` pods. P3 adds a filter keeping only pods without that label. |
| Soft profiles | `research/configs/routers/jsq-3tier-soft.yaml` | P2 and P3 add `header-label-affinity-scorer` bonuses for their home labels instead of filters. The active-request scorer can override the bonus, so all eight endpoints remain eligible. |
| Plugin opt-in, BLIS | `sim/llmd/support.go` | `header-profile-handler` and, for the soft ablation, `header-label-affinity-scorer` are allowed only when `BLIS_RESEARCH_ALLOW_HEADER_PROFILE=1`. Without it, BLIS refuses them as before. |
| Plugin opt-in, fork | `llm-d-router-header-profile.patch` | The fork's audited-plugin list gains both pure header plugins. They have no clock, randomness, blocking, or background work on their decision paths. The wrappers apply the patch to a copy of the pinned fork and select it with a Go workspace (`GOWORK`), so `go.mod` is unchanged. |
| Alpha flag | `research/semiclosed/run.py` | `serving.router.epp_flags` passes router flags through. The tiered runs send BLIS's default flags plus `--allow-experimental-plugins=true`, since the handler is an Alpha plugin. |
| Server labels | `cmd/router.go` | `BLIS_RESEARCH_INSTANCE_LABELS`, a JSON map from instance id to labels, adds pod labels to individual servers. Pool labels in BLIS apply per role, not per server. |

All research switches and the causal configuration are off by default, so a plain checkout behaves
as `main` does.

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
- **Causal does not mean universally trained.** The causal policy uses no future information at
  runtime, but its thresholds, output weight, and affinity bonus were tuned on seed 42 of this
  workload. Seeds 43–46 hold out arrival order, Poisson gaps, and router tie-breaking—not family
  identities—and the threshold grid uses this corpus's final-work distribution. The 2× rate is
  held out, but a different family corpus remains untested.
- **Observed work is a proxy.** A prompt token is charged at admission and a completed output token
  is weighted 64×. This is simple and auditable, not a learned estimate of actual GPU service.
- **Two loads, one workload.** The primary and doubled arrival rates use one commit0 + Oolong mix.
  Gains may shrink or reverse elsewhere, especially at low load where little work needs sheltering.
- **Research plugins.** Soft routing uses the Alpha `header-profile-handler` and
  `header-label-affinity-scorer` from the pinned router fork through an explicit research opt-in.
- **One optimum is an upper bound.** For the commit0 family `moviepy` (a 67-child fork), the
  isolated optimum is an upper bound, not proven optimal. Its search stopped at 53.74 s against a
  48.44 s lower bound, so its slowdown is slightly understated.
- **The longest families pay.** P3 families are 18.6% slower than under JSQ. A metric weighted
  toward long jobs would rank the policies differently.
