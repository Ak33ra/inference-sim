# Optimal isolated root-session latency (commit0 and Oolong, 8 × B200)

For every commit0 and Oolong agentic family, this directory gives the root session's end-to-end
latency when the family runs **alone** on 8 GPT-OSS-120B (mxfp4) B200 replicas (TP1). The latency
is reported under three routings:

- **Optimal**: the best work-conserving routing, or a tight bracket around it where it can't be
  proven (see [How exact the optimum is](#how-exact-the-optimum-is)).
- **JSQ**: the stock llm-d router config, `research/configs/routers/jsq.yaml`.
- **Least-token**: the stock llm-d router config, `research/configs/routers/least-token.yaml`.

The optimal value is the denominator of the root-session slowdown metric: loaded latency divided
by optimal isolated latency.

## Contents

| Path | What it is |
| --- | --- |
| `optimal_isolated.py` | The whole pipeline: extract families, isolated runs, fork search, report. |
| `replicate.sh` | Driver: clones the pinned catalog and registry if needed, then runs the pipeline. |
| `data/` | Commit0 results (164 families), produced by `./replicate.sh`. |
| `data/oolong/` | Oolong results (25 families), produced by `./replicate.sh --dataset oolong`. |
| `work/`, `work/oolong/` | Scratch and checkpoints (gitignored; safe to delete once the data exists). |

Each data directory holds:

| File | What it is |
| --- | --- |
| `root_e2e.csv` | One row per family: optimal, lower bound, JSQ and least-token root latency. |
| `forks.csv` | One row per fork: lower bound, optimal and how it was proven, JSQ and least-token fork makespan. |
| `partitions.json` | The best pinned routing: `{family: {spawn request: [[children on GPU 1], ...]}}`. |
| `summary.json` | Counts, seeds, consistency checks, per-bucket excess over optimal. |

## Datasets

| `--dataset` | Families | Source | Serving template |
| --- | --- | --- | --- |
| `commit0` (default) | 164 | `traces/commit0-fanout-buckets/*/traces.jsonl.zst`, ids starting `commit0` | `research/configs/semiclosed/commit0-overlap-jsq-8replicas.json` |
| `oolong` | 25 | `natural-oolong-fanout.jsonl.zst` (repo root) | `research/configs/semiclosed/natural-oolong-8nodes.json` |

Both templates set the same serving: GPT-OSS-120B mxfp4, one B200 per replica, TP1, prefix
caching off. The script copies the serving block and replaces the trace, router, replica count,
seed and arrivals.

## Definitions

- **Family**: one trace line. A family is a root session plus child (subagent) sessions, nested
  one level deep. Requests within a session form a chain.
- **Root end-to-end latency**: from the root's first request to the family's last completion,
  which equals the family makespan. It includes the time spent waiting for child sessions at
  joins. In one commit0 family (`c0nat4/graphene_s2`), the root's last request spawns a child that
  is never joined. There the latency runs until that child finishes.
- **Fork**: the child sessions spawned by one root request. The fork makespan runs from the spawn
  request's completion to the last child's completion.
- **Bucket**: for commit0, the source directory (`w00-04`, …, `w65-69`), named by peak fork width.
  The commit0 router-grid experiment used the 6 families in `w25-29` and `w30-34`. Oolong has a
  single bucket, `oolong`.
- **Excess**: `jsq_excess_pct = 100 × (JSQ median / optimal − 1)`, and the same for least-token.

## Method

1. **Structure check.** For every fork, the root's next request after the spawn is the join of
   every child, so the root is idle while the fork runs. The exception is a terminal fork, which
   is never joined. The script asserts this, plus one-level nesting and one spawn per child. Both
   datasets pass.
2. **Isolated runs.** All families run in one BLIS semi-closed run on 8 replicas, arriving 12,000 s
   apart; the longest commit0 family takes about 3,400 s. This is done for each router and each
   seed (default 42–46). The seed only changes router tie-breaking. The script asserts that no two
   families overlap.
3. **Fork routing.** Because the root is idle during a fork, pinning children to GPUs makes the
   GPUs independent. Each GPU's subset of children is simulated as its own single-replica run,
   keeping each child's recorded offset after the spawn. The search works like this:
   - **Lower bound:** the slowest child run alone on an idle GPU. No routing can beat it.
   - **Greedy assignment:** a longest-first greedy places each child on the GPU where it finishes
     earliest.
   - **Local search:** move/swap local search runs on any fork the greedy leaves above the bound.
   - **Exhaustive search:** a fork that is still above the bound, and has at most 12 children, has
     every split of its children across all 8 GPUs enumerated and scored.
4. **Family latency.** Routing can't change root requests, which run one at a time, so

   `optimal = observed root latency − Σ over forks (observed fork − best fork)`
   `optimal_lower_bound = observed root latency − Σ over forks (observed fork − fork lower bound)`.

   This is computed from every isolated run, and `optimal_spread_s` records how much the results
   disagree; it should be 0.

## How exact the optimum is

`forks.csv` labels each fork in its `proof` column:

| `proof` | Meaning |
| --- | --- |
| `lower_bound` | The best routing equals the lower bound. It's optimal over all work-conserving routings, including per-request routing. |
| `exhaustive_pinned` | No pinning of whole child sessions to GPUs does better. A routing that moves a session between GPUs mid-chain might, by up to `optimal_s − lower_bound_s`. |
| `none` | The best pinned routing found by greedy and local search. The true optimum lies between `lower_bound_s` and `optimal_s`. |

The lower bound can't be reached when a fork has more children than GPUs and its children are
short: some children must then share a GPU, and the shared GPU finishes after the slowest child
would alone.

In `root_e2e.csv`, a family's optimum is exact when `forks_not_at_bound` is 0. Otherwise the true
optimum lies between `optimal_lower_bound_s` and `optimal_s`. `summary.json` reports
`forks_by_proof`, `families_optimal_equals_lower_bound` and `max_optimal_minus_lower_bound_s`.

Every lower bound relies on one assumption that isn't verified here: batching a request with
others never makes it finish earlier than running it alone. The exhaustive search relies on the
same assumption, since it only enumerates splits that use all 8 GPUs.

## Oolong results

These are from `./replicate.sh --dataset oolong` on seeds 42–46, which took 5.5 minutes on a
24-core machine.

- **Proofs:** of the 67 forks, 61 reach the lower bound, 3 are `exhaustive_pinned` and 3 are
  `none`; the last three have 20, 39 and 44 short children.
- **Exactness:** for 20 of the 25 families the optimum is exact. For the other 5, the gap between
  the best routing found and the lower bound is 0.03–1.03 s, which is at most about 4% of the
  family's latency.
- **Agreement:** `max_optimal_spread_s` is 0.0 across all 10 isolated runs, and re-simulating 100
  cached subsets matched exactly.
- **Excess over optimal:** median 1.48% for JSQ (max 9.19%) and 1.54% for least-token (max 5.62%).
  The Oolong children are short, around a second each, so a single placement decision is likely a
  larger share of each fork than it is in commit0.

## Replicating

You need:

- the repo, with `traces/commit0-fanout-buckets/` and `natural-oolong-fanout.jsonl.zst`;
- Go (the version `go.mod` requires), since `research/scripts/semi_closed.sh` builds `blis`;
- Python 3.10 or newer;
- the `zstd` CLI;
- `git` and network access for the first-run clone.

```bash
cd research/outputs/optrouting
./replicate.sh                                   # commit0: 164 families, seeds 42-46 → data/
./replicate.sh --jobs 12 --vacuum-jobs 4 --verify 300
./replicate.sh --dataset oolong --vacuum-jobs 10 # oolong: 25 families → data/oolong/
```

`replicate.sh` uses `BLIS_CATALOG` and `BLIS_REGISTRY` if they're set. Otherwise it clones
blis-catalog `0.3.0` and blis-registry `v0.2.0` into `research/outputs/deps/`.

Options are listed by `python3 optimal_isolated.py --help`:

- **`--dataset`:** `commit0` (default) or `oolong`. This also sets the default `--work` and
  `--out`.
- **`--seeds`:** router seeds for the isolated runs.
- **`--jobs`:** parallel single-replica subset batches. Each needs up to about 4 GB of RAM. The
  default is `min(cores / 3, RAM / 4 GB)`.
- **`--vacuum-jobs`:** parallel 8-replica isolated runs, default 3. A commit0 run needs about 5–6 GB
  of RAM, and running more than RAM allows makes the machine swap to a crawl. Oolong runs are
  small, so all 10 can run at once.
- **`--verify N`:** re-simulates N random cached subsets from scratch and asserts they match.
- **`--only REGEX`:** restricts the run to matching family ids, which needs a fresh `--work`. It's
  useful for a quick check. For example, `--only 'c0nat3/commit0__dulwich__wnat__s0$' --seeds 42 --work /tmp/w --out /tmp/d`
  gives an optimal of 110.38 s and a fork bound of 53.59 s in about 2 minutes.

**Resuming.** The pipeline is resumable. Finished isolated runs are kept in `WORK/vacuum/`, and
every simulated subset is saved to `WORK/cache.json` as soon as its batch finishes. After an
interruption, rerun the same command. Only isolated runs without a `workload.json` and uncached
subsets are redone. The cache is only valid for the same BLIS build, catalog and registry, so
delete the work directory if any of those change.

**Cost.**

- **Commit0:** most of the work is the fork search, with well over 10,000 subset simulations,
  dominated by the largest forks (up to 67 children). A 300-subset batch takes a single-threaded
  BLIS process about 2–3 minutes. On a 24-core, 30 GB machine, the isolated runs took tens of
  minutes per round, and the search took several hours. Since batches run in parallel, more
  `--jobs` (given the RAM) scales roughly linearly.
- **Oolong:** about 5 minutes in total.
