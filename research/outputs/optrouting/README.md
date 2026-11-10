# Optimal isolated root-session latency (commit0, 8 × B200)

For every commit0 agentic family, this directory gives the root session's end-to-end latency when
the family runs **alone** on 8 GPT-OSS-120B (mxfp4) B200 replicas (TP1). The latency is reported
under three routings:

- **Optimal**: the best any work-conserving routing can do.
- **JSQ**: the stock llm-d router config, `research/configs/routers/jsq.yaml`.
- **Least-token**: the stock llm-d router config, `research/configs/routers/least-token.yaml`.

The optimal value is the denominator of the root-session slowdown metric: loaded latency divided
by optimal isolated latency.

## Contents

| Path | What it is |
| --- | --- |
| `optimal_isolated.py` | The whole pipeline: extract families, isolated runs, fork search, report. |
| `replicate.sh` | Driver: clones the pinned catalog and registry if needed, then runs the pipeline. |
| `data/root_e2e.csv` | One row per family: optimal, JSQ and least-token root latency. |
| `data/forks.csv` | One row per fork: lower bound, optimal, JSQ and least-token fork makespan. |
| `data/partitions.json` | The optimal pinned routing: `{family: {spawn request: [[children on GPU 1], ...]}}`. |
| `data/summary.json` | Counts, seeds, consistency checks, per-bucket excess over optimal. |
| `work/` | Scratch and checkpoints (gitignored; safe to delete once `data/` exists). |

`data/` is produced by `replicate.sh`. It doesn't exist until the pipeline has been run.

## Definitions

- **Family**: one trace from `traces/commit0-fanout-buckets/*/traces.jsonl.zst` whose id starts with
  `commit0`, which is 164 families. A family is a root session plus child (subagent) sessions,
  nested one level deep. Requests within a session form a chain.
- **Root end-to-end latency**: from the root's first request to the family's last completion,
  which equals the family makespan. It includes the time spent waiting for child sessions at
  joins. In one family (`c0nat4/graphene_s2`), the root's last request spawns a child that is
  never joined. There the latency runs until that child finishes.
- **Fork**: the child sessions spawned by one root request. The fork makespan runs from the spawn
  request's completion to the last child's completion.
- **Bucket**: the source directory (`w00-04`, …, `w65-69`), named by peak fork width. The commit0
  router-grid experiment used the 6 families in `w25-29` and `w30-34`.
- **Excess**: `jsq_excess_pct = 100 × (JSQ median / optimal − 1)`, and the same for least-token.

## Method

1. **Structure check.** For every fork, the root's next request after the spawn is the join of
   every child, so the root is idle while the fork runs. The exception is a terminal fork, which
   is never joined. The script asserts this, plus one-level nesting and one spawn per child.
2. **Isolated runs.** All families run in one BLIS semi-closed run on 8 replicas, arriving 12,000 s
   apart; the longest family takes about 3,400 s. This is done for each router and each seed
   (default 42–46). The seed only changes router tie-breaking. The script asserts that no two
   families overlap.
3. **Optimal fork routing.** Because the root is idle during a fork, pinning children to GPUs
   makes the GPUs independent. Each GPU's subset of children is simulated as its own
   single-replica run, keeping each child's recorded offset after the spawn. The search works
   like this:
   - **Lower bound:** the slowest child run alone on an idle GPU. No routing can beat it.
   - **Greedy assignment:** a longest-first greedy places each child on the GPU where it finishes
     earliest.
   - **Local search:** move/swap local search runs on any fork the greedy leaves above the bound.

   A fork whose result equals the bound is provably optimal. Pinning is a special case of
   per-request work-conserving routing, so per-request routing can't beat it either.
4. **Optimal family latency.** Routing can't change root requests, which run one at a time, so

   `optimal = observed root latency − Σ over forks (observed fork − optimal fork)`.

   This is computed from every isolated run, and `optimal_spread_s` records how much the results
   disagree; it should be about 0. `summary.json` reports `forks_at_lower_bound` and
   `max_optimal_spread_s`.

The one assumption not verified here is that batching a request with others never makes it finish
earlier than running it alone. The lower bound relies on it.

## Replicating

You need:

- the repo, with `traces/commit0-fanout-buckets/`;
- Go (the version `go.mod` requires), since `research/scripts/semi_closed.sh` builds `blis`;
- Python 3.10 or newer;
- the `zstd` CLI;
- `git` and network access for the first-run clone.

```bash
cd research/outputs/optrouting
./replicate.sh                      # all 164 families, seeds 42-46 → data/
./replicate.sh --jobs 12 --vacuum-jobs 4 --verify 300
```

`replicate.sh` uses `BLIS_CATALOG` and `BLIS_REGISTRY` if they're set. Otherwise it clones
blis-catalog `0.3.0` and blis-registry `v0.2.0` into `research/outputs/deps/`.

Options are listed by `python3 optimal_isolated.py --help`:

- **`--seeds`:** router seeds for the isolated runs.
- **`--jobs`:** parallel single-replica subset batches. Each needs up to about 4 GB of RAM. The
  default is `min(cores / 3, RAM / 4 GB)`.
- **`--vacuum-jobs`:** parallel 8-replica isolated runs, default 3. Each needs about 5–6 GB of
  RAM; running more than RAM allows makes the machine swap to a crawl.
- **`--verify N`:** re-simulates N random cached subsets from scratch and asserts they match.
- **`--only REGEX`:** restricts the run to matching family ids, which needs a fresh `--work`. It's
  useful for a quick check. For example, `--only 'c0nat3/commit0__dulwich__wnat__s0$' --seeds 42 --work /tmp/w --out /tmp/d`
  gives an optimal of 110.38 s and a fork bound of 53.59 s in about 2 minutes.

**Resuming.** The pipeline is resumable. Finished isolated runs are kept in `work/vacuum/`, and
every simulated subset is saved to `work/cache.json` as soon as its batch finishes. After an
interruption, rerun the same command. Only isolated runs without a `workload.json` and uncached
subsets are redone. The cache is only valid for the same BLIS build, catalog and registry, so
delete `work/` if any of those change.

**Cost.** Most of the work is the fork search: well over 10,000 subset simulations, dominated by
the largest forks (up to 67 children). A 300-subset batch takes a single-threaded BLIS process
about 2–3 minutes. On a 24-core, 30 GB machine, the isolated runs took tens of minutes per round,
and the search took several hours. Since batches run in parallel, more `--jobs` (given the RAM)
scales roughly linearly.
