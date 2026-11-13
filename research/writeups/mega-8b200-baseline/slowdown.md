# Mega-trace slowdown against optrouting

For the eight-B200 JSQ baseline with Poisson roots averaging 12 seconds apart,
we divide each loaded duration by its matching published `optimal_s`.
The tables show the median across arrival seeds 42–46 of each per-run statistic.
Ratios are computed per family or fork before aggregation, with equal weight
per object. All completed work is included, including the final drain.

| Cohort | Objects per run | Mean | Median | p90 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Families | 189 | 1.089× | 1.057× | 1.189× | 1.453× |
| Forks with two or more children | 160 | 1.160× | 1.100× | 1.368× | 1.730× |
| All forks, including one-child groups | 613 | 1.094× | 1.042× | 1.257× | 1.580× |

| Dataset and cohort | Objects per run | Mean | Median | p90 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Commit0 families | 164 | 1.074× | 1.053× | 1.170× | 1.295× |
| Oolong families | 25 | 1.179× | 1.104× | 1.349× | 1.803× |
| Commit0 multi-child forks | 113 | 1.132× | 1.084× | 1.338× | 1.525× |
| Oolong multi-child forks | 47 | 1.227× | 1.189× | 1.465× | 1.790× |

Family time runs from root arrival to the last family completion. Fork time
runs from the spawning request’s completion to the last child completion,
matching optrouting. This origin differs from the earlier finish-time comparison,
which started at the earliest child arrival. Both durations include recorded delays.

The matched population is 164 Commit0 families plus 25 Oolong families. The 98
SWE-bench families in the mega trace have no published denominator and are
excluded. All 613 published forks match; 160 have multiple children.
We use the published best-found `optimal_s` values, including cases whose exact
optimum is unresolved. These ratios are relative to that reference, not a claim
that every denominator is the proven theoretical minimum.

Per-seed variation:

| Statistic | Median across seeds | Seed range |
| --- | ---: | ---: |
| Family median | 1.057× | 1.056–1.061× |
| Family p99 | 1.453× | 1.376–1.569× |
| Multi-child fork median | 1.100× | 1.088–1.117× |
| Multi-child fork p99 | 1.730× | 1.518–1.922× |

Verification checks complete replay and token accounting, exact trace hashes,
family identities, and every fork’s child count. Loaded fork durations are
extracted by optrouting’s own `observed()` function and independently checked
against saved session completion times. Ratios just below one correspond to
differences below 0.5ms, consistent with the baseline CSV’s millisecond rounding;
they are retained without clamping.

Artifacts: [summary and seed ranges](../../outputs/analysis/mega-8b200-baseline/slowdown/summary.json),
[family rows](../../outputs/analysis/mega-8b200-baseline/slowdown/family-slowdowns.csv),
[fork rows](../../outputs/analysis/mega-8b200-baseline/slowdown/fork-slowdowns.csv),
and the [analysis script](slowdown.py). The summary records hashes of all four
published baseline tables.

Regenerate from the saved runs:

```bash
python3 -B research/writeups/mega-8b200-baseline/slowdown.py
```

Pass run directories as positional arguments and `--output DIR` to compare
another policy using this same mega trace and published baselines.
