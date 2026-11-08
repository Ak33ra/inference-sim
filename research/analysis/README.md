# Compare saved simulator runs

Create a report from one or more run directories. Each directory needs
`metrics.json`. Semi-closed `workload.json` adds family and session analysis.

```bash
uv run --with-requirements research/analysis/requirements.txt \
  python research/scripts/analyze_semiclosed.py \
  --run baseline=research/outputs/semiclosed/config-glm52-smoke-20261104 \
  --output research/outputs/analysis/my-report
```

Add named policies with another `--run LABEL=DIR`. Use quotes around an argument
whose directory contains spaces. The first run is the pairing baseline unless
you provide `--baseline LABEL`.

```bash
uv run --with-requirements research/analysis/requirements.txt \
  python research/scripts/analyze_semiclosed.py \
  --run baseline=/path/to/baseline-run \
  --run cache-policy=/path/to/policy-run \
  --baseline baseline \
  --output research/outputs/analysis/policy-comparison
```

Choose a fresh output directory. The script reads simulation outputs without
changing them. A report contains these artifacts:

- `report.ipynb` has embedded plots and cells that rerun the shared analysis.
- PNG and PDF files contain the TTFT bars, latency distributions, agent lifetime
  distributions, family progress, and matched-family policy comparison.
- `requests.csv`, `families.csv`, and `sessions.csv` contain normalized data.
- `ttft_statistics.csv` contains bar heights, component attribution, and eligibility.
- `paired_families.csv` and `pairing_coverage.csv` record matches and exclusions.
- `summary.json` records sample counts, notices, saved configuration, emitted
  aggregates, observed fields, and SHA256 hashes of the inputs used.

Metrics-only runs produce request plots. Missing family or time-series data is
reported as unavailable. The script does not recover measurements that were not
recorded. A report is an analysis of saved outputs, not a replacement for the
[source/token verifier](../semiclosed/README.md).

## Use the notebook

Open [the standard notebook](../notebooks/semiclosed_analysis.ipynb) in Jupyter or
your editor. Select a Python environment with the dependencies in
[requirements.txt](requirements.txt). Edit `RUNS` and `BASELINE`, then execute all
cells. Start its kernel within this repository so it can locate the shared code.

For the generated `report.ipynb`, the script fills in absolute repository and run
paths. Its embedded plots can be viewed without rerunning it. To rerun a report
after moving it to another machine, edit those paths first.

The notebook exposes `requests`, `families`, `sessions`, `ttft`, `pairs`,
`pair_coverage`, and `summary` as pandas data frames. Add exploratory cells using
those tables. The CLI and notebook use the same parser and plotting functions.

## Record server timelines

Set a positive interval in a simulator run config before replay:

```json
"run": {"series_interval_s": 1}
```

The report then adds waiting, running, and KV occupancy plots per engine.
Select an interval appropriate for the simulated duration. A full trace can
span weeks, so one-second recording can create large arrays.

## Interpret a comparison

Read [the field and statistic definitions](fields.md) before interpreting TTFT
components or comparing measured aggregates with request rows.

Check completeness, sample counts, and pairing coverage first. Matched families
must have identical source request identities, timestamps, token counts,
dependencies, recorded delays, and source/converter provenance. Different
serving models, hardware, policies, and
seeds are retained in the input/configuration metadata for review. Matching
source work does not prove that only one experimental variable changed.

Use [the plot recommendations](study_plots.md) to choose the next measurements
for an agent-makespan study.

Run the analysis checks with:

```bash
uv run --with-requirements research/analysis/requirements.txt \
  python -B -m unittest discover -s research/scripts/tests -v
```
