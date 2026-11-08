# Standard analysis validation

Source baseline: `47cffc8baa4f491923a3392e5c3c87397a4f9ec8`.

The task adds shared normalization/statistics, plotting and report export under
`research/analysis`, a thin script, a standard notebook, and documentation. It
does not change simulator output schemas or the partner's makespan work.

Independent source review established that scheduling delay includes admission
work and can be overwritten on preemption; no clean queue/prefill/KV phase split
is emitted. It also confirmed existing finite-family makespan, raw/aggregate E2E
differences, source/simulator model distinction, units, and incomplete-row rules.

The independent implementation review found three defects: contradictory family
summaries could produce false improvements, source/prefix provenance was missing
from pairing, and the final server time-series bin was omitted. All were fixed
and independently rechecked with their reproductions. Regression checks now
reject contradictory membership/state/timestamps, exclude unknown or different
source provenance, and render complete one-bin and multi-bin spans.

`uv run --with-requirements research/analysis/requirements.txt python -B -m unittest
discover -s research/scripts/tests -v` passes 32 tests, including 12 new analysis
tests and all 20 previous comparison/config tests. The new tests cover additive
quantile attribution, deterministic ties, preemption/missing timing, warmup and
zero-output populations, censoring, matching source work, provenance, generated
notebook validity, fresh output directories, immutable inputs, metrics-only runs,
and server-series bin boundaries.

Real reports:

- `research/outputs/analysis/standard-reviewed-20261104` compares the 175-request
  GLM smoke result with the saved identical legacy result. All TTFT mean/p50/p90/
  p95/p99 values match the emitted aggregate values within floating-point
  tolerance. Each stacked bar sums to its total. Both families pair and have
  zero makespan change. The source contains ten sessions.
- `research/outputs/analysis/standard-agentx-20261104` reads an actual metrics-only
  AgentX result. It reports 124 rows, one warmup row, and 123 eligible completed
  non-warmup rows. It labels the unreconstructable explicit measurement window
  and missing family data.

The standard notebook and generated notebook executed all 11 code cells in real
Jupyter kernels. The standard notebook ran from `research/notebooks`; generated
notebook execution ran from `/tmp`. Executed copies are retained under
`research/outputs/analysis`. PNGs were visually inspected. The generated report
contains PDF/PNG figures, embedded notebook outputs, normalized CSVs, coverage,
and source hashes. Documentation relative links resolve.

Measured values are existing simulator results, not new hardware measurements
or policy-performance claims. No confidence intervals or significance claims are
made. Separate seeds stay separate.
