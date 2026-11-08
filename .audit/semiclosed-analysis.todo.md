# Standard semi-closed analysis

- [x] `how` over the affected subsystem. Inspect native emitters, existing comparison script, and actual output directories. Independent read-only reviewer checks timing semantics.
- [x] `architect` for parallel design exploration. Capacity fallback applies from the previous turn: no fresh agent slots or close-agent tool. Compare two designs sequentially and obtain independent review.
- [x] Write the throughput checkpoint as four todo items.
  - Blocking first steps. Establish units, populations, timing components, and existing makespan semantics before plotting.
  - Independent workstreams. Reviewer inspects native semantics while parent designs and implements separate analysis artifacts.
  - Shared mutable state. Only parent writes the new analysis files. No simulator or partner-owned makespan code changes.
  - Smallest safe decomposition. A normalized run model feeds shared statistics and plotting functions, used by both CLI and notebook.
- [x] Delegate code-writing to a subagent. Capacity fallback: no fresh writer can be dispatched; parent owns implementation, existing non-author reviewer supplies independent review. Review of design and implementation completed, then all three findings fixed and independently rechecked.
- [x] Verify on the matching surface. Real GLM and AgentX reports generated; notebooks executed; figures inspected; 32 tests pass including 12 new analysis cases. Detailed evidence in semiclosed-analysis-verification.md.
- [x] Keep changes scoped to analysis, notebook, tests, documentation, and narrow ignore exceptions. No simulator, converter, or run script behavior changes. No commit or remote publication requested.
- [x] Design settled with independent review; extra interrogation not needed.
- [x] Local review and verification complete. No external PR requested.

## Design

The user needs a standard notebook and script for comparing policy outputs, especially agent makespan, plus an inventory of currently emitted data. Design A normalizes each directory once into a RunData model and exposes reusable statistics/plots to the CLI and notebook. Design B makes the notebook the canonical implementation and executes it through a parameterization tool for batch reports. Choose A: it permits direct statistical tests, keeps notebook cells short, and avoids duplicate parsers and an execution-framework dependency.

Use an isolated research/analysis package, a thin research/scripts/analyze_semiclosed.py entry point, and a standard notebook under research/notebooks. Accept repeated --run LABEL=DIR and a fresh --output directory. First run is the default pairing baseline. Save normalized CSV tables, summary JSON, PNG/PDF figures, and a populated rerunnable report notebook with embedded images. A metrics-only run supplies request plots; optional workload/config/verification files add family/session and provenance views. Every run remains separate; do not pool seeds or imply confidence intervals.

Quantiles use NumPy linear interpolation, consistent with BLIS. Mean stacks use means of components. Quantile stacks sort by total TTFT and interpolate the same request component vectors at the total's rank, never independent component percentiles. Current available components are scheduling delay and post-admission residual, not pure queue/prefill. Preemption can overwrite scheduling delay, so preempted runs show unsplit totals. Missing or invalid component measurements show an unsplit total rather than fabricated zeros. KV fetch duration is unavailable and is not inferred from token counters.

Request statistics use completed non-warmup rows, retain zero-output simulator TTFT, and distinguish raw engine E2E from measured aggregate E2E that subtracts hidden completion overhead. Explicit measured windows cannot be reconstructed exactly from current per-request output. Family makespan uses the existing complete-family report; session lifetime is completion minus arrival. Pair family policy deltas only for identical source work and dependency graphs; report unmatched, incomplete, and incompatible coverage.

Core plots: TTFT statistics with valid attribution; TTFT/E2E distributions; family/session lifetime distributions; paired family makespan changes; family active/completion timelines; optional per-engine waiting/running/KV series. Documentation lists further useful plots and instrumentation gaps without implementing new simulator metrics.
