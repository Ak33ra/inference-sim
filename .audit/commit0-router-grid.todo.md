# Commit0 routing grid

- [x] Ground the requested population and router meanings. Same two buckets, copies 2 each. User confirmed JSQ waiting plus running.
- [x] Model the Domain. Use native EndpointPickerConfig snapshots as the boundary; add no routing logic.
- [x] Compare design alternatives. Native scenario-only sweep is possible but bypasses config reproducibility. Choose optional endpoint_picker_config in serving.router, saved into each generated deployment.
- [x] Throughput checkpoint. Finish launcher/profile validation before running the grid. Independent runs get separate directories. Main is the only code writer; independent read-only reviewer. Existing fresh-agent capacity limit prevents new delegates; reuse finished reviewer through supported follow-up exception.
- [x] Add six configs and native profiles. Existing default configuration remains absent when router is omitted.
- [x] Run all six complete primary experiments and source/engine verification.
- [x] Repeat with five matched router seeds to measure tie-breaking variation.
- [x] Check the selected placements use the requested counter and token scoring does not saturate.
- [x] Compute family and per-fork makespan/mean/median finish statistics with matched populations; export CSV/JSON/plots.
- [x] Independent review, document configs and report complete grid.

Benchmark checklist applies. The measurement is virtual engine time. Host profiling is not relevant to the claim. Record input conservation, zero errors, source digests, topology, replica utilization, seed variability, and limits on token scoring. No commits or PR requested.

Final evidence: research/outputs/analysis/commit0-router-grid/{summary.json,report.txt,cells.csv,runs.csv,forks.csv,critical_paths.csv,makespan-grid.png}. Thirty complete runs, 13,914 requests each; exact source tokens, no failures/truncations/drops. Identical source, dependency/delay signature, births, model/engine/layout per replica and provenance. All replicas receive work. 400,987 isolated arrivals match minimum-load placement; 16,433 coincident events are excluded because reports lack event ordering. Max projected prompt load 615,058 < 4,194,304 scorer cap.

The first profiles exposed a hidden native default: utilization-detector is injected before scoring and can remove the true least-loaded replica. An independent bounded log capture proved this. Final profiles explicitly disable flow control and wrap the pool detector in a composite that implements no endpoint Filter. Exact-placement audit now gates every run before repeating seeds. Initial thirty filtered runs remain intact; their analysis is commit0-router-grid-filtered. Final run directories end in -pure. No Go engine edits in this task.

Independent reviewer approved launcher/profile changes and recomputed all 18 fork results for a completed pure run. Critical-path decomposition exactly reproduces all family makespans. Thirty-four targeted Python tests pass; git diff --check passes; final plot inspected. Default routing remains unchanged when serving.router is omitted.
