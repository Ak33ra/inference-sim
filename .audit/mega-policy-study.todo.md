# Mega trace policy study

- [x] Read the poteto-mode principles and apply the research verification workflow.
- [x] Frame. Compare family and fork mean, median and p99 makespan and per-object slowdown at 4s and 8s Poisson root arrivals on the same eight B200 replicas.
- [x] Scope. Five paired arrival seeds, cache-off routing/scheduling controls, Agentix ATLAS, and a separate cache-on routing study including Agentix affinity and Preble E2.
- [x] Ground. Inspect the scheduler, gateway, replay, verification and existing isolated references.
- [x] Sketch. Compare independent scheduling designs and review the routing integration before implementation.
- [x] Agree. Select the smallest faithful policy ports; state all differences from the full published systems.
- [x] Implement. Add scoped policy modules, explicit configuration, instrumentation and behavioral tests.
- [x] Scrap. Revisit a design if observed execution violates causality, progress preservation or accounting.
- [x] Verify defaults against a binary built before the changes, and verify constrained cases actually exercise each policy.
- [x] Run all conditions at both arrival rates and five matched seeds, interleaving conditions; verify every saved run.
- [x] Extract per-family/per-fork rows and per-run/aggregate CSV and JSON for later plotting. Audit slowdown reference comparability, especially cache-on runs.
- [x] Independently review code and measurements, then write a concise report under research/writeups/mega-policy-study.

## Throughput checkpoint

- Blocking first steps. Freeze a pre-change binary and source inputs; settle suspension/resume semantics and routing signals before launching the full grid.
- Independent workstreams. Scheduler and research router owners work in separate worktrees; the root owns the experiment runner, configuration adapter, analysis and report.
- Shared mutable state. Each simulator run has its own directory and each writer its own checkout. Merge patches sequentially. Program service state belongs to one simulation's event loop.
- Smallest safe decomposition. One scheduler owner handles its coupled lifecycle changes; one router owner handles routing state and hooks. The root integrates and independently verifies both.

## Acceptance gates

Every reported run completes all 58,741 requests, 287 families and 2,385 sessions with exact logical prompt/output token counts and all 60,551 dependency edges verified. Default execution paths remain working. Online policies cannot read future output lengths or unreleased DAG nodes. Saved artifacts identify the exact binary, policy parameters, trace, arrival seeds, router and cache settings. Policy counters prove whether each mechanism is exercised. Slowdown denominators and coverage are explicit.
