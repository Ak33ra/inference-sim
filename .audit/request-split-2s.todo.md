# Request SPLIT, two-second arrivals

## Prototype measurement playbook
1. Scope the decision the prototype exists to make: whether the existing policy comparison changes under twice the root arrival rate.
2. Gather references when the design space is open. Skip: direction is fully specified; use verified four-second study.
3. Build throwaway in an isolated scratch dir, separate from production source. A small standalone measurement driver owns derived configs and reuses native execution.
4. When comparing alternatives, build them behind one switcher, each variant labeled. Existing JSQ/request-SPLIT router settings are the switch.
5. Verify on the matching surface. Full native runs, priority-event audits, physical tokens, causal delays, and independent numerical recalculation.
6. Present alternatives, tradeoffs, and a recommendation. Save comparison, exact configs, raw outputs and plotting rows.

## Throughput checkpoint
- Blocking first steps: confirm frozen binary, trace, oracle, reference identity and adaptable native execution before any run.
- Independent workstreams: Astra writes the driver in its exclusive worktree; Sol reviews measurement validity; root owns output setup and independent audits/report.
- Shared mutable state: new driver file has one writer. Root alone integrates and writes new study; prior study is read-only.
- Smallest safe decomposition: simulator unchanged; one new measurement script, separate raw output directory, one report.

## Progress
- [x] Scope and source-study identity.
- [x] Driver and independent review.
- [x] Seed42 pilot and accounting/priority checks.
- [x] Five matched seed pairs.
- [x] Independent statistics and saved report/CSV.
- [x] Final protected-file and provenance checks.
