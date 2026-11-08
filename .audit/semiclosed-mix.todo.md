# Overlapping explicit-DAG traces

### Feature

**You own the design. Plan, review, verify.** Delegate implementation. Stay in the lead.

1. `how` over the affected subsystem.
2. `architect` for parallel design exploration.
3. Write the throughput checkpoint as four todo items. A dimension that genuinely does not apply (single file, no fan-out) keeps its item with `n/a: <reason>` rather than being dropped:
   - **Blocking first steps.** Gates run before fan-out.
   - **Independent workstreams.** Disjoint files, services, or layers parallelize. Shared writes serialize.
   - **Shared mutable state.** Default to splitting the target (the **separate-before-serializing-shared-state** principle skill). Serialize only for real invariants.
   - **Smallest safe decomposition.** If one worker is best, name why.
4. Delegate code-writing to a subagent using your configured feature model (default in poteto-mode's Models section) with a specific scope (file paths, named data shape and its organizing structure per **principle-model-the-domain**, a state machine over scattered booleans, a table/registry over branching, a typed model over repeated shape assumptions, chosen before the delegate writes logic, and success criteria). Pick each subagent's reasoning level for the job, per the Subagents section. When the implementation admits multiple valid shapes (error handling, abstraction layer, test structure), delegate via the **arena** skill instead so the runners surface the alternatives and the cross-judge guards the pick. Mandatory: no skip-with-reason escape, and Laziness Protocol does not override it (the gain is review separation, not lines saved). A subagent forbidden to spawn satisfies this by owning the diff directly with the same review separation. No "standing by" reply that waits on a nested agent. **Give every file-writing delegate its own worktree** (spawn it with `isolation: "worktree"`, or hand it an exclusive branch), and do not write files or run a suite in a worktree a delegate still holds. On Claude Code, `isolation: "worktree"` branches from the remote default branch unless the `worktree.baseRef` setting is `"head"`. When the delegate builds on commits the default branch lacks, commit them, create its worktree with `git worktree add <path> -b <delegate-branch> HEAD`, and name that base commit in the brief. Fencing a file in the brief's prose is not a lock (**principle-separate-before-serializing-shared-state**). Comments per **Comments**. Surgical edits, re-ground against the source for upstream-derived files. Port shared-primitive improvements to all consumers and verify each. Commit liberally.
5. Verify on the matching surface. "Inconclusive" or wrong-surface is not a pass. Flag it.
6. Rebase into small, ordered commits. Stack follow-ups.
   Use the **sequence-verifiable-units** principle skill, building, verifying, and committing each small unit before the next.
7. If the design is contested, `interrogate` before shipping.
8. Run **Opening a PR**.

Code-coupled work (one feature, one migration) goes to a single owner with the checkpoint inline. That owner fans out internally after the blocking phase. Parent-level fan-out is for slices that produce independent artifacts (audits, cross-subsystem investigations, competing experiments). Rewrite the checkpoint at phase boundaries. Spawn a fresh owner rather than chaining interrupts.

**Reply:** what you built, what you chose and why, the throughput checkpoint, open decisions. Tables for design alternatives.


## Task checkpoint

- [x] Blocking first steps. Inspect current launcher, DAG loader and new source schema.
- [x] Independent workstreams. Local implementation; independent read-only reviewer checks design and results.
- [x] Shared mutable state. Only main agent writes; existing user changes retained.
- [x] Smallest safe decomposition. One preparation module composes sources; existing replay engine and arrival scheduler remain authoritative.
- [x] Ground. All eight Commit0 buckets use supported DAG fields and gpt-oss-120b.
- [x] Sketch. Compare Go multi-input loader against Python materialization. Choose materialization to keep research artifact separable, reproduce exact input and avoid engine changes. Model Source(path, copies) with deterministic interleaving and explicit provenance.
- [x] Agree. User authorized feature; no remaining product question blocks a useful default.
- [x] Implement composition and launcher integration.
- [x] Add boundary and conservation tests.
- [x] Run mixed real-source smoke, verify overlap and exact source/token/dependency conservation.
- [x] Review, docs, examples and final instructions.
- [x] Scrap. n/a unless implementation reveals design friction.

Delegation: previous session capacity rejection and no close_agent tool prevent fresh code-writing agents. Exploration falls back to a labelled sequential design pass. Finished independent reviewer config_design_astra is reused through followup_task under the skill exception. Main agent owns the diff. No commit/PR requested; skip repository publication and rewriting unrelated work.


## Completed verification

Model the Domain kept the new behavior in Source(path, copies) and one preparation
module, research/semiclosed/mix.py. Prove It Works required driving the real config
and verifying engine outputs against original sources. Existing native engines
and Go workload code received no changes in this task.

- All 32 Python mixture, config, and DAG-verifier tests pass.
- Go semi-closed workload/cluster tests selected by Test(SemiClosed|DAG|Arrival) pass.
- The complete commit0-overlap config finished with 12 root families, 408 sessions,
  13,914 requests, 162,892,506 prompt tokens, 2,340,834 output tokens, and 14,298 DAG
  edges. Peak active families was 12. All four replicas served work. Cache hit rate
  was zero, as configured. No failed, censored, blocked, pending or issued requests
  remained. Original-source composition verification and engine verification pass.
- Saved-run verify and config dry-run were driven through semi_closed.sh.
- All eight source buckets were composed and independently verified without loss
  at research/outputs/analysis/commit0-composition. Totals were 262 families,
  1,752 sessions, 57,133 requests, 681,522,480 input and 9,701,003 output tokens.
  Native LoadInput/NewReplay validated this full corpus with trace, fixed-zero,
  fixed-0.5-second, and Poisson-2-per-second schedules. This larger corpus was
  validated as input; it was not fully simulated.
- Independent reviewer config_design_astra also ran a 15-request native mixed
  replay in all four arrival modes. Namespace, dependency and corruption probes
  passed. The no-comments review found no issues in the additions.
- Review caught missing-manifest fallback to raw verification and a dry-run uv
  provisioning regression. Both were reproduced, fixed, and covered by shell-level
  regression tests. Reviewer final verdict was Pass with no blocking findings.
- bash -n and git diff --check pass. No publication or commits requested.
