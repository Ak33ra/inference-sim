# Request SPLIT

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


## Execution
- [x] Ground native admission and exact isolated cost.
- [x] Sketch two distinct designs and synthesize.
- [x] Agree. No human checkpoint needed; user accepted semantics.
- [x] Implement with one owner in isolated worktree.
- [x] Verify native admission, oracle parity, old paths and full paired runs.
- [x] Scrap gate: no repeated structural friction; exact shared-core parity passed.
- [x] Report makespan and slowdown; save plot-ready artifacts.
- [x] Commit/PR: skipped; user requested implementation/evaluation in existing uncommitted workspace.

## Throughput checkpoint
- [x] Blocking first steps: settle exact request cost and shared queue boundaries before code.
- [x] Independent workstreams: Astra/Sol read-only design, oracle investigator, root provenance/measurement review.
- [x] Shared mutable state: writer gets a fresh worktree; root alone integrates and launches frozen study.
- [x] Smallest safe decomposition: one owner handles coupled Go policy/CLI and study adapter; independent reviewer and root validate.

## Runtime limits
Configured four-model panel reduced to Astra/Sol because the host has four active slots and no agent close tool; existing agents retain costly subsystem context. No fresh independent reviewer can be spawned under prior capacity rejection; completed independent reviewer reused via followup.

Phase checkpoint: Go core implementation compiles and existing focused tests pass. Independent core review found the exact output-budget guard, now added by owner with a mismatch test. Full legacy parity and frozen study are pending. Python runner and tests remain with the isolated owner.

Full calibration and legacy parity completed. Legacy workload, metrics and routing audit are byte-identical. Seed42 pilot pair verified, including exact JSQ control parity. Eight remaining paired replays launched from the frozen binary. Independent oracle audit checks all56134 shapes, all58741 mappings, single-request concurrency and715871543/10014744 physical tokens.

Completed: all ten runs verified; all five JSQ controls reproduce frozen outputs. Four initial controls hit home quota while saving, were retained, then archived and rerun after a hash-verified scratch migration. Independent audit matched 120 statistics and 60 paired changes. Mean family makespan increases 2.11% and mean session elapsed 2.17% (median paired change). Report: research/writeups/mega-request-split-4s/README.md. All 15 integrated source hashes match reviewed code; 1250-file starting inventory has no unexpected modifications. Protected prior-study helpers match.
