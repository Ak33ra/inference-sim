# Session SPLIT implementation handoff

Worktree: `/scratch/akira/session-split-implementation`.

Integration payload: the 21 paths and SHA-256 hashes in `.audit/split-changed-files.json`, computed relative to root's `.audit/split-overlay.json`. Do not copy other baseline overlay changes or generated `.audit`/rapid fixtures. Frozen semi-closed and policy-study helpers are unchanged. Reconciled design is `.audit/split-design/accepted.md`; caller commands are in `research/session_split/README.md`.

Completed implementation: central native-admission source and session ready map; static remaining oracle validation; native per-endpoint renderer preparation round-robin; deferred client completion lifecycle; exact metrics attribution; logical nonpreemptive KV promises including declared bounded-state envelope; full selection/preparation/capacity audit; frozen paired runner and raw elapsed/slowdown analysis.

Validation:
- Focused SessionSplit, Lifecycle and ResearchRouter Go tests pass, including tight capacity, native chunking, zero-preparation same-time successors with native output overhead, renderer contention/exactly-once charge and malformed reference timelines.
- All 71 Python research tests pass.
- Simulator, KV, kernelmodel, researchrouter and command suites pass (command suite 188.907s). Cluster suite passes excluding the one preexisting randomized P/D case described below.
- `git diff --check` passes.
- Real CLI through the adapted runner completes 4 original families / 97 sessions / 1,104 requests / 16,301,183 prompt tokens / 185,636 output tokens, with 1,193 dependency edges. The independent audit passes. Maximum native batch at admission is 23; all physical and promised KV releases finish at zero; preemptions, physical allocation failures and logical capacity refusals are zero. Renderer wait totals 3,657,381us across 1,104 renders. Evidence is `.audit/split-fixture/run/` and `.audit/split-fixture/audit-verification.json`.

The randomized `TestPDDecodeAdmission_LawsHoldUnderRandomSaturation` found inputs prefills=1, requests=6, in=57, out=44, decodeBlocks=12 (seed 17083441267423329396), completing 3/6. Root reproduced the exact saved case before SPLIT integration in `.audit/split-pd-baseline.log`. No P/D fix is included. Worktree logs: `.audit/split-cluster-tests.log`, `.audit/split-cluster-excluding-known-pd.log`; reproducer: `sim/cluster/testdata/rapid/TestPDDecodeAdmission_LawsHoldUnderRandomSaturation/TestPDDecodeAdmission_LawsHoldUnderRandomSaturation-20261120192228-2961210.fail`. Keep this diagnostic outside the integration payload.

No full experiment, commit or PR has been performed. Root owns integration, baseline parity, full paired runs and final results.
