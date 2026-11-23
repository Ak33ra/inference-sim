# Clairvoyant session reservation experiment

- [x] `how` over the affected subsystem.
- [x] `architect` for parallel design exploration.
- [x] Write the throughput checkpoint as four todo items.
  - Blocking first steps. Resolve session ranking, server eligibility and family grouping before implementing policy semantics.
  - Independent workstreams. Source/design inspection and baseline/reference auditing can proceed independently.
  - Shared mutable state. Preserve the previous frozen study; use a separate experiment output directory and one implementation owner.
  - Smallest safe decomposition. One owner for routing and experiment code; independent review of accounting, routing and statistics.
- [x] Delegate code-writing to a subagent using your configured feature model with a specific scope and success criteria.
- [x] Verify on the matching surface. Full native command/router suites, 77 Python tests, ten full-trace runs, five exact JSQ controls, all per-request placements, and independent statistics audit passed.
- [x] Rebase into small, ordered commits. Stack follow-ups. skip: no commit or PR requested; preserve existing uncommitted work.
- [x] If the design is contested, `interrogate` before shipping. skip unless competing designs leave a correctness question unresolved.
- [x] Run Opening a PR. skip: user requested local experiments and results.

## Experiment contract

Compare a clairvoyant policy that classifies individual sessions into the shortest 40% and longest 60%, reserving two of eight servers against long sessions. Use the unchanged mega trace, eight B200 GPT-OSS-120B replicas, 4-second Poisson root gaps, prefix caching off, FCFS, five paired arrival seeds 42–46 and simulator seed 42. The control is JSQ counting waiting plus running requests. Preserve recorded tool delays and dependencies and drain every family. Report mean, median and p99 session slowdown for all, short and long sessions; retain overall family slowdown for continuity. Save per-object data and provenance for plotting.

User clarified elapsed time, short sessions may use any server, and the breakdown is by session. Use the minimum elapsed session duration across the 35 saved cache-off isolated-family runs, including tool and dependency waits, and label this reference accurately. The partner's numerical results were not supplied.

## Existing evidence and boundaries

- `research/outputs/mega-policy-study-final` is the previous verified study. Do not mutate it or run Git commands inside its frozen dependencies.
- Existing reports and semantics are in `research/writeups/mega-policy-study` and `docs/guide/research-router.md`.
- Routing hooks are `sim/researchrouter/gateway.go`, `cmd/research_router.go` and `research/semiclosed/policies.py`.
- `sim/workload/semiclosed/replay.go` supplies distinct ProgramID (family) and SessionID.
- `research/policy_study/run.py` freezes and verifies experiments; `analysis.py` provides the existing family reference/statistics semantics.
- Existing working-tree changes belong to prior authorized work and must be preserved.

## Preparation verified

- `.audit/session-reservation-baseline-audit.json` records artifact-hash and completion checks for five 4s research JSQ controls and all 35 cache-off isolated runs.
- All 35 isolated-family runs contain the same 2,385 sessions. This is not session-alone isolation because siblings can overlap.
- Exactly 954 of 2,385 sessions form a 40% short class. A 40% family split needs an explicit rounding convention because 287 × 0.4 = 114.8.
- Existing five-seed JSQ family slowdown summaries are mean 1.8454446544, median 1.6621907337 and p99 3.9015648029, each the median of per-run statistics.
- Runtime session keys are `trace_id + "/" + session_id`, with both raw fields retained in workload reports. Reserved endpoint identities must remain fixed through routability changes.
- Implementation is delegated to a fresh owner in /scratch/akira/session-reservation-implementation, with the existing dirty tree overlaid without commits. Root retains the baseline/reference audit and final experiment execution.
