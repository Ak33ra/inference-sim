# SPLIT session scheduling experiment

- [x] `how` over the affected subsystem.
- [x] `architect` for parallel design exploration.
- [x] Write the throughput checkpoint as four todo items.
  - Blocking first steps. Read the paper, resolve remaining-size semantics, and prove the central queue can respect actual batch capacity before implementation.
  - Independent workstreams. Engine admission grounding and oracle/replay grounding proceed independently while root checks the paper and experiment contract.
  - Shared mutable state. Existing frozen studies stay read only. One implementation owner gets an isolated worktree with the current source overlay.
  - Smallest safe decomposition. Keep central scheduling, hooks, and its experiment runner with one owner; review correctness independently before the full experiment.
- [x] Delegate code-writing to a subagent using the configured feature model with a specific scope and success criteria.
- [x] Verify on the matching surface.
- [x] Rebase into small, ordered commits. Stack follow-ups. skip: no commit or PR requested; preserve existing uncommitted work.
- [x] If the design is contested, `interrogate` before shipping. skip: independent review found no remaining contested correctness question.
- [x] Run Opening a PR. skip: user requested local implementation and experiments.

## Architecture phases

- [x] Ground.
- [x] Sketch two distinct designs.
- [x] Agree on the smallest correct design.
- [x] Implement.
- [x] Scrap only if the implementation reveals a repeated ownership problem. skip: native admission ownership held in tests and real CLI smoke.

## Intended experiment

Adapt SPLIT (arXiv:2605.13749) to sessions and LLM request boundaries. One server chooses the largest remaining eligible session; seven choose the shortest remaining eligible sessions. Requests complete without priority preemption. Servers retain configured batching capacity. Ready requests wait in a central queue and may use different servers across turns. Dependencies and tool delays remain enforced by semi-closed replay.

Use the existing eight-B200 GPT-OSS-120B mega-trace configuration, cache off, FCFS within already admitted work, four-second mean Poisson family-root gaps, five paired arrival seeds 42–46 and simulation seed 42. Compare to default research JSQ/FCFS and retain full output for plotting. Use shared session/family references from the 35 previous cache-off isolated-family runs. This is an adaptation, not an implementation of the paper's preemptive job model or a transfer of its tail-optimality proof.

User accepted remaining oracle timeline. For each request use reference session completion minus reference request arrival from the same saved reference run as its session original size. Loaded queueing does not count as progress.

## Design arena

- [x] Frame.
- [x] Fan out. Astra and Sol running; Terra dispatch hit the host thread limit. No close-agent tool is exposed, so the panel is reduced to two designs.
- [x] Cross-judge. skip: no independent extra slot; root will compare the two candidates and retain Sol for implementation review.
- [x] Pick.
- [x] Graft.
- [x] Verify.

## Integration and validation

- [x] One writer handed off 21 reviewed files; preserved the preexisting root changes and frozen old helpers.
- [x] Independent core and comment review; both accepted follow-ups implemented.
- [x] Native CLI smoke: 1,104 requests, full tokens/dependencies, batching and zero preemption/KV leaks.
- [x] Affected Go and 71 Python checks; one randomized P/D failure reproduced exactly before SPLIT and retained as a preexisting limitation.
- [x] Full seed42 pair, exact old JSQ parity and central audit.
- [x] Remaining four seed pairs.
- [x] Independent numerical and preparation-time diagnostics.
- [x] Concise writeup and chat report with saved plotting data.
