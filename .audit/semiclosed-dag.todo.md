# Semi-closed explicit DAG input

- [x] `how` over the affected subsystem.
- [x] `architect` for parallel design exploration. Capacity fallback: sequential comparison, with independent review of the chosen design. Reconstructing fake FreeInference audits loses explicit edges; compile a separate DAG loader into the existing runtime instead.
- [x] Write the throughput checkpoint as four todo items.
  - Blocking first steps. Inspect recursive trace schema and runtime graph before editing.
  - Independent workstreams. Native graph/arrival implementation, then launcher and verification. Independent read-only review runs alongside.
  - Shared mutable state. One writer owns this checkout; preserve prior analysis changes.
  - Smallest safe decomposition. One owner implements the graph contract and its callers. Native adapter, arrival scheduler, research runner remain separate files.
- [x] Delegate code-writing to a subagent. Capacity fallback: existing slots exhausted, no close tool; primary owns implementation and a finished independent reviewer reviewed it.
- [x] Verify on the matching surface. Both full source runs, independent token/edge/timing checks, workload/cluster/CLI suites, and 19 Python tests passed.
- [x] Rebase into small, ordered commits. skip: working-tree delivery; unrelated prior work remains uncommitted.
- [x] If the design is contested, `interrogate` before shipping. skip: independent review accepted the corrected timing design.
- [x] Run **Opening a PR**. skip: local implementation requested; no remote publication.

Data shape: immutable corpus of families, sessions, requests, and explicit completion prerequisites. ArrivalConfig constructs a finite reproducible schedule. The event loop continues to own request lifecycle.

Checks: graph validation and fork/join release, seeded arrivals, exact token counts, existing AgentX and FreeInference tests, full natural Oolong CLI run.
