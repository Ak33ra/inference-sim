# Session SPLIT core review

Reviewed the implementation overlay in `/scratch/akira/session-split-implementation` against `accepted.md`, `kernel-proof.md`, the root baseline, and the study verifier. This was a read-only review while the implementation owner was active; I did not run the Go or Python test suites.

## Result

No blocking core correctness defect remains in the inspected snapshot.

The implementation keeps one central ready owner, lets each native FCFS batcher pull its current extreme, and commits ownership only after the first physical KV allocation succeeds. A refused lifecycle promise leaves the request central. The nil source path retains the original WaitQ behavior. Source-aware long-prefill counting, idle wakeup, early-return handling, and continuation/final-allocation guards cover the engine integration points.

Lifecycle promises cover the normalized input plus client output budget and the kernel-derived bounded sequence-state peak. Physical allocation and `UsedBlocks` remain native. Promise state commits after physical success and releases with native KV cleanup. The final audit now exposes physical blocks, logical promises, capacity, admission refusals, preemptions, and allocation failures per instance; the independent verifier requires both final ledgers to be zero, peak promises within capacity, and no preemption/allocation failure. Empty repeated release remains harmless and cannot decrement the ledger twice.

Placement and metric registration occur exactly once at native scheduling time. Assignment and gateway dispatch are stamped before `NewRequestMetrics`, and the explicit queue-delay assignment handles time-zero arrivals. Preparation preserves the original arrival, charges kernel service once, and does not inject a second engine arrival. `SetAdmissionSource` requires a fresh engine and fresh renderer.

Client completion remains the session-ownership boundary. `OnDone` schedules terminal publication at the reported client completion timestamp. A same-time successor arrival can prepare first, but its priority-2 ready publication follows the already-scheduled predecessor completion callback, so it cannot overlap the prior turn. Other sessions may use GPU capacity after the earlier engine step completes. The queue audit accounts for every prepare, ready, claim, and completion and reconstructs the exact largest/smallest choice.

The corrected frontend path reuses each replica's native `Renderer`. In arrival order it assigns preparation endpoints round-robin, reaches the renderer after router latency, and applies the unchanged kernel preparation service. GPU placement remains a later central claim. The audit records endpoint, service, wait, and final renderer counts; the verifier independently replays the renderer FIFO and checks native aggregate counters. This intentionally differs from JSQ's routing-coupled renderer placement and is disclosed in the generated report.

The manifest loader now requires all turns of a runtime session to share `reference_session_completion_us`, in addition to reference run/elapsed, predecessor, monotone step, and remaining checks. The frozen runner owns the trace-digest binding: it builds the oracle from the selected saved run, freezes source hashes, checks exact workload totals and identities, and checks paired logical work/root arrivals. Expanding the gateway API solely to pass a trace digest is unnecessary.

## Non-blocking follow-up

- Add one compact loader test that rejects two turns with different `reference_session_completion_us`; the production check exists but currently lacks a direct boundary test.
- Keep preparation-wait evidence visible in the final analysis artifacts/report. It is already present in each run's verification record and native renderer audit.

The study verifier's `metric.get("gateway_queue_delay_ms", 0)` is correct: that field is serialized with `omitempty`, so an absent key is the canonical representation of an exact zero. The audit event independently establishes the zero interval.
