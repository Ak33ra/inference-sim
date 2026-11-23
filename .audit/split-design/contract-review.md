# Session SPLIT contract review

The accepted ownership boundary is sound: the cluster owns prepared/ready/session state, while the native batcher owns token budgets, chunking, first KV allocation, and continuation. The lifecycle promise proof is sufficient for the installed non-speculative kernel if every engine request enters through the source. The following points need to be explicit in the implementation contract and pinned by focused tests.

## Required corrections

1. **Separate an expected promise refusal from a broken native allocation (critical).** A new request may return “cannot admit” only when `promised + candidatePromise > capacity`; it remains central and blocks that instance. Once that check passes, a false return from the physical allocator disproves the lifecycle bound and must abort. A single boolean path for both outcomes can silently leave the head request ready forever. Continuation and final-token allocation failures must likewise abort. The proof relies on `sum(promises) <= TotalBlocks`, each request's physical token/state occupancy staying within its promise, and no unpromised engine-local request.

2. **Make the native long-prefill decision source-aware (high).** `VLLMBatchFormation` currently decides whether to remove the prefill threshold from `len(RunningBatch)+WaitQ.Len()`. In source mode `WaitQ` is empty even when several central requests are ready. Treating that as a one-request engine lets the first long prefill consume the full token budget and changes native batching. Count source readiness conservatively or preserve the configured cap whenever a source is installed. The nil-source path must remain byte-identical.

3. **Stamp placement before constructing request metrics (high).** `NewRequestMetrics` snapshots `AssignedInstance`, `GatewayDispatchTime`, and gateway queue delay. Central commit must first set assignment/dispatch fields and perform the infallible source claim, then construct metrics and increment input accounting, all synchronously before `NewlyScheduled` processing. Otherwise `HandledBy` stays empty in raw metrics. If central queue delay is meant to be reported, stamp enqueue time at gateway arrival while handling time zero without relying on zero as both a valid timestamp and an “unset” sentinel.

4. **Require a fresh engine when installing the source (high).** `SetAdmissionSource` must reject a nonempty `WaitQ`, running batch, prior allocation/promise, or an already scheduled local admission. The batcher gives the source precedence over `WaitQ`, and lifecycle safety assumes all active allocations have promises. A fresh-engine precondition prevents silent starvation and unpromised occupancy even if the current cluster caller happens to install it before arrivals.

5. **Publish session completion at client completion, with exact same-time ordering (critical).** The engine invokes `Gateway.OnDone` before the semi-closed callback pushes the terminal arrival. `OnDone` must keep the session active and schedule a priority-`GatewayRequest` publication at `RequestCompletionTimes[id]`, including post-decode overhead. The terminal arrival at that timestamp has arrival priority and may release the successor; even with zero preparation, successor readiness must be another priority-`GatewayRequest` callback, never an inline ready-map insertion. FIFO sequence order then runs the already-scheduled completion publication before readiness. The test fixture must mirror `cmd/replay.go`'s completion-time wrapper and include nonzero output/post-decode overhead. Instance KV/capacity may release at GPU step end; only same-session ownership lasts until client completion.

6. **Preserve the latency and arrival clocks independently (high).** Preparation should be computed exactly once at original cluster arrival as `router latency + QueueingTime(req)` using one validated homogeneous engine. Readiness is scheduled for that future time, while `req.ArrivalTime` remains unchanged. Scheduling delay, TTFT, E2E, oracle ties, and central wait all continue to use the original arrival. Do not inject a second `ArrivalEvent` at claim.

## KV boundary

For `N = InputLen + normalized MaxOutputLen`, the accepted promise

`ceil(N / blockSize) + max(LifecyclePeak(N), Gate(N))`

is conservative for the installed kernel because `LifecyclePeak(N) = Hold(0, N)` bounds every non-speculative `Hold(computed, inFlight)` with `computed + inFlight <= N`. Enabling lifecycle mode must reject arbitrary `Hold` callbacks without an established peak. Promise commit occurs only after the first physical allocation succeeds, but the logical capacity check must happen first; the DES call is synchronous, so no intermediate reservation is needed. Release the promise exactly where native KV is released and diagnose missing or duplicate releases rather than allowing counter underflow.

## Minimum proof tests

- Two sessions whose predecessor completion, terminal arrival, completion publication, and zero-delay successor readiness share one timestamp; the successor cannot claim before completion publication.
- A nonzero post-decode completion where another session may use the freed instance at GPU step end but the same session remains unavailable until client completion.
- Promise-capacity refusal leaves central ownership, metrics, routing, input count, and physical/promise totals unchanged; an injected physical failure after logical fit aborts.
- Multiple central-ready requests retain the configured long-prefill cap and native multi-request batching.
- Source installation on queued, running, or allocated state fails; nil-source baseline output stays unchanged.
- Raw completed-request metrics contain the assigned instance, original arrival, scheduling delay, and exactly-once input/output accounting.

No change to the accepted whole design is needed beyond these boundary clarifications.
