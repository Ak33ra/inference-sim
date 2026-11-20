# Candidate Astra

## Usage, written before the types

The caller selects `session-split` through the existing research-router switch. Its strict configuration adds only `remaining_manifest`; the study fixes replica 0 as the largest-first server. Eight replicas, cache off, native FCFS, and the existing token and sequence limits remain deployment settings.

```sh
blis replay ... --research-router session-split \
  --research-router-config split.json \
  --research-router-decisions split-audit.json
python -m research.session_split.run init STUDY --old-study OLD --binary BLIS
python -m research.session_split.run run STUDY --jobs 2
python -m research.session_split.analysis STUDY
```

Three internal call sites establish the contract.

```go
// cmd/research_router.go. The existing JSQ constructor branch stays unchanged.
policy, err := sessionsplit.Load(cfg.RemainingManifest) // validates saved schema
if err != nil { return nil, nil, err }
gw := cluster.NewCentralQueue(policy, cfg.LatencyUs)
return gw, gw, nil // second result becomes the narrow WriteAudit interface

// cluster.CentralQueue.Start. This package owns the cluster integration.
for _, inst := range cs.instances {
    if err := inst.sim.SetAdmissionSource(q.sourceFor(inst.ID())); err != nil {
        return err
    }
}

// Native Phase 2, after continuing work consumes this step's budget.
next := ctx.NewRequests.Peek()
if next == nil { break }
// Existing chunk sizing, sequence limit and allocateBatchKV execute here.
if !allocateBatchKV(ctx, next, start, end, cached) { break }
ctx.NewRequests.Admit(next, ctx.Now) // sole ownership transfer after allocation
```

`SetAdmissionSource` also enables the cache-off lifecycle KV guarantee. Neither a caller nor the policy coordinates queue credits, preparation timers, registration, or wakeups.

## Problem and evidence

The queue must select at the engine's real new-request admission boundary. `sim/batch_formation.go` first schedules continuing requests and only later decides whether a new request fits. The current gateway dispatches before `ArrivalEvent` charges admission overhead and before `EnqueueRequest` validates and accounts it. A dispatch credit based on `MaxBatchSize` or `BatchSize` cannot reproduce this boundary.

`Simulator.processCompletions` mutates state during a step but reports its future end timestamp. `Gateway.OnDone` receives that callback immediately. `Step` and `ScheduleStepIfIdle` currently see only local work. These facts require an explicit queue lifecycle, future publication of completion, and source-aware wakeups.

KV is more than token blocks. `sim/kv/cache.go` holds both `RequestMap` and `StateBlocks`; `KVSequenceState.Hold` comes from the kernel's bounded-state model. GPT-OSS therefore needs a lifecycle promise covering both. Disabling native eviction alone can strand an admitted request.

## Selected shape

Use a cluster-owned central gateway with a small optional native admission source. The gateway owns every request before actual batch admission. Each engine owns it after admission. The session ordering policy owns only an immutable manifest and comparison keys. Semi-closed replay continues to own readiness and dependency release.

The public surface is deliberately small.

```go
// sim/admission.go
// Only native batch formation calls Admit, once, after successful allocation.
type AdmissionSource interface {
    Peek() *Request
    Admit(req *Request, at int64)
    HasReady() bool
}
func (s *Simulator) SetAdmissionSource(src AdmissionSource) error
func (s *Simulator) NotifyAdmissionReady(at int64)

// One optional field on BatchContext; nil takes the current WaitQ branch.
// NewRequests AdmissionSource

// cluster/central_queue.go
// Keys are immutable for a request. LargerFirst selects comparator direction.
type CentralOrder interface {
    Key(req *sim.Request) (int64, error)
    LargerFirst(id InstanceID) bool
}
func NewCentralQueue(order CentralOrder, gatewayLatencyUs int64) *CentralQueue
func (q *CentralQueue) WriteAudit(path string) error
// CentralQueue implements existing Gateway; no new GatewayHost methods.

// sim/sessionsplit/manifest.go
func Load(path string) (*Policy, error)
func (p *Policy) Key(req *sim.Request) (int64, error)
func (p *Policy) LargerFirst(id cluster.InstanceID) bool
```

`CentralQueue.Start` requires the actual cluster host, checks all native instances, and installs their sources. This explicit integration is preferable to expanding `GatewayHost` for every unrelated gateway. `newReplayGateway` returns `interface{ WriteAudit(string) error }` as its optional reporter, so existing callers remain one constructor call plus one audit call.

Each pending entry has `req`, immutable `key`, original replay-ready time, preparation-finished time, a monotonic tie sequence, and a lifecycle state. Use an indexed min heap and max heap containing the same entry pointers; one owner updates both. Entries leave both heaps atomically on admission. Before admission, all state and timers belong to the central gateway. No engine has a copy in `WaitQ`. The cluster event loop serializes this owner without locks.

Use states `preparing`, `ready`, `active`, `terminalPending`, `terminal`. The session map is an assertion index, not another priority queue. It records the current request and terminal-publication time. Blocked future turns never enter the ready heaps and never receive priorities.

Tie order is ascending original ready time, publication sequence, then request ID in both heaps. Larger-first reverses only the remaining-size component. Every audit decision records the comparator direction and the full winning tuple. Engine events retain existing deterministic instance ordering at equal timestamps.

`Key` returns the saved `remaining_us` for that request. This value never decreases while waiting or running. A subsequent turn uses its own saved remaining value. The oracle joins explicit `trace_id`, `session_id`, and `step` into runtime identities at manifest construction. Runtime lookup checks `SessionID` and `RoundIndex`; it never parses escaped request IDs. Missing keys abort the run.

## Preparation, ownership, and accounting

Preparation starts after the existing gateway delay at `ArrivalTime + latency_us`. Homogeneous engines with no renderer workers share a pure admission-overhead function, so the queue can charge `QueueingTime(req)` once without choosing an engine. Publish eligibility at `ArrivalTime + latency_us + QueueingTime(req)`. Record all three times. Never rewrite `Request.ArrivalTime`.

Extract the existing request normalization and servability checks from `EnqueueRequest` into a package-private preparation helper shared by ordinary enqueue and central preparation. Preserve the ordinary call order and results. The helper returns a typed accepted/refused result and the normalized client output budget; it does not write metrics or enqueue. The central queue invokes it at preparation completion using the validated homogeneous engine settings. A refused request terminates through cluster rejection accounting. The study requires zero refusals and asserts exact input totals.

Successful central preparation records accepted input in a gateway-owned pending ledger. When Phase 2 allocation succeeds, its source's `Admit` removes the entry from the heaps, creates that engine's `Metrics.Requests` row with the original arrival, transfers its input-token accounting exactly once, sets SLO priority, installs its deadline event, sets `GatewayDispatchTime` and `AssignedInstance`, records routing, increments in-flight state and notifies `OnDispatched`. It does not schedule `ArrivalEvent`, `QueuedEvent`, or enqueue locally. The preparation receipt is consumed exactly once.

Keep the registration helper private in `sim/admission.go`. `SetAdmissionSource` wraps the supplied source so its engine-facing `Admit` performs registration before forwarding the committed claim to the cluster source; that source supplies routing/in-flight work. Native `scheduleBatch` can then record scheduling delay, TTFT, and completions exactly as today. Central wait is included because every metric still uses the original ready timestamp.

Pending deadline timers remove a central entry and reject it once; active deadline ownership transfers to the engine. Cancel the pending timer at admission. The queue's counters and pending input ledger participate in aggregate unfinished/rejected accounting, including horizon truncation. The target manifest has no deadline failures, but tests must cover deadline expiry during preparation and during ready waiting so a request cannot disappear or count twice. Where existing gateway and engine classifications differ, report the gateway category while centrally owned. Do not pretend that a never-dispatched request was already accepted by an endpoint.

## Admission and wakeup contract

At the existing Phase 2 boundary, select either the unchanged local queue path or the optional source. Continuing work retains priority. The source offers the extreme ready entry for this engine's direction. Existing sequence limits, token-budget chunking, native KV checks, and kernel batch pricing remain authoritative.

On allocation failure, leave the entry centrally ready and stop this engine's new admissions for this step, matching native head-of-line admission. It may fit on a different engine. Do not skip a larger remaining request merely to pack a smaller one onto the long server. The ordering rule must be visible in the audit. On success, `Admit` must not fail; every fallible validation happened during preparation. The event loop cannot interleave another engine between allocation and admission.

`NotifyAdmissionReady(at)` calls the existing `scheduleStepBy`, retaining `busyUntil` clamping and deduplication. A prepared publication notifies all idle-capable engines. `Step`'s early return must include source readiness. When a step finishes all of its running work, source readiness schedules the next step at the existing step end. An empty source schedules no retry. A stale wake from another engine consuming the last ready entry exits with no simulated work. Permanent solo-capacity failures are rejected before publication, preventing repeated empty retries.

The source contains no free-slot estimate. It cannot admit by reading `BatchSize`, `Request.State`, `UsedBlocks`, or a future completion mutation.

## Completion causality

`OnDone(req, at, outcome)` records a terminal-pending transition and uses `host.Schedule(at, PriorityGatewayRequest, false, ...)` to publish terminal state. Do not release a session, update policy ownership, or trigger any early wake at callback invocation time. Native engine resources can be released internally during the step because every next step is clamped by `busyUntil`.

`OnArrival` also schedules publication at priority 2, after its raw priority-0 replay arrival. By then a predecessor's already-scheduled terminal event at the same time runs first. Defensively validate a successor against the recorded `terminalAt <= readyAt`, not the mutable engine `State`. This permits legitimate same-time turns while still rejecting intra-session overlap. Priority-2 publications all precede priority-3 batch formation. Replay itself remains responsible for enforcing every cross-session dependency and tool delay.

## Nonpreemptive requests, including KV

Add an opt-in lifecycle promise to single-tier `kv.KVCacheState`, enabled only by `SetAdmissionSource`. Ordinary allocation remains byte-identical with the option off. The first allocation checks a promise against the sum of existing promises before mutating request state. A failed attempt changes neither the promise ledger nor physical KV. Successful allocation commits the promise; `ReleaseKVBlocks` removes it on completion or cancellation. Growth allocation consumes the same promise.

The maximum token count is normalized `InputLen + MaxOutputLen`, capped by the accepted model window. It uses the ordinary client budget, not a second oracle. Replay already sets this budget from its supplied workload. The promise includes `ceil(maxTokens / blockSize)` token blocks plus a bounded-state upper envelope from the configured `KVSequenceState` model, and any native admission-gate requirement. For the supported non-speculative kernel state, `max(Hold(0,maxTokens), Gate(maxTokens))` is a conservative envelope because keeping the whole sequence in flight retains every bounded-state page that any chunk can hold. Verify that envelope against the actual kernel state over the study's lengths and chunk boundaries; if that proof fails, implement the model's explicit peak query before enabling this mode. Do not silently fall back to token blocks alone.

Promises are logical capacity claims, not precomputed tokens or early physical allocations. Native `UsedBlocks` and priced batches remain physical. Record promised blocks separately. Since total promises never exceed capacity, every admitted request can finish even if no other one finishes first. Add a source-mode invariant guard before native memory preemption and on final-token allocation failure. Abort with a diagnostic if either occurs. This guard detects a broken promise calculation; it is not the capacity policy.

We accept conservative reservations reducing admission concurrency to provide the user-requested nonpreemption. The study must expose that tradeoff through actual batch-size distributions, memory usage, promise occupancy, and reservation-refusal counts. Native sequence and token capacities remain available; no max-sequence-one shortcut is permitted.

## Boundary validation

CLI/config validation rejects cache reuse, offload, speculative decoding, renderer workers, adapters, P/D, DP endpoint balancing, lifecycle changes/autoscaling, heterogeneous engines, and a non-FCFS/research local scheduler. Central mode requires single-tier KV with the promise capability and exactly eight active replicas for this experiment. Validate the same restrictions in the gateway start path for programmatic callers.

The manifest validates full request/session/family coverage, strictly sequential turns with explicit predecessor-completion edges, positive remaining values, first-turn agreement with the frozen reference, and nonincreasing remaining values. Save hashes of all 33 selected reference runs and the original 35-run reference set. Seriality is a manifest contract because the generic replay loader permits overlap.

## Module map and study

| Module | Responsibility |
| --- | --- |
| `sim/admission.go` | Optional source, shared request preparation, consumed preparation receipts, engine registration and source wakeup. |
| `sim/batch_formation.go`, `sim/simulator.go` | Small nil-gated source seam and lifecycle guards at existing authoritative boundaries. |
| `sim/kv/cache.go` | Opt-in lifecycle promise ledger and its native allocation/release integration. |
| `sim/cluster/central_queue.go` | Central ownership, preparation events, deadlines, heap ordering, dispatch accounting, causal terminal publication, audit. |
| `sim/sessionsplit/manifest.go` | Immutable saved reference lookup and replica direction. |
| `cmd/research_router.go`, `cmd/replay.go` | Strict opt-in policy configuration and audit reporter interface. |
| `research/session_split/` | Manifest construction, existing-plan adaptation, verification, paired analysis. |

Do not edit frozen `research/semiclosed` or `research/policy_study` helpers. Follow `session_reservation.run.adapted_plan` to prepare existing JSQ configurations and replace only the candidate policy/configuration in a copied plan. Write new outputs under `research/outputs/mega-session-split-4s`. Read the existing reference manifest hash from reservation study metadata without revalidating unrelated reservation helper hashes. Freeze one binary and source overlay for ten runs, paired seeds 42 through 46. Four-second mean Poisson gaps apply to family roots; simulation seed stays 42. Preserve all raw outputs and statuses for restartable execution.

New controls must exactly match frozen JSQ `workload.json`, `router-decisions.json`, `verification.json`, and `metrics.json`, ignoring only `catalog.path`. Verify 287 families, 2,385 sessions, 58,741 requests, 722,128,759 input tokens, 10,121,700 generated output tokens, and 60,551 dependency edges. Verify identical root schedules and logical work across each pair.

Save per-request ready/prepared/dispatch/completion times, selected key and reference run, central queue depth, batch size/token budget, KV physical/promise occupancy, and all admission/refusal/terminal events. A replay verifier reconstructs the two heaps and checks every winner at its exact admission event. It also checks each request has one admission, central delay appears in native latency, same-time causality, zero preemptions and allocation failures, and observed batches above size one when feasible.

Produce per-session and per-family makespan and slowdown using the frozen reference denominators, with paired per-object ratios and per-seed mean/p50/p99 changes. Summarize median and min/max across five seeds. Keep below-one slowdown values and label references as best observed isolated-family runs with sibling competition and recorded tool delays. No additional policy sweep is part of this task.

## Alternatives considered and synthesis decision

A dispatch-credit gateway keeps the engine unchanged and pushes requests when estimated sequence slots free. It loses because it cannot observe token/KV admission, assigns an endpoint backlog before true capacity exists, and sees future completion state early. Correcting it would expose nearly all batch internals through a shallow host interface.

A cluster batch coordinator could construct all eight batches and then call engines to price and execute them. It centralizes decisions but duplicates or relocates native scheduling, KV and completion behavior. Its integration surface is much larger and would put default parity at risk.

Select the optional native admission source. It hides preparation and cluster accounting behind the existing gateway constructor, retains native capacity/pricing ownership, and adds only one nil-gated branch in the admission loop. This candidate uses no other runner's design; the parent owns cross-candidate synthesis.

## Reconciliation and next step

No implementation has been written. First implement the source seam and a small real-cluster fixture proving central delay, multiple admissions, no duplicate arrivals, and a future completion followed by a same-time successor. Then implement and prove lifecycle promises before running the study. Any need to consume a request before successful native allocation invalidates this sketch and must return to design review.

The architecture checklist is Ground complete, Sketch complete, Agree deferred to parent synthesis, Implement pending accepted sketch, and Scrap not triggered. Source files were inspected read-only. Remaining risk is the bounded-state envelope proof; this is a code/kernel validation task, not a request for user approval.
