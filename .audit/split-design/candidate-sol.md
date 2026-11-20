# Central session SPLIT design candidate

## Usage from the caller

The study adds one opt-in research policy. The existing replay, JSQ, and frozen study helpers keep their current paths.

```json
{
	"serving": {
		"router": {
			"research_policy": "session-split",
			"parameters": {
				"remaining_manifest": "research/outputs/mega-session-split-4s/provenance/session-remaining.json"
			}
		}
	}
}
```

`research/session_split/run.py` prepares and runs ten cases. Each arrival seed from 42 through 46 has a JSQ control and a `session-split` treatment. Both use simulation seed 42 and the same prepared semi-closed plan.

```bash
python -B -m research.session_split.run prepare \
	--old-study research/outputs/mega-policy-study-final \
	--output research/outputs/mega-session-split-4s
python -B -m research.session_split.run run research/outputs/mega-session-split-4s
python -B -m research.session_split.analysis research/outputs/mega-session-split-4s
```

The replay command remains small.

```bash
blis replay ... \
	--research-router session-split \
	--research-router-config .../router.json \
	--research-router-decisions .../router-decisions.json
```

The engine integration is also opt-in. The cluster installs a pull source only when the gateway implements `cluster.CentralAdmissionGateway`.

```go
gw, _ := researchrouter.New(researchrouter.Config{
	Policy:            researchrouter.PolicySessionSplit,
	RemainingManifest: manifestPath,
})
cs.SetGateway(gw)
err := cs.Run()
```

An ordinary gateway does not implement the optional interface. Its route remains `GatewayHost.Dispatch -> ArrivalEvent -> QueuedEvent -> WaitQ`, with the current batch formation and KV behavior.

## Problem

SPLIT needs one central queue of ready session turns. At every real new-request admission slot, instance 0 chooses the largest `remaining_us`; instances 1 through 7 choose the smallest. A choice must remain central if the engine cannot allocate it. Existing dispatch assigns a request before the engine knows token, sequence, and KV capacity, while `VLLMBatchFormation` owns those decisions. Completion callbacks also expose a future step-end time before the cluster clock reaches it. The design must retain admission overhead, registration, metrics, native batching, DAG release, and strict request nonpreemption without changing default paths.

## Two whole designs

### Design A, gateway push with per-instance credits

The gateway tracks `MaxBatchSize - inFlight`, assigns ready turns to local instance queues, and refreshes credits on completion. This is a conventional central dispatcher and requires few engine edits.

It loses on correctness. A credit cannot represent the current step token budget or KV allocation. Preparation and arrivals can make a credit stale. Once pushed, a request has acquired an instance and cannot migrate even if Phase 2 rejects it. Repairing those gaps requires a claim, rollback, and credit reconciliation protocol across the gateway and every engine. The public protocol exposes more scheduling state than it hides.

### Design B, Phase 2 pull with lifecycle KV reservations

Each engine receives a narrow optional admission source. Phase 2 peeks at the central queue only when native sequence and token gates are open. The engine reserves the request's bounded lifetime KV and allocates its first native chunk. Only then does it commit the central choice. A failed reservation performs no commit and changes no scheduler ownership.

This design keeps batch sizing, prefix work, and latency pricing in their current owners. The new interface hides queue ordering, completion publication, routing bookkeeping, and wakeups behind `Peek`, `Commit`, and `HasReady`. This candidate selects Design B.

A third shape, a cluster scheduler that builds eight batches itself, was rejected. It would duplicate `VLLMBatchFormation` token, sequence, KV, prefix, and continuation rules in the control plane. Every kernel change would then need two implementations.

## Selected shape

### Core data

```go
// sim/admission_source.go
type AdmissionSource interface {
	// Peek is pure with respect to ownership. Repeated calls return the same
	// candidate until Commit succeeds or central readiness changes.
	Peek(now int64) *Request
	HasReady(now int64) bool
	// Commit is called only after the first KV allocation succeeds.
	Commit(req *Request, now int64)
}

// Optional KV capability. The cache-off GPU store implements it.
type LifecycleKVReservation interface {
	TryReserveLifecycle(req *Request, endToken int64) bool
	CancelLifecycleReservation(req *Request)
}

type BatchContext struct {
	// Existing fields stay unchanged.
	AdmissionSource AdmissionSource // nil preserves the current WaitQ path
	NoPreemption   bool            // true only with lifecycle reservations
}
```

`TryReserveLifecycle` removes enough free blocks for `[0, endToken)` and stores unused blocks in a per-request reservation map. Later `AllocateKVBlocks` consumes those blocks before the free list. `ReleaseKVBlocks` releases both used and unused reserved blocks. Reserved blocks count in `UsedBlocks`, publish no prefix hashes, and create no cache events until tokens actually fill a block. The initial mode requires prefix caching off, so cached-block aliasing is outside this contract.

The reservation bound is `req.InputLen() + int64(req.MaxOutputLen)`. Semi-closed replay already sets `MaxOutputLen = max(1, actual output length)`. Request validation rejects a bound above `MaxModelLen` or total KV capacity before the request enters the ready queue. A continuing request therefore cannot fail KV allocation. `NoPreemption` makes any such failure an invariant error instead of selecting a victim. This is the structural proof of request nonpreemption, including KV.

```go
// sim/researchrouter/session_split.go
type turnKey struct {
	RuntimeSessionID string
	Step             int
}

type remainingTurn struct {
	RequestID         string
	ReferenceRun      string
	ReferenceArrival  int64
	ReferenceComplete int64
	RemainingUs       int64
}

type sessionState struct {
	key       string
	readyReq  *sim.Request // nil while blocked or active
	readyAt   int64
	remaining int64       // fixed oracle value for this turn
	activeOn  cluster.InstanceID
}

type pendingTerminal struct {
	requestID string
	sessionID string
	at        int64
	outcome   cluster.DoneOutcome
}

type sessionSplit struct {
	turns     map[turnKey]remainingTurn
	sessions  map[string]*sessionState
	active    map[string]cluster.InstanceID
	terminal  map[string]pendingTerminal
	ready     map[string]*sessionState
	seq       uint64
}
```

The manifest builder joins explicit `trace_id`, `session_id`, and `step` fields from workload artifacts. It records the request ID as a cross-check. Runtime lookup uses the canonical `req.SessionID` and `req.RoundIndex`; it never parses a request ID. For each session, the builder first selects one isolated-family reference run. Every turn then uses that same run's session completion and request arrival:

```text
remaining_us(turn) = reference_session_completion_us - reference_request_arrival_us
```

The builder rejects missing turns, duplicate keys, first-size mismatches, increases across steps, and nonpositive remaining values. Queue wait under loaded replay never mutates `remaining_us`.

The ready set is keyed by session. The manifest and runtime both require at most one ready or active request per session. This encodes one job as one session. Instance 0 compares `(remaining_us descending, ready_at, session_id, request_id)`. Instances 1 through 7 compare `(remaining_us ascending, ready_at, session_id, request_id)`. A peek does not remove the winner. The instance-index event order gives instance 0 the first same-tick pull, then each short instance sees the remaining set.

### Optional cluster boundary

```go
// sim/cluster/central_admission.go
type CentralAdmissionGateway interface {
	Gateway
	Candidate(instance InstanceID, now int64) *sim.Request
	HasCandidate(instance InstanceID, now int64) bool
	CommitCandidate(req *sim.Request, instance InstanceID, now int64)
}

type CentralAdmissionHost interface {
	GatewayHost
	PrepareCentral(req *sim.Request) (readyAt int64, err error)
	WakeCentralAdmission(at int64)
}
```

`ClusterSimulator.Run` detects `CentralAdmissionGateway` after `Start`. It installs one instance-bound `sim.AdmissionSource` adapter per engine. The adapter delegates selection to the gateway. Its `Commit` method performs all assignment effects in one place:

1. Remove the request from the gateway ready set.
2. Register it once in the selected instance's metrics and input accounting.
3. Set `GatewayDispatchTime` and `AssignedInstance`.
4. Record routing and increment `inFlightRequests`.
5. Call `Gateway.OnDispatched` without injecting an `ArrivalEvent`.

This is the only central-mode assignment path. `GatewayHost.Dispatch` remains unchanged for every existing gateway.

`PrepareCentral` validates and normalizes the request with the same rules extracted from `Simulator.EnqueueRequest`. It prices `LatencyModel.QueueingTime` on the canonical homogeneous engine and returns the preparation completion time. It does not register metrics, allocate KV, or choose an instance. CLI validation requires eight homogeneous colocated engines, cache off, FCFS, no P/D, DP endpoint balancing, offload, speculative decoding, adapters, renderer workers, lifecycle changes, or gateway eviction. These restrictions make the canonical preparation price identical on every eligible instance.

`sessionSplit.OnArrival` sets `GatewayEnqueueTime`, calls `PrepareCentral`, and schedules readiness at `PriorityGatewayRequest`. A client deadline remains a gateway timer while the request is preparing or central-ready. After commit, the selected simulator owns its normal timeout event. The original `ArrivalTime` is never rewritten, so request scheduling delay includes preparation and central queue delay.

### Phase 2 admission

The existing WaitQ loop remains byte-for-byte on the nil branch. The external branch changes only candidate access and successful removal.

```go
for sequenceSlotOpen() && tokenBudgetOpen() && !preempted {
	next := waitQHead()
	if ctx.AdmissionSource != nil {
		next = ctx.AdmissionSource.Peek(ctx.Now)
	}
	if next == nil {
		break
	}

	end := next.InputLen() + int64(next.MaxOutputLen)
	if ctx.AdmissionSource != nil && !reserve.TryReserveLifecycle(next, end) {
		break // no ownership change; retry after a running request releases KV
	}

	// Existing prefix, chunk, token-budget, sequence, and first-step allocation logic.
	if !allocateFirstNativeChunk(next) {
		reserve.CancelLifecycleReservation(next)
		panic("lifecycle reservation admitted an unallocatable first chunk")
	}

	if ctx.AdmissionSource == nil {
		dequeueCurrentWaitQHead()
	} else {
		ctx.AdmissionSource.Commit(next, ctx.Now)
	}
	markNewlyScheduled(next)
}
```

Phase 1 remains native. It schedules continuing requests first. Phase 2 still applies `MaxNumSeqs`, `MaxNumBatchedTokens`, the long-prefill cap, and actual KV allocation. The latency kernel prices the resulting real batch. The policy chooses only among eligible new requests.

### Completion causality and DAG release

`processCompletions` releases KV and calls `OnRequestDone(req, stepEnd)` while the instance is still executing the step-start event. The gateway must not expose that release at the cluster's earlier clock.

`sessionSplit.OnDone(req, at, outcome)` records `pendingTerminal{at: at}` and schedules one non-daemon callback at `at` with `PriorityGatewayRequest`. The callback removes the active session, records the terminal audit row, and wakes admission sources. Every scheduler callback begins with `publishTerminalsThrough(host.Now())`, so a same-time arrival cannot observe stale active state even if callback sequence IDs change.

Semi-closed replay keeps ownership of dependencies and tool delays. Its terminal timer and successor arrivals remain cluster events. `OnArrival` only starts preparation. Selection runs in a priority-2 callback after same-time priority-0 arrivals and after `publishTerminalsThrough(now)`. No code changes `Replay.Gate`, `s.arrivalUs`, or `Request.ArrivalTime`.

### Idle wakeup

The simulator's work predicate becomes source-aware only when `AdmissionSource != nil`.

```go
func (sim *Simulator) hasNewAdmission(now int64) bool {
	return sim.admissionSource != nil && sim.admissionSource.HasReady(now)
}

func (sim *Simulator) NotifyAdmissionReady(at int64) {
	if sim.hasNewAdmission(at) {
		sim.scheduleStepBy(at) // existing busyUntil clamp and idle-wake supersession
	}
}
```

The empty-step guard, `scheduleNextStep`, and `HasWork` use `hasNewAdmission`. A readiness callback calls `WakeCentralAdmission` once and each eligible idle engine schedules at most one step. If a reservation fails while requests are running, the continuing batch already schedules the next step. If the batch empties, its lifecycle KV release makes any individually valid candidate fit. No empty-step polling is needed.

### Module map

- `sim/admission_source.go` owns the optional source interface and source-aware work checks.
- `sim/batch_formation.go` owns the Phase 2 peek, lifecycle reserve, and commit point. Its nil path stays unchanged.
- `sim/kv_store.go` and `sim/kv/cache.go` own lifecycle KV reservations and their release invariant.
- `sim/simulator.go` owns per-instance registration, metrics, timeout scheduling, and wake clamping.
- `sim/cluster/central_admission.go` binds an optional gateway to instances and owns routing and in-flight accounting.
- `sim/researchrouter/session_split.go` owns manifest lookup, session state, largest and smallest selection, terminal publication, and audit rows.
- `sim/researchrouter/gateway.go` delegates only the `session-split` policy to that state owner. Existing policies keep their push path.
- `cmd/research_router.go` adds the strict `remaining_manifest` allowlist and rejects unsupported feature combinations before simulation construction.
- `research/session_split/{run,common,analysis}.py` owns the new manifest, paired run matrix, verification, and report. It imports frozen outputs but does not edit `research/semiclosed`, `research/policy_study`, or old artifacts.

## Interface depth and ownership

The engine sees only whether a candidate exists, which request to try, and how to commit it. It does not know SPLIT ranking, oracle fields, sessions, or replica roles. The gateway never sees token budget or KV internals. The KV store owns capacity and the scheduler owns session state. This follows model-the-domain by representing each session as one state record, boundary-discipline by validating manifests and unsupported modes at load time, and type-system-discipline by using distinct turn keys and instance IDs.

The design keeps one assignment path for central mode and one unchanged assignment path for default mode. It avoids a pass-through scheduler layer and keeps the mutable central state in `sessionSplit`, following minimize-reader-load and the laziness protocol.

## Tradeoffs accepted

- We accept lower effective concurrency from full-lifecycle KV reservations in exchange for a proof that admitted requests never suffer memory preemption.
- We accept an initial homogeneous, cache-off research boundary in exchange for pricing preparation once without leaking an instance assignment.
- We accept one small Phase 2 source branch in exchange for preserving native batch capacity decisions and request migration before admission.
- We accept `O(number of ready sessions)` selection at first. The trace has 2,385 sessions, and a scan avoids two heaps that must support deletion and changing readiness. A measured need can replace the scan later.

## Verification contract

Focused Go tests must prove observable behavior.

- A failed lifecycle reservation leaves the same request central, records no route, increments no in-flight count, and permits a later instance to admit it.
- Instance 0 repeatedly chooses the largest remaining value, while instances 1 through 7 choose the smallest. Ties follow the documented stable key.
- Loaded queue time never changes a turn's remaining value. The next turn receives only its manifest value.
- Preparation overhead is charged once. Registration, input tokens, dispatch time, timeout ownership, routing attribution, and in-flight accounting occur once on successful commit.
- A completion reported with a future `at` does not free a session early. A successor arriving at that exact timestamp becomes selectable only after terminal publication.
- An idle engine wakes when central work becomes ready, honors `busyUntil`, and does not poll empty steps.
- A capacity-pressure run produces real batches larger than one, zero preemptions, zero evictions, no allocation failures, and zero leaked reserved blocks after drain.
- Existing simulator, gateway, research-router, and replay tests produce identical results with no central gateway. A dedicated parity test compares default metrics and event order before and after the source hook.

The Python verifier freezes the trace SHA256 and the new manifest. It checks all 2,385 sessions, 287 families, 58,741 requests, 722,128,759 prompt tokens, 10,121,700 output tokens, and 60,551 dependency edges. It also checks root schedules, strict session seriality, DAG readiness, central delay inclusion, exact selection order, routing-to-engine agreement, real batch sizes, zero priority or KV preemption, reservation release, and complete accounting.

For each seed, the new JSQ control must equal the frozen control in workload, decisions, verification, and metrics after only `catalog.path` normalization. The analysis saves raw run artifacts, per-session and per-family elapsed time and slowdown, per-run mean, p50, and p99 values, paired seed changes, and paired per-session ratios.

## Open risks

- Does `LatencyModel.QueueingTime` mutate any kernel implementation used by the target scenario? The implementation should prove that it is a pure admission-overhead query for this scenario before enabling canonical preparation. A mutable implementation must move that state into a dedicated central preparer.
- Does GPT-OSS-120B's configured KV store include bounded per-sequence state beyond full-attention blocks? `TryReserveLifecycle` must reserve every capacity group that can make a later allocation fail, or CLI validation must reject the model shape.

## First implementation unit

Build `LifecycleKVReservation` and the instance-bound `AdmissionSource` with a two-instance test that proves failed allocation does not commit ownership, then add the session policy and study runner against that boundary.
