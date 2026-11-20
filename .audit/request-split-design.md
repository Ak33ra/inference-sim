# Request SPLIT accepted design

## Grounding
The gateway owns requests until native first-chunk KV allocation. Native batching owns continuation, token caps, sequence caps and chunking. Lifecycle KV promises prevent eviction/preemption without eagerly computing or allocating full sequences. Preparation uses native renderers distributed round-robin across endpoints. GPU capacity releases at GPU completion; client terminal events publish later when output overhead finishes. Semi-closed dependencies advance on client completion.

## Usage
`--research-router request-split --research-router-config router.json` with `{"service_manifest":"/absolute/path/service.json"}`. A new `research.request_split.run` package freezes an oracle then executes five paired JSQ controls and request-SPLIT runs. Analysis saves session/family makespan and shared-reference slowdown, with request diagnostics.

## Candidates and synthesis
Astra proposed one shared central lifecycle core with separate typed manifests and audit projections. Sol initially proposed a standalone request gateway, then independently cross-judged the common-core proposal as safer against lifecycle drift. Both rejected online cloned-engine or approximate kernel formulas. Root selected the common core. Host capacity reduced the configured panel to Astra/Sol, with a separate Astra oracle investigator.

Rubric (0 to 2): semantics, old-policy preservation, interface depth, lifecycle maintainability, exact oracle proof. Shared core scores 2/1/2/2/2 before parity validation. Duplicate gateway scores 2/2/1/0/2. The preservation gap is a mandatory byte-identity parity check, not assumed away.

## Contract
One concrete core holds validated entries with `priorityUs`, `jobKey`, `predecessorID`, `refRun`, request and preparation metadata. Session entries use session identity and old predecessor. Request entries use request identity and no predecessor. One owner handles preparing, ready, active and completed, ready extrema, first-allocation claim, native wake and client-terminal publication. Avoid a general strategy/callback framework. Keep old constructor, strict session loader and audit schema. New request manifest binds request/session/step identity and exact input/output to positive isolated service. Shape table deduplicates pair costs. The new audit uses `service_us`; old audit keeps `remaining_us`. Exact eight-replica validation enforces the requested 7+1 role count. Ties are causal arrival then request ID.

Native batch/token/sequence settings remain authoritative and requests stay central when native allocation refuses. Instance zero takes the largest ready request and all others the smallest. Already running requests finish and never migrate or preempt. Priority is fixed isolated client service, including preparation even though preparation has finished by ready publication.

## Oracle
Generate one synthetic family of unique exact input/output shapes in a serial explicit DAG. Set times and delays to zero, empty hashes, one replica, zero gateway latency, same binary and engine, cache off. Use integer completion minus arrival. Root native proof in `.audit/request-oracle-probe/proof.json` verifies chain/reverse/clean-singleton parity over eight representative shapes and ten CLI runs. Full oracle must prove exact tokens, shape coverage, no overlap, no delay, no errors/caps/preemption/cache hits and final drain. Scheduling delay legitimately contains preparation service. Freeze raw oracle, manifest and all config/source dependencies.

## Ownership and proof
One Astra writer owns `/scratch/akira/request-split-implementation`. Root owns integration, full study and independent numerical checks. Independent Sol reviews core; independent Astra reviews comments/oracle. Frozen Python helper directories must remain byte identical. Test new priority/identity/error behavior and native batching, plus unchanged session-SPLIT tests. Full new JSQ outputs must equal prior controls after catalog.path normalization. Run one legacy session-SPLIT full replay and compare workload, metrics and event audit to frozen output after the same normalization. The prior randomized P/D saturation failure is separately reproduced and out of scope.

## Accepted implementation refinements
The writer keeps one concrete SplitEvent record with mutually exclusive optional service_us and remaining_us fields. Strict old manifests guarantee positive remaining_us and nonempty reference_run, so old valid event JSON is unchanged and the new service field is absent. This replaces separate projection wrappers with less code, conditional on the required full byte-parity check. Root accepted this after source review.

Request lookup now requires MaxOutputLen equal to max(1, exact output length). Root and independent Sol both identified the mismatch risk. This binds priority and lifecycle reservation budget. The CLI study verifies full trace, binary and engine fingerprints; event request IDs link through the frozen manifest to raw oracle rows. Reference_run is not repurposed to mean a request ID.
