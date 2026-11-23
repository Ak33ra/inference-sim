# Request SPLIT implementation handoff

Implementation is complete in `/scratch/akira/request-split-implementation`. No commit or full study was run. Writer stops edits after this handoff. Integrate only the 15 files and hashes in `.audit/request-split-changed-files.json`; the preexisting dirty overlay is accounted for by `.audit/request-split-before.json`.

The implementation shares one central lifecycle core between typed session and request gateways. Request mode requires eight replicas and validates runtime ID/session/step, input/output shape and exact `MaxOutputLen=max(1,actual output)`. Native admission, renderer preparation, batching, lifecycle KV promises and client completion ownership are shared. The frozen session manifest parser and all old Python helpers are byte-identical. One existing session test fixture constructor changed mechanically; all its assertions remain.

Accepted refinement: one concrete `SplitEvent` uses mutually exclusive `remaining_us` and `service_us` fields with `omitempty`, rather than separate projection wrappers. Strict old session manifests guarantee positive remaining size and nonempty reference run, preserving old valid JSON field order and presence. Root owns the required full legacy parity gate.

New `research/request_split` freezes a native single-replica client-serial oracle for unique input/output shapes, then ten paired runs. Nine proof shapes cover chunk boundaries, longest prompt and longest actual output `(48521,16384)`. All source/raw calibration artifacts, binary and engine hashes are frozen. Initialization checks reference/control trace equality before creating output or calibrating. `calibrate()` sets `GIT_OPTIONAL_LOCKS=0` before execution. Direct CLI manifest use validates runtime identity and shape; the study runner enforces binary, trace and engine provenance. The README states that boundary.

The analysis saves session/family elapsed and slowdown tables, paired rows and changes, request diagnostics, verification, summary JSON and generated report. Request diagnostics include preparation service, additional admission wait, post-admission and post-first-token spans, and central-only queue/batch observations. Frozen helper directories are untouched and verified against initial hashes.

## Verification

All commands ran from the exclusive worktree after `source /home/akira/inference-sim/.inference-sim-env/activate`.

```bash
go test ./sim/cluster ./cmd -run 'TestRequestSplit|TestSessionSplit|TestResearchRouter' -count=1
BLIS_CATALOG=/scratch/akira/session-reservation-test-catalog go test ./sim ./sim/cluster ./sim/kv ./sim/kernelmodel ./sim/researchrouter ./cmd -skip '^TestPDDecodeAdmission_LawsHoldUnderRandomSaturation$' -count=1
uv run --with-requirements research/analysis/requirements.txt python -B -m unittest discover -s research/scripts/tests
git diff --check
```

All passed. Python: 75 tests. Broad Go: sim, cluster, kv, kernelmodel, researchrouter and cmd; cmd finished in 189.116s. Only the explicitly named preexisting P/D randomized property test was skipped. Root previously reproduced the same failure before SPLIT; no unrelated fix or generated failure fixture was added. Logs:

- `.audit/request-split-focused-go.log`
- `.audit/request-split-focused-python.log`
- `.audit/request-split-broad-go.log`
- `.audit/request-split-broad-python.log`

Actual native CLI smoke:

```bash
go build -o .audit/request-split-blis .
python3 -B .audit/request-split-smoke.py
python3 -B .audit/request-split-oracle-parity.py
```

The second script extends the first's native oracle with reverse and nine clean singleton runs. All nine service values match exactly. Longest-output isolated client service: 42,847,719us. Proof: `.audit/request-split-smoke/oracle-parity.json`. The loaded smoke executes 32 requests in 16 two-turn families on eight actual model replicas. All 32 claims independently verified, maximum batch at admission 5, total preparation waiting 430,138us, zero preemptions/allocation failures/admission refusals, final used/promised KV zero on all replicas. Artifacts: `.audit/request-split-smoke/{oracle,oracle-reverse,oracle-single-*,loaded,service.json,proof.json,requests.csv}`; logs `.audit/request-split-smoke.log` and `.audit/request-split-oracle-parity.log`. Initial eight-shape smoke is retained separately under `.audit/request-split-smoke-initial`.

The first smoke preceded the GIT_OPTIONAL_LOCKS fix and may have refreshed the frozen catalog index. Root identified the metadata-only difference and is restoring the exact original index plus validating dependency hashes before initialization. No calibration data/content changed; future helper execution disables optional Git writes.

## Root integration and full evaluation

After integrating the 15 files, perform the planned legacy full parity using root's `.audit/request-split-legacy-parity.py`, then freeze and run:

```bash
source .inference-sim-env/activate
go build -o /tmp/request-split-blis .
python3 -B -m research.request_split.run init research/outputs/mega-request-split-4s \
  --old-study research/outputs/mega-policy-study-final \
  --reference-study research/outputs/mega-session-reservation-4s \
  --binary /tmp/request-split-blis
python3 -B -m research.request_split.run run research/outputs/mega-request-split-4s --jobs 2
python3 -B -m research.request_split.analysis research/outputs/mega-request-split-4s \
  --report research/writeups/mega-request-split-4s/README.md
```

Root owns the full oracle, ten paired replays, legacy byte parity, independent numerical audit and final concise writeup. Full-study initialization and analysis have not been exercised end-to-end; their new calibration and replay components were exercised by the native smoke, and aggregation/audit logic by Python tests. No improvement claim has been made.
