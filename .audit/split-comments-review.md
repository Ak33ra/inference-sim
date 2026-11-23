# SPLIT comment and maintainability review

Reviewed only the SPLIT additions in `/scratch/akira/session-split-implementation`, against `/home/akira/inference-sim` and `.audit/split-root-baseline.json`. Existing root files in scope still matched the baseline snapshot. Read the comment-sicko instructions in full. No source edits, tests, or simulations were performed.

No blocking comment or maintainability finding. Two low-priority cleanup requests remain.

- `sim/cluster/session_split.go:62`. Delete `// ValidateSessionSplitDeployment is also called at the command boundary.` It narrates a caller location rather than defining the public API contract. `ValidateSessionSplitDeployment` and its explicit rejection branches already identify the operation. Suggested comment deletion count is one line. No replacement comment is needed.
- `sim/cluster/session_split.go`, `splitSource.Admit`. Delete the final `q.OnDispatched(req, at)` call. The receiver is concretely `*SessionSplitGateway`, whose `OnDispatched` method has an empty body. The call adds no behavior after recording routing and incrementing the in-flight count. Keep the empty method itself because it implements the gateway interface. No abstraction or replacement callback is needed.

`MUST KILL` flags for hidden internal constraints are absent. The one redundant call above can be deleted directly. `HOLD` findings are absent.

Kept the `AdmissionSource`, `LifecycleKVStore`, `PrepareCentralAdmission`, `SessionSplitGateway`, `EnableLifecycleReservations`, and `LifecyclePeak` comments because they define public ownership, capacity, or initialization contracts. The research module docstrings describe the package or command, and the command modules use them as CLI descriptions.

Kept the `LifecycleBlocks` bounded-state explanation after inspecting the actual dependency. `sim/kernelmodel/settings.go:191` supplies the explicit lifecycle peak using `SequenceBoundedBytesAt(0, tokens)`. The dependency's `/home/akira/go/pkg/mod/github.com/inference-sim/blis-latency-kernel@v0.3.0/kernel.go:373` defines the bounded-window token formula, and `caches.go` implements the bounded-cache calculation. The new generic KV code calls the declared `LifecyclePeak` instead of assuming an arbitrary `Hold` function has that property. No comment asserting a dependency, platform, vendor, or protocol rule is proposed for deletion.

Skipped preexisting reservation and policy-study changes, preexisting comments in the modified engine files, functional correctness sign-off owned by the separate reviewer, and speculative shared experiment frameworks. The current focused research package remains independently reviewable. The extracted `normalizeAdmission` removes duplicated rejection behavior and does not need a further abstraction.

The only file written by this review is `.audit/split-comments-review.md`. Suggested implementation cleanup touches only `sim/cluster/session_split.go`. Actual comment deletion count is zero because this was a report-only review.

Reviewed file hashes follow. The writer may continue editing after this snapshot.

```json
{
  "sim/admission.go": "7d0e8e0af759ab3eb1f30558a957a735c836043f51de04548d7d5f516661d771",
  "sim/cluster/session_split.go": "73144a275901236dea57272145fb328296cd6f4adb1ca2c12ba2bf1da1122f52",
  "sim/cluster/session_split_manifest.go": "f994c975fad02b965414acb10c6a7f603f362951d35268bcaa84b5155f85f491",
  "sim/cluster/session_split_test.go": "2adc59e6c9ed8761e4c09f9effd4e210138c66228f61405cceb83d114b657b09",
  "sim/kv/lifecycle.go": "3e2739e88dd90e981dc34a21a86d8739388620573df12cbc397439c99feabc0e",
  "sim/kv/lifecycle_test.go": "15f7aad12e3766f3b190a98a28a570a956b3852b6cd6172ad02c9b5c9063aea0",
  "sim/batch_formation.go": "f90142800e374bad4a8b416ebad69f225f8603c5ffa638934961457f568d6517",
  "sim/simulator.go": "9c5f458de820f80072aa0fd22aeb22f0b6ff5cbc44ce8b67d886e5fa089a9200",
  "sim/kv/cache.go": "d3b6fb2fdb7544d59f8993e6818d240cdbc8e058af1fca285c9adbc684f7c20b",
  "sim/config.go": "96032eff56db8efb57ba7e19efe2c325a0dfcc2035284f3645aa50242f873362",
  "sim/kernelmodel/settings.go": "d6549bfceff9f022a1503e1bb758e5d3e7785612c989d4cbc142b01fd46e00f6",
  "sim/kernelmodel/lifecycle_test.go": "e704e0ab184209adbacb9b205bf17570651056145424082604a2e9ec2a1fcbad",
  "cmd/research_router.go": "597dc01c4641084ed610c077feb83823fd56693ab50e06907a3cd87934c0ce2a",
  "cmd/research_router_test.go": "0b360f76ea21865e335509136aaf131e413112324fe5399229a8a4296faaaf3e",
  "cmd/replay.go": "fbaa8c1f02bbfeb8ae26c064de4ea42ce790002ca21a61f9907bc03515ad59a6",
  "research/session_split/__init__.py": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "research/session_split/analysis.py": "1f065b7230aea5e3cd84ef9cb418a0c89772fff1a14641cb83c9153d281bfa50",
  "research/session_split/common.py": "09e7c9f0404fbab73ab58e82147108219bd0367aef7bce01be57407d912527a7",
  "research/session_split/run.py": "086e7b8114c9947ee5655c4f12331c16611d88b497548bac23e84a2ab5b082ca"
}
```
