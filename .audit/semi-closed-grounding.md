# Semi-closed replay implementation brief

The user confirmed recorded root arrivals, completion-driven internal work, and natural departure after every selected family drains. There is no family concurrency cap, sampling, warmup, recycling, or idle compression. The user approved controlled AgentX comparisons with matched requests, tokens, timing, and measurement windows while preserving existing defaults.

The cluster already supplies initial arrivals, ArrivalGate, OnComplete, SetUnservedHook, and natural drain. P/D reports a copied parent request with its original ID. RequestSource is consumed before the event loop, so it cannot be an online session driver. Root timers and continuations must use the existing arrival/completion hooks.

AgentX owns private tree and prompt reconstruction, benchmark windows, sampling, and recycling. Its wire prompts add chat tokens and cache markers. It forces each output to at least one token. At MinRatio=MaxRatio=0, its initial snapshot starts at turn zero without primer warmup. Disabling idle caps and suppressing recycled trees in a test adapter permits a finite-cohort comparison. The adapter must match actual prompt arrays before service; comparing projected prompt counts after service cannot establish engine metric equality.

The research converter preserves original counts, source request IDs, absolute family origins, candidate joins, and residuals in corpus.jsonl plus audit.jsonl. Conversion rejects overlapping turns and nested delegation. Its current pilot contains no child that starts before its spawning request finishes. Future overlap handling must explicitly use the parent's send plus offset, or reject the family, never clamp a negative completion-relative delay silently. Joins are inferred, not observed barriers.

The original pilot has 207 families, 252 sessions, 18,368 requests, 3,092,043,723 prompt tokens, 9,195,478 output tokens, and nine zero-output requests. Engine zero-output support already has tests in sim/simulator_test.go. The new workload must not add prompt tokens or force outputs to one.

A fresh source conversion is available at research/outputs/semiclosed-source-smoke. It selects roots in [1784081087725,1784086819492) from 2026-07-12_2026-07-18 for glm-5.2 and retains complete families. An independent Parquet sum in parquet-reference.json gives two families, ten sessions, 175 requests, 9,077,878 prompt tokens, 169,589 output tokens, and one zero-output request.

The implementation worktree is /tmp/inference-sim-semi-closed. Its baseline commit 2b2204a snapshots the user's existing untracked research files, without changing their state in the primary checkout. Only changes after that commit belong to this task.

Use source /home/akira/inference-sim/.inference-sim-env/activate for Go. Set GOCACHE=/tmp/semi-closed-go-cache and unset BLIS_CATALOG BLIS_REGISTRY for tests. The live catalog has .git metadata that makes the existing provenance fixtures fail after copying it. The three failing fixtures pass with those environment variables unset. CLI HTTP tests need socket permission outside the sandbox. Python with PyArrow is /scratch/akira/routing-sim-venv-h200b/bin/python.

## Design rubric

1. Preserve original tokens, arrivals, dependency delays, and finite population without silently omitted work.
2. Keep serving engines, AgentX defaults, and other replay paths unchanged.
3. Use one owner for dependency/lifecycle state and a small boundary for CLI integration.
4. Produce independent engine metric parity and original-trace token conservation evidence.
5. Leave a reproducible, readable research artifact with explicit join and prefix assumptions.
