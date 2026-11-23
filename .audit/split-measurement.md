# Measurement contract

Claim to evaluate: the session-level central-queue SPLIT adaptation changes mean, median and p99 session/family elapsed time and slowdown relative to JSQ/FCFS on the complete mega trace at four-second mean root interarrival. Direction is not assumed. This measures simulated time under the configured kernel model, not physical B200 wall-clock performance.

Both conditions use eight B200 replicas, one GPU and TP1 per replica, GPT-OSS-120B MXFP4, cache off, native FCFS batching, 16,384 batch tokens, 1,024 sequences and a 131,072-token model window. Arrival seeds42-46 are paired; simulation seed42 is fixed. Poisson gaps apply to family roots, with original dependency/tool waits for later work. Each run drains all 287 families, 2,385 sessions and 58,741 requests. No warmup or arbitrary measurement-window trimming is added.

The candidate has one largest-remaining and seven smallest-remaining endpoints. Requests remain resident once admitted. The comparator uses each request's fixed remaining oracle timeline from its session's selected saved reference run. Logical lifecycle KV promises provide nonpreemption; any admission restriction from those promises is measured and disclosed. Native baseline behavior is untouched.

Slowdown denominators are the same saved best-observed isolated-family references for both sides. They include sibling competition and tool/dependency waits. A value below one is permitted. Compute per-run mean and linearly interpolated p50/p99 over all objects, then report median and range across the five seeds. Paired percent changes are computed within each seed first. Keep them distinct from ratios of aggregate medians.

Required evidence before reporting:

- Identical source trace/hash, root arrival schedules, logical work, full token totals and DAG edges.
- Same frozen binary for all ten new runs; new JSQ reproduces frozen old workload/audit/verification/metrics after catalog path normalization only.
- Zero failures, drops, caps, timeouts, preemptions, residual queue/running work or KV allocation failures.
- Independent replay of central ready/claim events verifies exact extreme priority, actual scheduling time, role, and placement.
- Request TTFT/E2E retains causal arrival and central waiting; all dependencies retain recorded delay.
- Real batches with more than one request, distribution of work across eight endpoints, physical/promise capacity evidence and any promise refusals.
- Raw per-request, per-session and per-family output, seed-level tables, paired changes and source/config/binary hashes retained for later plotting.
- Root independent numerical audit uses a separate quantile implementation and direct workload/reference joins.

Inspect queue time, batch sizes, endpoint work distribution and KV admission refusals to explain measured differences. Host load affects experiment execution speed, not the simulated time being compared. Before preparation, host load was4.65 on256cores.
