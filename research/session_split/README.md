# Session SPLIT study

Freeze one binary and compare JSQ with central session scheduling on paired arrival seeds 42–46:

```bash
source .inference-sim-env/activate
go build -o /tmp/blis-session-split .
python3 -B -m research.session_split.run init research/outputs/mega-session-split-4s \
  --old-study research/outputs/mega-policy-study-final \
  --reference-study research/outputs/mega-session-reservation-4s \
  --binary /tmp/blis-session-split
python3 -B -m research.session_split.run run research/outputs/mega-session-split-4s --jobs 2
python3 -B -m research.session_split.analysis research/outputs/mega-session-split-4s \
  --report research/writeups/mega-session-split-4s/README.md
```

The runner freezes the chosen isolated reference timelines and derives each turn's remaining size from reference session completion minus reference request arrival. The original reference timestamps, run IDs and file hashes remain in `provenance/remaining.json`. Old helpers and results remain unchanged; new JSQ controls must reproduce the frozen controls exactly, apart from `catalog.path`.

This is a [SPLIT-inspired](https://arxiv.org/pdf/2605.13749) adaptation. It ranks sessions by remaining oracle time and makes individual requests nonpreemptive while retaining batching. It does not reproduce the paper's preemptive job model or its theoretical guarantees. Each session must have sequential requests; forks and joins between sessions remain supported. The study runner binds the oracle to the frozen trace digest; the gateway validates runtime request, session and turn identities.

`--research-router session-split` accepts `{"remaining_manifest":"/absolute/path/remaining.json"}` through `--research-router-config`. Optional `latency_us` overrides gateway latency. This mode requires complete-drain semi-closed replay, homogeneous independent engines, FCFS and prefix caching off. It rejects deadlines, offload, speculation, adapters, P/D, autoscaling and competing research schedulers.

Replica 0 claims the largest remaining ready session. Other replicas claim the smallest. Ties use causal arrival and request ID. Native batching controls admission; a request stays central until its first KV allocation succeeds. Logical KV promises cover its full client output budget, including bounded model state, while physical allocation follows native chunks. Capacity-blocked requests stay central; dependency-blocked turns have not arrived. Each admitted request finishes without preemption.

Preparation retains native renderer workers. Central mode distributes preparation round-robin across endpoints independently of eventual GPU choice. JSQ couples preparation and GPU routing. Both charge the same kernel service once; this ingress difference is part of the comparison. The audit records and independently verifies renderer queues, ready-set extrema, actual GPU admission, completion, native batching, KV promises, final release and zero preemption.

`analysis/` contains raw session and family elapsed times and slowdowns, paired object ratios, per-run mean/p50/p99, five-seed median/range, paired seed changes, and audit evidence. Slowdowns use the frozen best-observed isolated-family reference, including sibling contention and tool delays; values below one are retained.
