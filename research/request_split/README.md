# Request SPLIT study

```bash
source .inference-sim-env/activate
go build -o /tmp/request-split-blis .
python3 -B -m research.request_split.run init research/outputs/mega-request-split-4s \
  --old-study research/outputs/mega-policy-study-final \
  --reference-study research/outputs/mega-session-reservation-4s \
  --binary /tmp/request-split-blis
python3 -B -m research.request_split.run run research/outputs/mega-request-split-4s --jobs 2
python3 -B -m research.request_split.analysis research/outputs/mega-request-split-4s
```

Use the existing frozen policy study directory for `--old-study`. Initialization validates its controls and isolated-family references, copies the binary, calibrates request sizes, and freezes the ten paired configurations. The study preserves eight B200 replicas, GPT-OSS-120B MXFP4 TP1, prefix caching off, FCFS native batching, four-second mean Poisson family arrivals, seeds 42–46 and simulator seed 42. Old study helpers are unchanged.

Replica 0 claims the longest ready request; replicas 1–7 claim the shortest from the same central queue. Ties use original arrival then request ID. Priorities stay fixed. Requests retain native chunked prefill, batching and decode, with lifecycle KV reservations preventing preemption. Requests sharing a session may coexist when the replay dependency graph permits it. Dependency-blocked requests have not arrived; capacity-blocked requests remain central.

Each replica retains its native preparation renderer. Central ingress selects preparation endpoints round-robin before GPU assignment, whereas JSQ selects preparation with its GPU. This ingress difference is part of the comparison. Preparation is charged once, using the same kernel service.

## Exact isolated service

The oracle deduplicates the frozen control's input/output lengths. One native single-replica replay chains these shapes with explicit completion dependencies and zero tool delay. Each next request arrives at the preceding request's client-visible completion. The service value is raw completion minus arrival, including preparation and output host service. It excludes queue waiting. No latency approximation or aggregate E2E metric supplies priorities.

Initialization verifies complete token accounting, no request overlap, empty native waiting queues, zero cache reuse and preemptions, and drained KV. It also runs nine representative shapes forward, backward and as clean standalone requests, including prefill chunk boundaries and long output, requiring exact service equality. Raw calibration traces, configurations, workload reports, metrics, logs and proof results remain under `oracle/`.

`provenance/service.json` records the trace and binary hashes, engine configuration fingerprint, source artifact hashes, exact shape costs, and each runtime request's identity, step and shape. The runner checks that loaded runs share the calibrated engine configuration. Direct CLI callers supply an externally trusted manifest:

```bash
blis replay --scenario scenario.yaml --scenarios /path/to/scenarios \
  --semi-closed-input requests.jsonl --num-instances 8 \
  --semi-closed-report workload.json --metrics-path metrics.json \
  --research-router request-split --research-router-config router.json
```

`router.json` contains `{"service_manifest":"/absolute/path/service.json"}`. Use the normal semi-closed replay and complete-drain flags for the input workload. The gateway validates manifest format, exact request/session/step, actual input/output lengths, and `MaxOutputLen=max(1,actual output length)`. It requires exactly eight compatible replicas. The direct CLI validates manifest identity and shape; binary, trace and engine provenance are enforced by this study runner, not by the gateway alone.

## Results and checks

Each run retains raw native artifacts and an ordered gateway audit. Independent audit replay reconstructs every ready-set extreme, preparation endpoint and worker wait, actual admission time, completion, batch capacity and lifecycle reservation accounting. JSQ must exactly reproduce the frozen controls, except catalog path metadata. Both policies retain identical logical work, root arrivals, tools and dependencies.

`analysis/` contains session and family elapsed times and common-reference slowdowns, per-run mean/p50/p99, medians and min–max across the five seeds, paired seed and object changes, `summary.json`, and `report.md`. Slowdown denominators are the shared best-observed isolated-family references, where siblings can contend; ratios below one remain valid. `requests.csv` records isolated service, client elapsed time, admission wait beyond preparation service, post-admission and post-first-token spans, and placement. Central-only fields additionally include preparation wait, central wait, ready count and batch size/tokens at claim; those fields are blank for JSQ.

A failed or interrupted run retains its output. Inspect it before using `run --retry-failed`, which archives the previous output. Runs and analysis verify frozen provenance; modify code or configuration before initialization, not after it.
