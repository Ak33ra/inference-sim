# Research MLFQ and ATLAS scheduling

Semi-closed replay accepts `--research-scheduler mlfq` or
`--research-scheduler atlas`, optionally with `--research-scheduler-config PATH`.
The strict JSON configuration is:

```json
{"quanta_ms":[100,200,400,800,1600,3200,6400,12800],"starvation_ratio":10}
```

These are experiment defaults, not parameters reported by the Agentix paper.
Priority bands start at zero and use cumulative preceding quanta: [0,100),
[100,300), [300,700), etc.; the last band has no upper bound. Lower bands run
first, with FIFO ordering within a band. All new MLFQ requests start in band
zero. ATLAS snapshots the family's completed service at engine arrival and
selects the initial band from that snapshot. Family IDs come from the complete,
namespaced source family, not the child session.

Only calls that actually execute tokens receive the full model-step duration as
service. Prefill and recomputation count; queueing, render overhead, tool gaps,
and post-decode overhead do not. Execution is indivisible within a model step:
quantum expiration is handled at the next engine boundary. ATLAS publishes
`max(previous_service, inherited_service + call_model_time)` at model completion,
not at the earlier simulator event that calculates the forward pass. Timestamped
completion receipts are applied before arrivals at the same timestamp. All
replicas share one process table on the simulator event loop.

An expired quantum demotes a call by one band and appends it to that band.
At the bottom band, renewal preserves FIFO position. This is a progress-preserving
adaptation for the recomputation backend: bottom-band quantum expiry alone does
not repeatedly destroy a long prefill's progress. Higher-priority work can still
preempt it. The engine fills sequence and token budgets across all bands; it does
not artificially restrict a batch to the top band.

Starvation promotion uses waiting/service >= `starvation_ratio`. MLFQ uses the
call's counters; ATLAS adds the family's completed history. A promoted call moves
to the highest band with a fresh quantum and resets its starvation wait/model
counters. Lifetime accounting is separate and never reset. Completed family wait
history uses the maximum inherited-wait-plus-call-wait across completions, matching
the scalar service-history reduction. This is an explicit modeling choice for
the process-table wait update left unspecified by Algorithm 1.

The implementation ports Agentix's ATLAS scheduling policy, not its complete
serving engine or locality-aware load balancer. Displaced running calls release
GPU KV and use BLIS's existing recomputation resume path, preserving generated
output and TTFT while charging repeated prefill work. It does not implement
Agentix's CPU swap kernel or multi-step scheduler. Repeated higher-priority
arrivals can increase recomputation substantially; report that cost. The current
research path supports native prefix caching and excludes P/D, speculative decoding,
LoRA and offload. Existing scheduler/preemption overrides cannot be combined with
it. Without these flags, the native scheduling path is unchanged.

When a queued higher-priority call cannot allocate KV, it can displace a
lower-priority running holder even if sequence and token budgets admit both.
Research scheduling delays new prefix-hash and window-tail publication until
the final batch executes, so a canceled allocation cannot become a cache hit.
This also prevents reuse of another request's newly allocated prefix within
the same batch. Native FCFS retains its existing eager registration behavior;
cache-on comparisons therefore include this registration-timing difference.

The optional `research_scheduler` metrics block records effective configuration,
backend, `bottom_queue=fifo-renewal`, `program_wait_update=max-inherited-plus-call`,
per-request inherited and lifetime model service, cumulative queue wait,
demotions, promotions, policy displacements, resumes, memory preemptions and
recomputed tokens. Family service/wait histories are also saved. Initial scheduling
delay is retained across readmissions. The interval from first scheduling until
completion includes suspended time and must not be treated as execution occupancy.
Policy counters can be zero when all calls fit; this is a valid experiment result.

Reference: [Agentix, NSDI 2026, Algorithm 1 and sections 4–5](https://www.usenix.org/system/files/nsdi26-luo.pdf).
