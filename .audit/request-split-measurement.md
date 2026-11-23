# Request SPLIT measurement contract

The claim to test is whether request-level nonpreemptive SJF/LJF central scheduling lowers session elapsed time and family makespan relative to unrestricted JSQ/FCFS on the mega trace. No improvement is assumed.

Use eight one-B200 replicas, GPT-OSS-120B MXFP4 TP1, caching off, native batch token cap 16384 and sequence cap 1024. Keep root arrivals Poisson with mean four seconds, arrival seeds 42 through 46, simulator seed 42. Run matched pairs from one frozen binary. All requests, tokens, tool delays and DAG edges must be preserved, and no timeouts, drops, cancellations, preemptions or failed KV promises are allowed. Frozen old JSQ control equality is the compatibility gate.

Rank ready requests by client-visible elapsed service of the same input/output shape run alone with the same engine configuration. Include preparation and output host service, exclude tool delays and all queue waiting. Freeze oracle raw results, source identity and config hashes. Verify independence from calibration order and representative one-request standalone runs. Use per-request raw completion/arrival, not aggregate E2E that differs in output overhead accounting.

Report mean, median and p99 elapsed time and slowdown for both sessions and families. Each table value is median of five per-run statistics. Save min/max and paired per-seed percent changes; do not claim significance from a median alone. Slowdown references remain common best-observed isolated-family experiments, where siblings can contend, rather than proven optimal per-session runtimes.

Inspect per-request preparation wait, central wait, ready queue contention, batch size, decode span and native token processing. Bound an observed improvement by time that could be reordered. A lower ingress queue with worse decode batching can raise family makespan. These are simulated times, so wall-clock CPU competition cannot alter outcomes, but runtime limits must not truncate work. Host check before calibration: 256 cores, load average about 7.4.

Save raw runs, complete work reports, gateway event audits, calibration data, artifact hashes, session/family rows, per-run/aggregate stats and paired deltas under research/outputs/mega-request-split-4s. Write the concise report under research/writeups/mega-request-split-4s.
