# Research routing policies

Semi-closed replay accepts `--research-router jsq|least-token|random|agentix|preble-e2`.
These policies share a gateway, dispatch timing and causal outstanding-request
ledger. Existing llm-d routing remains the default. The study includes llm-d JSQ
as a separate control because its endpoint signals and tie-breaking differ.

JSQ counts all unfinished dispatched requests, including waiting and running
requests. Least-token sums their original input lengths. Random samples uniformly.
Agentix sends prompts of at most 2,048 tokens to the least-loaded replica and
pins longer calls from a family to the replica chosen for its first long call.
The family is the complete namespaced program, including its child sessions.

Preble E2 uses a 180-second history. When more than half the prompt is cached,
it chooses among replicas with the longest observed prefix by total cost.
Otherwise it chooses among all replicas. Cost combines recent service demand,
estimated eviction cost and new prefill work. Output estimates use completed
requests, with an explicit cold-start prior. Costs require independently
calibrated prefill and decode rates. No policy reads future output lengths.

Cache observations arrive through timestamped engine KV events, with the
configured delivery delay. The E2 implementation uses block prefixes and an
observed LRU approximation. It excludes Preble's prefill/decode balancing
shortcut, prefix replication/rebalancing and local scheduling. Label results
as **Preble E2 cost**, rather than a reproduction of the complete system.

Optional strict JSON parameters in `--research-router-config` are
`latency_us`, `cache_event_delay_us`, `history_window_s`,
`prefill_us_per_token`, `decode_us_per_token`, and `initial_output_tokens`.
Preble requires explicit positive prefill/decode rates and a nonnegative output
prior. Unspecified latencies inherit the normal gateway configuration.
`--research-router-decisions PATH` saves effective configuration, decision
records and cache-event/exploit/explore counters.

The JSON experiment wrapper exposes the same choices under
`serving.router.research_policy` and `serving.router.parameters`.
See [the study runner](../../research/policy_study/README.md) for a complete grid.

References: [Agentix Algorithm 2](https://www.usenix.org/system/files/nsdi26-luo.pdf)
and [Preble Algorithms 1–2](https://arxiv.org/html/2407.00023v2).
