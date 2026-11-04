# FreeInference replay with AgentX

Decision as of 2026-10-30: first convert FreeInference to Weka and evaluate BLIS's
existing AgentX replay behavior. Defer a custom workload driver until the pilot
shows a consequential limitation. BLIS's serving engine remains unchanged.

The [offline converter](freeinference_trace/README.md) writes a Weka corpus,
provenance manifest and family audit for this evaluation. The
[research launcher](freeinference_replay/README.md) supplies the FreeInference
corpus identity and runs the existing BLIS CLI with saved configurations.

Weka is the trace format. AgentX supplies fixed family concurrency,
completion-driven turns and recycling. Lanes remain occupied through tool/idle
gaps; recorded arrivals between independent families no longer determine load.

Desired timing: simulated completion plus the recorded non-LLM gap, with only
the residual delay after required children finish at a join. Unmodified AgentX
does not guarantee exact preservation of these delays.

| Behavior or limitation | Implication for the pilot |
| --- | --- |
| **Idle compression** | The standard scenario locks the global idle cap to 10 seconds, pulling pending timers forward when the whole workload is idle. Long gaps may shrink. The underlying driver supports disabling this; the standard scenario does not. |
| **Spawn and join timing** | Spawn offsets need careful conversion. A blocked parent resumes immediately when children finish, losing its post-join residual. Negative sequential gaps are clamped to zero. |
| **Dependency reconstruction** | The loader infers joins and can split agents using prefix patterns. FreeInference's reconstructed links do not prove joins. BLIS attaches children to the root and cannot preserve arbitrary nested delegation. Check branch membership and report unsupported families. |
| **Prefix and token semantics** | FreeInference's chained 16-token `o200k` blocks need mapping to 64-token hashes while retaining provider prompt lengths. AgentX adds synthetic chat-template tokens and isolates plays with cache-busting prefixes. Outputs are at least one token. Check sharing, especially among siblings. |
| **Sampling, warmup and outcomes** | Roots are selected in corpus order; short runs may never reach later fan-out families. Sampled starts can yield partial families; warmup primes caches. Ordinary errors allow continuation; context overflow ends a stream. Windows/horizons can interrupt families. Separate complete, partial, failed and censored observations in makespan metrics. |
| **Configuration and provenance** | The standard descriptor expects SemiAnalysis data and locked settings. The research launcher registers a FreeInference alias and pins the converted digest; timing locks remain. All requests use the scenario's model; initially select complete single-model families. |

Concurrency, recycling, sampled starts, warmup and cache isolation can be accepted
benchmark choices if stated. Audit request/token counts, branches, shared prefixes
and changed delays; inspect several pilot families. Prioritize branch structure
and sharing because they affect contention and routing. Retain original timing
and IDs for comparison; adapt only behavior shown to matter.

References: [AgentX scenario](https://github.com/ai-dynamo/aiperf/blob/v0.12.0/docs/tutorials/agentx-mvp.md),
[BLIS tree reconstruction](../sim/workload/agentx_tree.go),
[replay and prompt generation](../sim/workload/agentx_replay.go),
[CLI configuration](../cmd/replay_agentx.go).
