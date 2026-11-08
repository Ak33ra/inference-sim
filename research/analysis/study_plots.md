# Plots for agent-makespan policy studies

Family makespan should be the primary outcome. Request latency explains some
changes, but a faster request on a noncritical branch need not shorten the agent.
Recorded non-LLM delays can dominate the total even when serving improves.

The standard report implements the first five views below. The remaining views
are useful next additions once the policy experiment and instrumentation are
settled.

| View | Research question | Availability |
| --- | --- | --- |
| Family/session lifetime ECDF and sample counts | Does the policy improve typical agents and the slow tail? | Existing complete-family makespan and session timestamps |
| Paired family makespan change versus baseline | Which of the same agents improve or regress? | Existing stable identities, with matching source work and dependency checks |
| TTFT mean/p50/p95/p99 bars and request latency ECDF | Does the policy change admission delay or the remaining first-token time? | Current limited attribution; pure queue/prefill split needs more timing instrumentation |
| Active/unfinished family count and completion curve over time | Does progress slow during bursts or near trace drain? | Existing arrival/completion times; failed/incomplete families stay in the unfinished count |
| Waiting/running requests and KV occupancy over time | Are gains associated with less queueing or cache pressure? | Existing optional server series; recording must be enabled before replay |
| Paired makespan change by family size, fan-out, prompt length, and non-LLM-delay share | Does one class of agent gain at another's expense? | Size/tokens/session counts are available; exact causal fan-out and critical-path delay shares need careful definitions |
| Branch completion spread, slowest/median branch ratio, and parent join wait | Is one straggler delaying the parent? | Needs explicit fork/join boundaries from the makespan recorder; current joins are timing-inferred |
| Critical-path breakdown into LLM waiting/service, KV transfer, and external work | Why did whole-agent latency change? | Requires time-aligned per-request phase intervals and dependency semantics; sums across parallel branches are invalid |
| Completed-family fraction below a makespan target, with failed/censored counts | Does the policy satisfy an agent-level latency goal without sacrificing completion? | Existing lifetimes plus explicit target and censoring treatment |
| Makespan versus token throughput, cache-hit rate, and replica balance | Does higher server throughput translate to better agents? | Existing aggregates and request instance tags; report workload duration and idle trace gaps |

Use paired changes on the same source work before comparing unpaired means.
Show coverage and failures beside the main result. Different model populations,
hardware, arrival windows, or inferred dependency graphs can otherwise look like
policy gains. The report retains those inputs but does not enforce a single
experimental variable.

For repeated seeds, summarize at the run and family level. Requests within an
agent are correlated, so treating every request as an independent experimental
replicate overstates precision. The current report gives descriptive statistics
and keeps seeds separate. It does not generate confidence intervals or claim
statistical significance. With only a few complete families, show their
individual values and sample counts rather than emphasizing p99.
