# DeepSeek H200 comparison

User authorizes substituting trace deepseek-v4-flash with catalog deepseek-v4.1-flash. Confirmed hardware is one or two individual H200 GPUs per replica. Draft decoding is believed active, method/acceptance unknown.

Candidate configuration will be promoted only if it is feasible, drains every selected family with exact token totals, and matches on a held-out time selection. No catalog/coefficient edits to manufacture agreement.

Before looking at simulator results, define good agreement as median and p95 E2E and valid TTFT/TPOT distribution ratios within 0.8–1.2, with median paired absolute relative E2E error <=30%, on both discovery and validation selections. Report all metrics and limitations even if no configuration passes. Family makespan is supplementary because recorded non-LLM delays can dominate it.

Use complete single-model families, original root arrivals and delays, context counts/prefix identities, natural drain. Stratify by week/provider and prompt length; do not assume provider labels are globally stable. Source TTFT/TPOT uses the published 0<TTFT<0.95*E2E quality rule and output>1 for TPOT; show coverage. No source latencies or durations feed serving cost coefficients.

Initial grid is TP1/TP2, one replica, native checkpoint precision on Hopper, CPU engram offload, max context1M, DSpark K1/K3/K5 with registry coding acceptance. Resolve exact acceptance and report it. No-draft is a diagnostic control, not a candidate matching the premise. Hold scenario capacity/queue settings constant where feasible. Test memory feasibility first.

Data selections: inspect source weeks/providers and distributions before choosing a chronological discovery window and separate holdout. Preserve all roots/background traffic within each selected window and all their descendant work. Expand only if a possible match warrants it; do not promote based on one tiny cohort.

Prove It Works: check error counts, request/token conservation, source membership, replay determinism and actual speculation counters. Deterministic simulated time is not host wall time; use repeated seeds for stochastic draft acceptance and paired summaries across at least5 seeds for contenders. Machine load and runtime logs recorded for execution feasibility only.

Throughput checkpoint: hardware/acceptance inspection and comparison-tool implementation run independently; writers have isolated worktrees; parent owns all experiments and final interpretation. Serving engines and old paths stay unchanged.

## Feasibility and selection update

Direct kernel memory inspection shows even target-only TP2 weights exceed physical H200 HBM. DSparkK1/K3/K5 at TP2 use144.008GiB/rank vs139.801GiB device, withCPUEngramoffload. None of the requested1/2GPU configurations can be promoted. EP gives the same weight bound. Preserve this as the main feasibility result.

An additional TP4/K5 diagnostic is allowed to quantify model latency behavior; it is outside the confirmedhardware premise and cannot validate it. InitialTuesdayJuly14 discovery window had no eligible complete families. Before simulated latencyresults, change to FridayJuly17 andFridayJuly24 00:00–12:00UTC rootwindows. Counts4288/1202requests,36sessions each,30/33families, all selected families retained; choice based on availability/branchcoverage only. Original frozen windows and reason retained here.

The first holdout CLI attempt used a run-only --timeout flag and was rejected before simulation; corrected replay invocation drops it. This CLI error is not a performance result. No servingcode changed.

## Diagnostic repeatability

TP4/K5 seed42 fails the predefined latency agreement rule on both windows. Run seeds43 through46 on both fixed selections to quantify draft acceptance noise. These are five diagnostic runs per cohort, with no tuning or hardware promotion. Source latency data are fixed historical observations and cannot be remeasured. Every run receives independent source/request/token verification before aggregation.

Repeatability correction: sim/config.go SpeculativeConfig and sim/simulator.go commitDecodeTokens implement deterministic mean acceptance with fractional carry, not per-step RNG. Repeated seeds measure reproducibility, not draft-sampling uncertainty. All five holdout engine metric files are identical. The source profile reports measured mean acceptance on B200, but this study does not measure real H200 draft acceptance.

## Final result

All ten TP4 diagnostic runs completed. All five seeds per cohort independently passed original-source and engine verification. Engine metric objects are identical across seeds within each cohort. Every run fails the predeclared latency agreement rule. TP1/TP2 remain infeasible from weights alone; no key research configuration is pinned. The comparator passed eight unit tests and independent review reproduced both seed42 JSON outputs. Report and reproduction commands are in research/semiclosed/deepseek_h200.md and deepseek_h200_reproduce.md.
