# Mega trace and eight-B200 baseline

- [x] Ground: nine original sources, existing composer/DAG replay/router profiles, saved metrics. One copy each, no FreeInference.
- [x] Sketch and compare: unchanged composer with isolated preparation artifact versus extending composer ordering. Keep model, requests, dependencies and delays intact; shuffle whole families once to spread unequal source populations.
- [x] Agree: proceed within authorized scope, no human checkpoint needed.
- [x] Implement trace preparation and verify exact provenance, counts, tokens, dependencies, and ordering.
- [x] Run independent full-trace arrival pilots on eight B200 TP1 replicas, GPT-OSS-120B mxfp4, pure JSQ, prefix cache off.
- [x] Choose and repeat a medium-load arrival configuration; drain every family and verify each run.
- [x] Analyze load over a defined arrival window and full-run makespan/fork ratios; save commands, summaries, plots, config and concise writeup.
- [x] Independent review and direct artifact verification.
- [x] Scrap: skip; source preservation and all replay checks passed, so no redesign was needed.

Throughput checkpoint: main owns all files and runs. Two existing agents provide bounded read-only metric/design reviews because the session previously exhausted new-agent capacity. Use distinct run directories and a single aggregation writer. Reuse the standard engine/config path. First verify the trace and pilot measurement before broadening repetitions.

Claim to test: a fixed, reproducible Poisson arrival rate gives moderate replica activity and modest queueing for this finite mixture on eight B200s. Engine virtual time is measured, not host runtime. Initial operational target: roughly 65–75% replica request-active time during the central 80% of the root-arrival interval, with no failures and bounded scheduling delay. This is not hardware SM utilization or a universal definition of medium load. Final target/definition must reflect inspected metric semantics.

Host at start: load average 4.25/4.61/4.55, 256 CPUs. Pilot rates will bracket the target, with matched order/router/arrival seeds. Selected rate will be repeated across seeds; do not turn this into a router comparison.

Pilots: 16/8/4s mean Poisson root spacing yielded 56.59/88.32/99.87% request-active time in the central 80% arrival window. p95 scheduling 79.54/98.18/129.89ms; exact work, zero errors and preemptions. Twelve-second spacing is the next measured candidate. The builder retains complete namespaced JSONL families, hashes a seeded family key, and verifies all bytes against the original source composition. Three new tests plus ten existing mixer tests pass.

Complete: selected Poisson rate 1/12 roots/s, five arrival seeds 42–46 with router/order seed 42. Request-active fraction median 70.12%, range 64.16–71.60%; p95 scheduling median 84.11ms, range 82.56–85.93ms; family median makespan median 68.207717s. Eight distinct full-trace runs complete with exact tokens, zero failures/preemptions, all 60,551 dependencies verified. Final config matches measured seed 42 except output location; dry-run and documented saved-run verification succeed. Thirty preparation/mixer/config tests pass. Final independent review reproduced all five summaries and found no blockers. Plots visually inspected and writeup links checked.

Artifacts: traces/mega-commit0-oolong; research/configs/semiclosed/mega-8b200-medium.json; research/writeups/mega-8b200-baseline/{README.md,analyze.py,plot.py}; research/outputs/analysis/mega-8b200-baseline. No commits or publication performed.
