# Eight-second Poisson arrivals

- [x] Inspect the existing 8s pilot and keep the eight-B200 JSQ serving settings fixed.
- [x] Verify the seed-42 pilot matches the new config; run arrival seeds 43–46.
- [x] Analyze activity, TTFT, makespan and optrouting slowdown for all five seeds.
- [x] Compare with the five 12s runs and save runnable config, reproducible analysis and writeup.
- [x] Check all artifacts and report results.

Claim: increasing the root rate from 1/12 to 1/8 per second raises activity and may increase latency/makespan; measure rather than predict its size. Only arrival rate changes; input order, all tokens/delays, router seed and serving configuration remain matched. Simulation time is measured, not host wall time. Four independent output directories run concurrently; one writer aggregates.

Completed four new 8s runs and reused the matching seed-42 pilot. All five verify exact work and zero failures/preemptions. New config dry-run matches the saved pilot. Existing analyzers compute all request, family and fork statistics; compare.py verifies matching control settings/provenance and root schedules scaled by 2/3. Median window activity 88.3154% (84.3057–90.4646%); median family makespan 74.1383s. Outputs and reproducible writeup saved under mega-8b200-8s.
