# Run the session reservation experiment

Build a new binary and freeze the ten-run experiment in a new directory. Run these commands from the repository root after activating the simulator environment.

```sh
go build -o /tmp/session-reservation-blis .
python3 -B -m research.session_reservation.run init research/outputs/session-reservation \
  --old-study research/outputs/mega-policy-study-final \
  --binary /tmp/session-reservation-blis
python3 -B -m research.session_reservation.run run research/outputs/session-reservation --jobs 2
python3 -B -m research.session_reservation.analysis research/outputs/session-reservation
```

The `init` command verifies the old study, freezes the new source and binary, and derives one shared session manifest. It reuses the old trace, defaults, catalog, and registry read-only. Their hashes remain part of the new provenance. Do not edit helper sources or frozen inputs after initialization.

The ten loaded runs pair JSQ and `session-reservation` at arrival seeds 42 through 46. Each uses simulator seed 42, FCFS, eight B200 replicas, prefix caching off, and Poisson root arrivals with a four-second mean. Every run drains all 287 families, 2,385 sessions, and 58,741 requests.

To run one condition or seed, pass `--match` with a run-ID substring. Repeating `run` verifies saved artifact hashes before reusing completed results. After inspecting an incomplete output, pass `--retry-failed` to move it into `failed/` and retry. A changed frozen input or completed artifact always fails verification.

Read `analysis/report.md` for the generated report. `sessions.csv` and `families.csv` retain raw identities and each reference denominator. `per-run.csv` contains the mean, p50, and p99 slowdown for every run and cohort. `aggregate.csv` summarizes the median and range of each per-run statistic. `paired-deltas.csv` compares matched seed statistics, and `paired-sessions.csv` retains individual session ratios. `aggregate-median-ratios.csv` reports the separate ratio of aggregate medians.

The manifest ranks individual sessions by the minimum of `completion_us - arrival_us` across all 35 old cache-off isolated-family runs. This elapsed time includes tool and dependency waits. Sibling sessions still compete in those runs, so the denominator is a best-observed isolated-family session reference rather than a session-alone optimum. Ratios below one remain in the analysis. The candidate does not generate new reference runs.

The shortest 954 sessions can use all eight servers. The remaining 1,431 can use only the six servers other than `instance_0` and `instance_1`. Ranking ties use the full runtime session ID, formed as `trace_id + "/" + session_id`. Equal reference durations select the lexicographically first source run ID. The manifest stays fixed across all turns and both conditions. Family results are reported overall because a family can contain both session classes.

The native router accepts this JSON only for `--research-router session-reservation`.

```json
{
  "session_class_manifest": "/absolute/path/provenance/session-classes.json",
  "reserved_instances": ["instance_0", "instance_1"]
}
```

The router validates the reserved endpoint identities at startup. It selects the least-loaded eligible endpoint using the existing JSQ outstanding-request ledger and tie RNG. Unknown session classes and empty eligible sets abort the run. The focused runner adapts a standard JSQ preparation plan and saves the actual candidate configuration and command. Existing `research/semiclosed` and `research/policy_study` helpers remain unchanged.

Run the focused tests with these commands.

```sh
go test ./sim/researchrouter ./cmd -run 'TestSessionReservation|TestResearchRouter|TestMinLoad|TestFutureCompletion'
python3 -B -m unittest research.session_reservation.test_study
```
