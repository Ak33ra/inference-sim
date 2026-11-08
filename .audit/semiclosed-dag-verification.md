# Explicit DAG replay verification

Working-tree implementation on top of `74ba01f`. Existing analysis work remains
separate. No commits or remote writes were made for this task.

## Source and timing

`natural-oolong-fanout.jsonl.zst` contains 25 families, 608 child sessions,
633 total sessions, 1,608 leaf requests, and 2,191 explicit dependency edges.
The only source model is `gpt-oss-120b`.

Source totals are 40,606,279 prompt tokens and 420,697 output tokens.
There are no zero-output requests in this file; the small Go fixture tests them.

The independent reviewer found that source `think_time` includes join waits.
The implementation therefore uses the recorded start minus the latest recorded
parent completion as the continuation delay. All edges are completion edges.
The reviewer reproduced every recorded source arrival with recorded durations.

## Real runs

- Fixed spacing, one family per second, seed 42:
  `research/outputs/semiclosed/natural-oolong-fixed-1s/verification.json`.
- Poisson gaps, one family per second in expectation, seed 42:
  `research/outputs/semiclosed/natural-oolong-poisson-1ps/verification.json`.

Both runs completed every request and matched both source token totals.
The verifier checked every edge, continuation time, and family/session lifetime.
Both reached 25 active families. The scenario uses one GPT-OSS-120B B200 TP1
replica with the committed MXFP4 engine settings. This is functionality evidence,
not a calibration claim.

## Automated checks

- `go test ./sim/workload/... ./sim/cluster`: passed, including existing AgentX
  and FreeInference coverage.
- `go test ./sim/workload/semiclosed ./cmd -run 'Test(DAG|SemiClosed|ReplaySemiClosed)' -count=1`: passed.
- `python3 -B -m unittest research.scripts.tests.test_semiclosed_config research.scripts.tests.test_semiclosed_dag_verify`: 19 passed.
- `research/scripts/semi_closed.sh verify research/outputs/semiclosed/natural-oolong-fixed-1s`: passed.
- Bash syntax, config dry-run, and `git diff --check`: passed.

An initial full CLI suite under the machine-local `BLIS_CATALOG` failed three
catalog provenance fixtures. Their fixture copy includes that catalog's existing
`.git`, so an attempted initial commit reports a clean detached checkout.
`env -u BLIS_CATALOG go test ./cmd` passed with the vendored catalog fixture.

## Independent review

The independent reviewer checked the native loader, replay integration, runner,
verifier, and comments. Findings fixed and retested:

- Reject output aliases of the input, including symlinks and hardlinks.
- Round absolute source completion times, rather than separately rounding duration.
- Reject missing timestamps and negative values smaller than one microsecond.
- Verify reported source timestamps against the original source.

Additional probes passed for out-of-order requests, multiple roots, maximum
session completion time, and decompressor termination after malformed input.
No blocking findings remain in the reviewed scope.
