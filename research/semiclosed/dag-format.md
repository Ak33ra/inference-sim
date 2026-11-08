# Explicit-DAG Weka input reference

The semi-closed file loader accepts one family per JSONL object, optionally
compressed with zstd. Each object has `id`, `models` containing one model,
`block_size: 64`, `hash_id_scope: "local"`, and `requests`.

Each leaf request has:

| Field | Meaning |
| --- | --- |
| `req_id` | Nonnegative integer unique within this family. Zero is valid. |
| `parent_ids` | List of immediate prerequisite request IDs in the same family. An empty list marks a root. |
| `type` | `n` or `s`. |
| `model` | The family's source model name. All families must use the same model. |
| `in`, `out` | Exact prompt and output token counts. Input must be positive; output can be zero. |
| `hash_ids` | Identities of full 64-token prompt blocks. Missing trailing tokens receive a request-local identity. |
| `t`, `api_time` | Required recorded start and duration in seconds. Both must be nonnegative. |
| `think_time` | Optional nonnegative source metadata in seconds. It does not add another runtime delay. |

A `type: "subagent"` entry uses `agent_id` and nested `requests` to identify a
child session. Nesting can be recursive. Every session must contain a leaf
request. Family and session membership come from this structure, while
`parent_ids` alone define execution dependencies. Array order need not be
topological. There are no added sequential, spawn, or join edges.

Every listed parent must complete successfully before a dependent is eligible.
Several parents represent a join. Several children sharing a parent can execute
concurrently. Missing parents, duplicate IDs or edges, self-dependencies, and
cycles are rejected. Explicit edges cannot be disabled with join policy `none`.

For a nonroot request, the delay is:

```text
delay = recorded start - latest recorded parent completion
eligible time = latest simulated parent completion + delay
```

Recorded starts and completion times are rounded to microseconds. Negative
delays are rejected. `think_time` in the natural Oolong trace includes child wait
time, so adding it after the join would count that wait twice. An entirely
untimed synthetic DAG can set both `t` and `api_time` to zero; its dependency
delays will be zero. The loader supports completion prerequisites only.

Root-family arrivals use `run.arrivals` in the JSON runner config:

| Config | Schedule |
| --- | --- |
| `{"mode":"trace"}` | Use the earliest root's `t` in each family. Default when arrivals are omitted. Oolong roots all arrive at zero. |
| `{"mode":"fixed","interval_s":1}` | Families arrive at 0, 1, 2, ... seconds in file order. Zero interval starts all families together. |
| `{"mode":"poisson","rate_per_s":1,"seed":42}` | First family arrives at zero. Subsequent independent exponential gaps have a mean of one second. |

The seed defaults to `run.seed`, which defaults to 42. Arrivals use a separate
RNG from the engine. The finite schedule is generated before replay and does not
depend on workload completion. Multiple roots within a family retain their
relative recorded offsets. Child sessions arrive through their dependency edges.

Native flags are `--semi-closed-arrivals`, `--semi-closed-arrival-interval-s`,
`--semi-closed-arrival-rate`, and `--semi-closed-arrival-seed`. The native arrival
seed defaults to 42 independently of `--seed`. The JSON runner currently keeps
FreeInference arrivals recorded; generated-arrival configs select `weka-dag`.

`workload.json` records the arrival settings, scheduled family `birth_us`, actual
request prerequisites and delays, family makespan, and session arrival/completion.
Each session's lifetime spans its earliest issued request through its latest
completion. Source timestamps remain separate from the generated schedule.
The report's `start_ms` and `end_ms` source fields have millisecond precision;
dependency scheduling uses microseconds. The source fingerprint covers the
uncompressed JSONL bytes, so plain and compressed copies have the same identity.

The loader lives in `sim/workload/semiclosed/dag.go`; arrival schedules live in
`arrivals.go`. Both feed the existing replay lifecycle and inference engines.
The independent `research/semiclosed/verify_dag.py` checks every source request,
token count, dependency, continuation time, and family/session lifetime against
the saved workload and engine reports. For Poisson schedules it checks ordering
and the zero-time first arrival; Go tests check RNG reproducibility.

## Composed inputs

For `trace.format: "weka-dag"`, the JSON runner accepts exactly one of `trace.path`
or `trace.sources`. `sources` is a nonempty list of objects with a required file
`path` and an optional positive integer `copies`, defaulting to one. Files can be
plain JSONL or zstd-compressed JSONL. Directories and globs are not accepted.

The preparation module `research/semiclosed/mix.py` writes a standard DAG input.
For each row ordinal, it visits sources in config order, emits that family's
copies, and skips sources that have ended. It continues until every source ends.
For example, two copies of source A and one of B produce A0-copy0, A0-copy1, B0,
A1-copy0, A1-copy1, B1, and so on. Fixed and Poisson arrivals follow this order.
Trace arrivals preserve each family's recorded root time regardless of row order.

Family IDs and every nested `agent_id` receive the same `sourceN/copyM/` prefix.
Both indices are zero-based. `req_id`, `parent_ids`, token counts, `hash_ids`, and
timestamps remain unchanged. Request IDs and prefix identities retain their
family-local scope, so copied families cannot acquire dependencies or cache hits
from one another. Existing prefix reuse within a family is preserved.

`composition.json` records the source paths, copy counts, uncompressed SHA-256
digests, original family IDs, emitted family order, and token/request totals.
Verification compares each emitted row with its original row, allowing only the
identity prefix, before checking the full replay against `input.jsonl`. It checks
copy multiplicity and ordering as well as totals. The original `trace.path` path
does not rename identities or create a composition manifest.
