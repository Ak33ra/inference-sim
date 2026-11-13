# Configure a semi-closed FreeInference run

For the combined synthetic workload, use
[mega-8b200-medium.json](mega-8b200-medium.json): all eight Commit0 buckets plus
Oolong, served by eight B200 TP1 replicas with JSQ and prefix caching off.
The [baseline writeup](../../writeups/mega-8b200-baseline/README.md) documents
trace preparation, arrival calibration, results, and reproduction commands.
Use [mega-8b200-8s.json](mega-8b200-8s.json) for Poisson arrivals averaging eight
seconds between root families. The [8s comparison](../../writeups/mega-8b200-8s/README.md)
reports the same workload at the higher arrival rate.

Edit one JSON file and invoke it from any working directory:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/glm52-smoke.json
```

Use the repository's absolute script and config paths when invoking them from
another directory. `research/scripts/semiclosed.sh` accepts the same command.
JSON parsing needs no additional configuration library.

The config launcher requires Python 3.11 or newer as `python3` on `PATH`.
Conversion and verification also need PyArrow; the runner uses `uv` when it is
available and the default Python lacks PyArrow. `--python` selects the interpreter
for those data-processing steps, after the launcher has parsed the config.

Start with one of these complete examples:

| Config | Source selection | Serving layout |
| --- | --- | --- |
| [glm52-smoke.json](glm52-smoke.json) | Week 9, the existing 175-request root window | GLM-5.2-FP8, two B200 TP8 replicas |
| [glm52-all.json](glm52-all.json) | All available complete weeks, no root window | GLM-5.2-FP8, two B200 TP8 replicas |
| [deepseek-h200-diagnostic.json](deepseek-h200-diagnostic.json) | Week 10, all eligible DeepSeek-V4-Flash families | DeepSeek-V4.1-Flash, one H200 TP4 replica, DSpark K5 |
| [deepseek-h200-2replicas.json](deepseek-h200-2replicas.json) | Week 10, all eligible DeepSeek-V4-Flash families | DeepSeek-V4.1-Flash, two H200 TP4 replicas, DSpark K5 |

The DeepSeek example reproduces a serving configuration used in the
[diagnostic study](../../semiclosed/deepseek_h200.md). It is not a calibrated
baseline. Native V4.1 weights with DSpark exceed the modeled capacity of one or
two H200 GPUs.

The two-replica DeepSeek config allocates two four-GPU nodes (eight H200 GPUs
total) with an `ib-400g` fabric. It keeps the diagnostic config's engine settings
and uses BLIS's default llm-d Endpoint Picker: queue, KV-cache utilization, and
prefix-cache scorers with weights 2, 2, and 3, followed by a max-score picker.
Run it with:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/deepseek-h200-2replicas.json
```

Omitting `serving.router` uses the default router. To select a native Endpoint
Picker profile, add this object inside `serving`:

```json
"router": {"endpoint_picker_config": "../routers/jsq.yaml"}
```

The path is relative to the JSON config. The launcher snapshots the profile as
`router.yaml` in the run directory, records its SHA-256 in the resolved config,
and points the generated deployment at that snapshot. See the
[routing guide](../../../docs/guide/routing.md) for supported plugins.

Preview the resolved dates, layout, scenario, and commands before a large run:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/glm52-all.json --dry-run
```

Dry-run resolves configuration without creating files, building BLIS, converting
data, or downloading Python dependencies. BLIS checks model and hardware
feasibility when the real run starts.

## Choose the models and weeks

`trace.model` is the exact FreeInference request label. `serving.model` is the
catalog model whose serving costs BLIS simulates. For example:

```json
{
  "trace": {"model": "deepseek-v4-flash", "weeks": [9, 10]},
  "serving": {"model": "deepseek-v4.1-flash"}
}
```

This fragment illustrates the two fields; start from a complete example to run
it. The substitution preserves source prompt and output token counts.
Conversion retains complete single-model families. Mixed-model families remain
excluded, including their requests for the selected model.

Week numbers refer to the pinned release, independently of the model and which
weeks are downloaded:

| `trace.weeks` | Selection |
| --- | --- |
| `1` | May 17 to May 23, 2026 |
| `[1, 2, 3, 4]` | First four release weeks |
| `9` | July 12 to July 18, 2026 |
| `10` | July 19 to July 25, 2026 |
| `"all"` | Every available complete week in the pinned 16-week release |

Missing an explicitly requested week is an error. `"all"` records which weeks
are available and which are missing or incomplete. Missing earlier weeks never
renumber later ones. A complete week has its manifest and processed table
metadata plus requests, sessions, and tool-call Parquet tables.

Optional `trace.start_ms` and `trace.end_ms` restrict root arrivals to a half-open
Unix-millisecond interval. Every selected family still drains beyond its end.
The smoke example uses these fields. Remove them to include the full selected
weeks. A week can contain no eligible families for a particular model; conversion
reports that condition instead of choosing a different model or week.

## Set the cluster and engine

`serving.cluster` describes a uniform colocated layout:

```json
{
  "hardware": "b200",
  "fabric": "ib-400g",
  "gpus_per_node": 8,
  "tp": 8,
  "replicas": 2
}
```

The runner derives two nodes and passes two instances to BLIS for this example.
For TP4 with one replica on an eight-GPU node, it allocates one node with four
unused GPUs. For TP16 on eight-GPU nodes, it allocates two nodes per replica.
TP must divide the node width or be a whole multiple of it. Multi-node layouts
require an explicit `fabric`.

This config format uses PP1 and DP1. Use `replicas` for independent copies of the
model. Optional `pp` and `dp` must both equal 1. The existing script's scenario
path remains available for other deployment configurations.

`serving.engine` uses native BLIS field names for the supported cache, batching,
context, CUDA graph, memory, prefix-cache, Engram, and speculative settings.
Start with the complete engine object in an example. Misspelled or unsupported
config fields are rejected; model-specific feasibility remains BLIS's decision.

When the engine has speculative decoding enabled, supply the registry acceptance
selectors in `serving.acceptance`. The DeepSeek example uses:

```json
{
  "category": "coding",
  "thinking": "off"
}
```

The scenario's engine version defaults to `0.31.0`. Override
`serving.engine_version` and `serving.coefficients` only when the selected catalog
and registry support those values.

## Choose paths and run settings

| Field | Meaning |
| --- | --- |
| `version` | Config format, currently `1` |
| `output` | New run directory; omission selects a timestamped repository output directory |
| `trace.source` | FreeInference release directory; defaults to the repository trace path |
| `catalog`, `registry` | Repository paths; default to the activated BLIS environment |
| `defaults` | BLIS defaults file; defaults to the repository's `defaults.yaml` |
| `python` | Optional Python executable with PyArrow; otherwise use the existing Python/uv selection policy |
| `run.seed` | Simulation seed, default `42` |
| `run.join_policy` | `"timing-inferred"` by default; `"none"` disables inferred joins |
| `run.series_interval_s` | Server metrics interval in simulated seconds, default `0` to disable |

Paths written in the config are relative to the config file's directory.
Repository-owned defaults stay relative to the repository. CLI overrides are
relative to your current directory. For example:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/glm52-smoke.json \
  --output /tmp/my-glm-run
```

`--output` overrides the config. `--python` overrides its interpreter setting.
The launcher loads `.inference-sim-env/activate` when present and preserves caller
catalog, registry, and Go-cache overrides. Use a fresh output path for each run.

The run retains `config.original.json`, `config.resolved.json`, the generated
`scenario.yaml`, exact argument arrays in `commands.json`, shell-readable
`commands.log`, and `run.log`. Workload artifacts are the converted `input/`,
`workload.json`, `metrics.json`, and `verification.json`. A failed command returns
a nonzero status and leaves its artifacts for inspection. No simulation horizon
or family concurrency cap is added.

Verify a saved successful run again with the existing command:

```bash
research/scripts/semi_closed.sh verify /tmp/my-glm-run
```

If the config used a custom source directory, pass that directory with `--source`
to this legacy verification command.

Synthetic traces with explicit request dependencies use
[natural-oolong.json](natural-oolong.json). It runs GPT-OSS-120B on one B200,
with one root family arriving each second. See the [DAG run guide](../../semiclosed/dag.md)
for Poisson arrivals, simultaneous starts, and saved-run verification.

The Oolong node-count study keeps prefix caching disabled and uses one B200 TP1
replica per node. The default router distributes requests across replicas.
Use [one node](natural-oolong-1node.json), [two nodes](natural-oolong-2nodes.json),
[four nodes](natural-oolong-4nodes.json), or [eight nodes](natural-oolong-8nodes.json).
Each config writes to its own output directory and retains fixed one-second root
arrivals with seed 42. For example:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/natural-oolong-4nodes.json
```

[commit0-overlap.json](commit0-overlap.json) mixes the `w25-29` and `w30-34`
Commit0 buckets, with two copies of every family. It uses four B200 TP1 replicas,
prefix caching off, and one root arrival per second. Edit `trace.sources` to mix
other files, and `run.arrivals` to control overlap. See
[Overlap traces and add users](../../semiclosed/dag.md#overlap-traces-and-add-users)
for simultaneous starts and Poisson arrivals.

The same overlapped workload has a six-config routing grid:

| Router | 4 replicas | 8 replicas | 16 replicas |
| --- | --- | --- | --- |
| JSQ | [config](commit0-overlap-jsq-4replicas.json) | [config](commit0-overlap-jsq-8replicas.json) | [config](commit0-overlap-jsq-16replicas.json) |
| Least-token | [config](commit0-overlap-least-token-4replicas.json) | [config](commit0-overlap-least-token-8replicas.json) | [config](commit0-overlap-least-token-16replicas.json) |

[JSQ](../routers/jsq.yaml) scores replicas by unfinished requests, including
waiting and running requests. [Least-token](../routers/least-token.yaml) scores
them by in-flight prompt tokens, with output-token estimates disabled. These
nonstreaming traces retain that token load until response completion. Each
profile uses only its named scorer and a max-score picker. Hardware, model,
arrivals, prefix-cache settings, and trace copies are otherwise identical.
Both profiles explicitly disable flow control and use a composite pool detector
that adds no endpoint filter. This prevents llm-d's default utilization filter
from excluding the minimum-load replica before scoring.

Run one config, using a fresh output path when repeating a saved run:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/commit0-overlap-jsq-8replicas.json \
  --output /tmp/commit0-jsq-8
```
