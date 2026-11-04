# Configure a semi-closed FreeInference run

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

The DeepSeek example reproduces a serving configuration used in the
[diagnostic study](../../semiclosed/deepseek_h200.md). It is not a calibrated
baseline. Native V4.1 weights with DSpark exceed the modeled capacity of one or
two H200 GPUs.

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
