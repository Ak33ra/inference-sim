# FreeInference replay through BLIS

Run a [converted FreeInference corpus](../freeinference_trace/README.md) through
BLIS's existing AgentX driver and serving simulator. The research launcher adds
a process-local corpus alias so the descriptor names FreeInference correctly.
It retains the standard scenario's timing, sampling, warmup and recycling rules;
see [replay limitations](../AGENTX_LIMITATIONS.md).

From the repository root:

```bash
source .inference-sim-env/activate
go run ./research/freeinference_replay \
  --config research/configs/experiments/glm52-2r-smoke.yaml \
  --output research/outputs/glm52-config-smoke
```

This experiment uses one family lane, seed 42, 900 simulated seconds of profiling,
30 seconds of grace, and two GLM-5.2 FP8 replicas, each using eight B200s.
The deployment uses GPU-only KV storage and BLIS's default llm-d router.
Warmup precedes profiling; simulation time is independent of wall-clock runtime.
The output directory must be new. Python and PyArrow are needed only for conversion.
See [research configurations](../configs/README.md) for the configuration layers,
cluster controls and named one-, two- and four-replica candidates. Calibration
is pending. The earlier `--input DIR --output DIR` form still uses the smoke defaults.

Increase workload concurrency independently of serving replicas:

```bash
go run ./research/freeinference_replay \
  --config research/configs/experiments/glm52-2r-c32.yaml \
  --output research/outputs/glm52-config-c32
```

Concurrency counts live family trees. Children can issue additional concurrent
requests, and completed families are replaced. AgentX selects roots in corpus
order and samples starting positions within the initial families. A short run
can miss later fan-out families; inspect `children_spawned` and `subagent_requests`
before using it to evaluate branches. It does not promise to execute every source
request or complete every family.

`research/configs/workloads/agentx.yaml` holds shared workload settings, including
`split_flattened_agents: false`. An experiment supplies load, seed and duration;
explicit CLI flags override those values. Its referenced paths resolve relative
to the experiment file, while CLI paths resolve relative to the working directory.
The launcher fills the corpus fields from the conversion manifest and verifies
the converted file's size and SHA-256; BLIS verifies that digest again when loading.
The source revision identifies upstream Parquet data, while the digest identifies
the derived Weka file. The copied conversion manifest connects them. Named
experiments additionally pin the converted corpus digest, even if the input
directory's conversion manifest is replaced.

Use `--workload` for another settings file and `--scenario` for another
self-contained Scenario/Deployment YAML; accompany a placement change with the
matching `--num-instances`. Scenarios that reference additional configuration
files need snapshot support before use here. `--catalog` and `--registry` can
replace the `BLIS_CATALOG` and `BLIS_REGISTRY` environment variables. Further flags
are listed by `go run ./research/freeinference_replay --help`.

For repeated experiments, build once and optionally reuse parsed corpora:

```bash
go build -o research/outputs/freeinference-replay ./research/freeinference_replay
research/outputs/freeinference-replay \
  --input research/outputs/glm52-pilot \
  --output research/outputs/glm52-replay-c8 \
  --concurrency 8 --corpus-cache research/outputs/agentx-cache
```

Each run directory contains:

- `metrics.json`: BLIS aggregate and per-request metrics, AgentX outcomes,
  per-replica time series, and catalog/kernel provenance.
- `run.json`: status, actual invocation, executable digest and Go build metadata,
  catalog/registry revisions, and artifact digests. Failed runs retain evidence.
- `experiment.yaml`: the original named experiment when `--config` is used.
- `workload.yaml`, `scenarios/*.yaml`, `defaults.yaml`: configuration snapshots.
- `conversion-manifest.json`: the exact input conversion manifest.
- `stdout.log`, `stderr.log`: simulator output and diagnostics.

Read per-request `phase` and `agentic` fields to distinguish warmup, family plays,
streams and outcomes. Measured-window counters and AgentX lifecycle counters have
different boundaries. Cancelled rows can have zero E2E placeholders; filter by
outcome before computing latency statistics. Neither tree completion counts nor
request latencies alone constitute a validated complete-family makespan metric.

The standard `./blis` binary cannot validate the research corpus alias. Use this
launcher; its alias registration does not modify BLIS source or dependency files.
The launcher tests cover provenance, overrides, rejection of corrupt inputs,
output preservation and recording failed BLIS runs:

```bash
go test ./research/freeinference_replay
go vet ./research/freeinference_replay
```
