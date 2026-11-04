# Research experiment configurations

Named experiments bind a converted trace, a serving cluster and AgentX settings.
The current configurations are reproducible starting points; load calibration is
pending. Each run snapshots its configuration and conversion manifest; the corpus
is referenced by path and digest rather than copied into every run directory.

From the repository root:

```bash
source .inference-sim-env/activate
go run ./research/freeinference_replay \
  --config research/configs/experiments/glm52-2r-smoke.yaml \
  --output research/outputs/glm52-config-smoke
```

The output directory must be new. The referenced converted pilot must already
exist; the launcher checks its SHA-256 against both the experiment and its
conversion manifest. It does not repeat conversion or select raw Parquet rows.

## Configuration layers

| Layer | Controls | Where to change it |
| --- | --- | --- |
| Experiment | Converted input path and digest, active replicas, family concurrency, seed, duration and metric sampling | `experiments/*.yaml` |
| Serving cluster | Model, GPU type/count, fabric, parallelism, engine limits, cache allocation, offload and router configuration | `clusters/*.yaml` |
| Replay behavior | Warmup, sampled starts, family reconstruction, idle compression, grace period and context filtering | `workloads/agentx.yaml` |
| Trace selection | Source week/model/root window, included families, request tokens, prefixes and timing | Converter arguments and the input `manifest.json` |
| Physical cost models | Model/hardware descriptions and latency coefficients named by the cluster | Catalog and registry selected by the environment or launcher flags |

The trace's model label controls dataset selection; `Scenario.model` chooses the
simulated serving model. These experiments serve the `glm-5.2` trace on
`glm-5.2-fp8`. The copied `defaults.yaml` supplies LoRA settings and is inactive
here because no adapters are configured.

Paths inside an experiment are relative to that file. Command-line paths are
relative to the working directory. Explicit flags override experiment values;
experiment load/seed/duration values override the shared workload. The effective
workload and actual BLIS command are saved beside the original experiment file.
An `--input` override relocates the pinned corpus; selecting different contents
requires updating `corpus_sha256` in a new experiment.

## Available experiments

| Experiment | Replicas | Total B200s | Family lanes | Purpose |
| --- | ---: | ---: | ---: | --- |
| `glm52-2r-smoke.yaml` | 2 | 16 | 1 | Wiring and reproducibility check |
| `glm52-1r-c32.yaml` | 1 | 8 | 32 | Candidate replica comparison |
| `glm52-2r-c32.yaml` | 2 | 16 | 32 | Candidate replica comparison |
| `glm52-4r-c32.yaml` | 4 | 32 | 32 | Candidate replica comparison |

All use the same GLM-5.2 pilot corpus, seed 42, a 900-second profiling window,
30-second grace period and one-second server metrics bins. The c32 candidates
hold family concurrency fixed across replica counts; they do not hold utilization
or realized request throughput fixed. No candidate is designated moderate load
or saturation yet.

## Controlling the cluster

Each cluster file contains a BLIS `Scenario` followed by a `Deployment`:

- `Scenario.model`, `engine_version` and `coefficients` choose what is served and
  the cost model used to predict execution time.
- `Scenario.cluster.hardware`, `nodes`, `gpus_per_node` and `fabric` describe the
  physical inventory and inter-node network.
- `Deployment.pools[].nodes` allocates nodes to a serving pool. `parallel.tp`,
  `pp`, `dp` and `enable_expert_parallel` describe its placement.
- `engine.max_num_seqs` caps active sequences **per engine**;
  `max_num_batched_tokens` sets the token budget per scheduling step.
- `engine.max_model_len` limits a request's context. `gpu_memory_utilization`
  specifies a memory budget fraction, not a target GPU compute utilization.
- `cache_dtype`, `block_size` and `enable_prefix_caching` control KV storage.
  The baseline has no `offload` block, so KV storage is GPU-only. BLIS derives
  available KV blocks from model weights, hardware memory and engine settings.

The current files use TP8/PP1/DP1 on eight GPUs per node: one replica per node.
The experiment's `num_instances` is how many replicas BLIS actually instantiates;
the cluster file supplies their placement capacity. The supplied experiments
match both counts. Change the cluster file and `num_instances` together for a
replica comparison; changing `concurrency` changes workload pressure instead.

Router inputs belong in `Deployment.router`; an omitted block uses the pinned
llm-d router's defaults. Engine scheduling belongs in
`pools[].engine.scheduling_policy`. The current baseline resolves to FCFS with
tail preemption. Configurations referencing separate router files need additional
snapshot support in this launcher before use; current cluster files are
self-contained. Offload and disaggregated serving experiments also need their
own explicit deployment definitions and verification.

## Reproducibility and later calibration

`experiment.yaml` records the selected experiment, `workload.yaml` its resolved
replay settings, `scenarios/*.yaml` and `defaults.yaml` the serving/configuration
snapshots, and `conversion-manifest.json` the exact trace conversion manifest.
`run.json` records the executable digest, Go dependency versions, invocation,
catalog/registry revisions and artifact hashes. `metrics.json` carries resolved
kernel provenance and per-request outcomes. Matching configuration files also
requires matching code, catalog and registry versions; the local activation file
selects the current pinned environment.

For calibration later, keep the trace, cluster, seed policy and replay semantics
fixed while varying family concurrency. Inspect queueing, latency, throughput,
KV occupancy, preemption and unfinished work before naming light/moderate/stress
profiles. Use several seeds and sufficient duration to establish branch coverage.
AgentX selects roots in corpus order, so the short c32 pilot can miss later
fan-out families; increasing concurrency alone does not guarantee coverage.
