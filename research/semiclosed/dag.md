# Run an explicit-DAG Weka trace

From the repository root, run the natural Oolong trace with one root workload arriving each second:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/natural-oolong.json
```

The config uses GPT-OSS-120B on one B200 with TP1. Its engine settings match
`gpt-oss-120b-b200-mxfp4-vllm-tp1.yaml`. This is a starting configuration for
policy experiments, not a latency calibration. It uses one GPU per node in the
generated topology because this experiment allocates one serving replica.

Compressed inputs require `zstd` on PATH. Plain JSONL also works. The DAG path
does not require PyArrow or FreeInference Parquet files.

For another run, supply a fresh output directory:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/natural-oolong.json \
  --output research/outputs/semiclosed/natural-oolong-repeat
```

The runner saves a copy of the input, the resolved config, the generated scenario,
`workload.json`, `metrics.json`, and `verification.json`. Verify saved outputs with:

```bash
research/scripts/semi_closed.sh verify research/outputs/semiclosed/natural-oolong-fixed-1s
```

Edit `trace.path` to select another file. Config paths are relative to the config
file. Edit `serving.model`, `serving.cluster`, and `serving.engine` to change the
deployment. Each input must contain a single source model; the serving model can
have a different catalog name. All input requests run exactly once.

Edit `run.arrivals` to select the arrival process described in the
[format reference](dag-format.md). The starter config uses fixed spacing because
it provides a repeatable baseline. Decreasing the interval raises offered load.
Concurrency emerges from arrival rate and workload duration; there is no fixed
session cap or recycling.

Use Poisson arrivals for a study with irregular arrival gaps:

```json
"arrivals": {"mode": "poisson", "rate_per_s": 1, "seed": 42}
```

Use the simulator directly with an existing scenario:

```bash
source .inference-sim-env/activate
go build -o /tmp/blis .
/tmp/blis replay \
  --scenario gpt-oss-120b-b200-mxfp4-vllm-tp1.yaml --scenarios . \
  --num-instances 1 \
  --semi-closed-input natural-oolong-fanout.jsonl.zst \
  --semi-closed-arrivals poisson --semi-closed-arrival-rate 1 \
  --semi-closed-arrival-seed 42 \
  --semi-closed-report /tmp/oolong-workload.json --metrics-path /tmp/oolong-metrics.json
```

The existing FreeInference conversion-directory path and AgentX defaults remain
available. AgentX does not yet consume the new explicit parent fields; its graph
reconstruction still uses its existing rules.

## Overlap traces and add users

Run two Commit0 fanout buckets with two copies of each source:

```bash
research/scripts/semi_closed.sh run research/configs/semiclosed/commit0-overlap.json
```

This example serves GPT-OSS-120B on four independent B200 TP1 replicas with the
default router and prefix caching off. It schedules one root family per second.
The two selected buckets contain six families. Two copies produce 12 independent
users, each with its complete child-session DAG.

Replace `trace.path` with `trace.sources` to select files and copy counts:

```json
"trace": {
  "format": "weka-dag",
  "sources": [
    {"path": "../../../traces/commit0-fanout-buckets/w25-29/traces.jsonl.zst", "copies": 2},
    {"path": "../../../traces/commit0-fanout-buckets/w30-34/traces.jsonl.zst", "copies": 2}
  ]
}
```

Paths are relative to the config. Each file can contain many root families.
Omit `copies` to use each family once. List any of the eight Commit0 bucket files,
or mix these with Oolong. All selected files must use the same source model.

Set `run.arrivals` to control overlap:

- Use `{"mode":"fixed","interval_s":0}` to start every selected user together.
- Use `{"mode":"fixed","interval_s":0.5}` to introduce two users per second.
- Use `{"mode":"poisson","rate_per_s":2,"seed":42}` for an average of two users per second with irregular gaps.
- Use `{"mode":"trace"}` to retain each family's recorded root time. Copies of the same family then start together.

Fixed and Poisson rates apply to the combined workload. Increasing `copies` adds
users and total work. To raise the arrival rate too, decrease `interval_s` or
increase `rate_per_s`. Concurrency depends on both arrivals and family duration.
Every selected user drains once, with no recycling or fixed population cap.

The mixer interleaves families across sources and copies so different buckets
start near each other. It drains longer files after shorter files end. Requests
within each family keep their original tokens, timestamps, and dependency edges.

Inspect `composition.json` to map each reported family to its original file,
family ID, and copy index. The run also saves the composed `input.jsonl`. Check
both composition and replay with:

```bash
research/scripts/semi_closed.sh verify research/outputs/semiclosed/commit0-overlap
```

Full verification reads the original source files and checks their saved hashes.
Keep those files at their recorded paths. The prepared `input.jsonl` remains a
standalone input for native semi-closed replay.
