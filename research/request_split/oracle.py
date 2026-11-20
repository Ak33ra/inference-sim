"""Price exact input/output shapes by serial native replay, without a latency formula."""

from copy import deepcopy
from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys

from research.policy_study.run import digest, write_json
from research.policy_study.analysis import read_json, require
from research.semiclosed.run import execute, prepare

PROBE_SHAPES = ((1, 1), (543, 189), (16383, 2), (16384, 2), (16385, 2),
                (32769, 100), (109756, 288), (2000, 1000), (48521, 16384))


def engine_fingerprint(config):
    serving = deepcopy(config["serving"])
    serving.pop("router", None)
    serving["cluster"].pop("replicas")
    return hashlib.sha256(json.dumps(serving, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def build_chain(shapes):
    return {"id": "oracle", "models": ["m"], "block_size": 64, "hash_id_scope": "local",
            "requests": [{"req_id": i, "parent_ids": [i-1] if i else [], "type": "n", "t": 0,
                          "api_time": 0, "model": "m", "in": prompt, "out": output, "hash_ids": []}
                         for i, (prompt, output) in enumerate(shapes)]}


def verify_oracle(shapes, work, metrics):
    require(work["complete"] and work["accounting_match"] and work["engine_available"], "oracle did not drain")
    rows = sorted(work["requests"], key=lambda row: row["step"])
    require(len(rows) == len(shapes), "oracle shape coverage differs")
    totals = {"completed_requests": len(shapes), "injected_requests": len(shapes),
              "total_input_tokens": sum(p for p, _ in shapes), "total_output_tokens": sum(o for _, o in shapes),
              "still_queued": 0, "still_running": 0, "preemption_count": 0, "cache_hit_rate": 0,
              "dropped_unservable": 0, "length_capped_requests": 0, "timed_out_requests": 0,
              "cancelled_requests": 0, "context_overflow_requests": 0, "failed_requests": 0}
    require(all(metrics[key] == value for key, value in totals.items()), "oracle engine accounting differs")
    engines = metrics["server_series"]["engines"]
    require(len(engines) == 1, "oracle is not single replica")
    series = engines[0]
    require(max(series["num_requests_running"]) <= 1 and max(series["num_requests_running_last"]) <= 1 and
            max(series["num_requests_waiting"]) == 0 and max(series["num_requests_waiting_last"]) == 0 and
            series["kv_cache_usage_last"][-1] == 0 and series["num_requests_running_last"][-1] == 0,
            "oracle overlapped or failed to release capacity")
    require(sum(series["prompt_tokens_local_compute"]) == totals["total_input_tokens"] and
            sum(series["prompt_tokens_local_cache_hit"]) == sum(series["prompt_tokens_external_kv_transfer"]) == 0,
            "oracle did not compute every prompt token")
    metric_rows = {r["requestID"]: r for r in metrics["requests"]}
    result = {}
    for i, (row, (prompt, output)) in enumerate(zip(rows, shapes)):
        require(row["step"] == i and row["prompt_tokens"] == prompt and row["output_tokens"] == output and
                row["state"] == "completed" and row["delay_us"] == 0, "oracle request differs from shape")
        require(row["arrival_us"] == (rows[i-1]["completion_us"] if i else 0), "oracle is not client-serial")
        require(row["prerequisites"] == ([{"request_id": rows[i-1]["request_id"], "on": "completion"}] if i else []),
                "oracle dependency chain differs")
        elapsed = row["completion_us"] - row["arrival_us"]
        require(elapsed > 0 and elapsed == row["e2e_us"] and metric_rows[row["request_id"]]["handled_by"] == "instance_0",
                "invalid oracle client elapsed time")
        key = f"{prompt}/{output}"
        require(key not in result, "duplicate calibrated shape")
        result[key] = {"input_tokens": prompt, "output_tokens": output, "service_us": elapsed,
                       "oracle_request_id": row["request_id"],
                       "preparation_us": round(metric_rows[row["request_id"]]["scheduling_delay_ms"] * 1000)}
    return result


def calibrate(directory, binary, base_config, shapes):
    os.environ["GIT_OPTIONAL_LOCKS"] = "0"
    directory.mkdir(parents=True)
    source = directory / "input.jsonl"
    source.write_text(json.dumps(build_chain(shapes), separators=(",", ":")) + "\n")
    config = deepcopy(base_config)
    config["trace"] = {"format": "weka-dag", "path": str(source)}
    config["serving"]["cluster"]["replicas"] = 1
    config["serving"]["router"] = {"research_policy": "jsq", "parameters": {"latency_us": 0}}
    config["run"] = {"seed": 42, "series_interval_s": 1,
                     "arrivals": {"mode": "fixed", "interval_s": 0, "seed": 42}}
    config["output"] = str(directory / "run")
    config["python"] = sys.executable
    config_path = directory / "config.json"
    write_json(config_path, config)
    plan = prepare(config_path)
    commands = list(plan.commands)
    commands[1] = (sys.executable, "-B", "-c", "import shutil,sys; shutil.copy2(sys.argv[1],sys.argv[2])",
                   str(binary), str(plan.output / "blis"))
    require(execute(replace(plan, commands=tuple(commands))) == 0, f"oracle replay failed: {directory}")
    shapes = verify_oracle(shapes, read_json(plan.output / "workload.json"), read_json(plan.output / "metrics.json"))
    log = (plan.output / "run.log").read_text()
    require("level=error" not in log and "level=fatal" not in log, "oracle logged an execution error")
    write_json(directory / "shapes.json", shapes)
    return shapes


def build_service_manifest(directory, binary, control, trace_sha256):
    base = read_json(Path(control["config"]))
    require(base["serving"]["cluster"]["replicas"] == 8 and
            base["serving"]["engine"]["enable_prefix_caching"] is False,
            "oracle requires the eight-replica cache-off control configuration")
    work_path = Path(control["output"]) / "workload.json"
    work = read_json(work_path)
    pairs = sorted({(r["prompt_tokens"], r["output_tokens"]) for r in work["requests"]})
    shapes = calibrate(directory / "all-shapes", binary, base, pairs)
    forward = calibrate(directory / "proof-forward", binary, base, PROBE_SHAPES)
    reverse = calibrate(directory / "proof-reverse", binary, base, tuple(reversed(PROBE_SHAPES)))
    singles = [calibrate(directory / f"proof-single-{i}", binary, base, [shape])
               for i, shape in enumerate(PROBE_SHAPES)]
    for sample in [reverse, *singles]:
        for key, shape in sample.items():
            require(shape["service_us"] == forward[key]["service_us"], "oracle service depends on calibration order")
    for key in shapes.keys() & forward.keys():
        require(shapes[key]["service_us"] == forward[key]["service_us"], "full oracle service differs from standalone")
    write_json(directory / "proof.json", {"exact_parity": True, "probe_shapes": PROBE_SHAPES,
                                          "unique_shapes": len(shapes), "client_service_us": {k: v["service_us"] for k, v in forward.items()}})
    requests = {r["request_id"]: {"trace_id": r["trace_id"], "session_id": r["session_id"],
                "runtime_session_id": r["trace_id"]+"/"+r["session_id"], "step": r["step"],
                "shape": f"{r['prompt_tokens']}/{r['output_tokens']}"} for r in work["requests"]}
    require(len(requests) == len(work["requests"]), "duplicate request IDs in control")
    files = {str(p): digest(p) for p in directory.rglob("*") if p.is_file()}
    manifest = {"format_version": 1, "trace_sha256": trace_sha256, "binary_sha256": digest(binary),
                "engine_sha256": engine_fingerprint(base),
                "reference_label": "exact isolated BLIS client service including preparation and output host service; no tool delay or queue wait",
                "source_hashes": {str(work_path): digest(work_path), **files}, "shapes": shapes, "requests": requests}
    return manifest, files
