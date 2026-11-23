"""Frozen request service references and independent central admission verification."""

from pathlib import Path

from research.policy_study.analysis import RunSpec, logical_work_signature, read_json, require, validate_run
from research.policy_study.run import check_hashes
from research.session_reservation.common import validate_manifest

SEEDS = tuple(range(42, 47))
POLICIES = ("jsq", "request-split")
TOTALS = {"families": 287, "sessions": 2385, "requests": 58741,
          "prompt_tokens": 722128759, "output_tokens": 10121700, "zero_output_requests": 0}
EDGES = 60551


def runtime_id(row):
    return row["trace_id"] + "/" + row["session_id"]


def verify_audit(audit, oracle, work, metrics):
    require(audit["format_version"] == 1 and audit["policy"] == "request-split", "invalid split audit")
    requests = {r["request_id"]: r for r in work["requests"]}
    engine = {r["requestID"]: r for r in metrics["requests"]}
    ready, prepared, claimed, completed, active = {}, {}, {}, set(), {}
    endpoints = audit["renderers"]
    require([r["instance"] for r in endpoints] == [f"instance_{i}" for i in range(8)], "renderer endpoint order differs")
    renderer_state = {}
    for endpoint in endpoints:
        require(endpoint["api_servers"] >= 1 and endpoint["workers"] >= 0, "invalid renderer configuration")
        renderer_state[endpoint["instance"]] = {
            "free": [[0] * endpoint["workers"] for _ in range(endpoint["api_servers"])],
            "next": 0, "rendered": 0, "wait_us": 0}
    last_at = -1
    max_batch = 0
    for event in audit["events"]:
        ident, at = event["request_id"], event["at_us"]
        require(at >= last_at, "audit time regressed")
        last_at = at
        ref, row = oracle["requests"][ident], requests[ident]
        shape = oracle["shapes"][ref["shape"]]
        require(event["session_id"] == runtime_id(row) == ref["runtime_session_id"] and
                event["service_us"] == shape["service_us"] and
                row["prompt_tokens"] == shape["input_tokens"] and row["output_tokens"] == shape["output_tokens"] and
                row["step"] == ref["step"] and event["preparation_service_us"] == shape["preparation_us"],
                "audit oracle identity differs")
        require(event["arrival_us"] == row["arrival_us"] and event["prepared_us"] >= event["arrival_us"],
                "audit changed causal arrival")
        kind = event["kind"]
        if kind == "prepare":
            require(ident not in prepared and at == row["arrival_us"], "duplicate or delayed preparation")
            endpoint = endpoints[len(prepared) % len(endpoints)]
            require(event["preparation_instance"] == endpoint["instance"], "preparation endpoint violates round robin")
            state = renderer_state[endpoint["instance"]]
            service, reached = event["preparation_service_us"], at + audit["router_latency_us"]
            require(service >= 0 and audit["router_latency_us"] >= 0, "negative preparation duration")
            start = reached
            if endpoint["workers"]:
                threads = state["free"][state["next"]]
                state["next"] = (state["next"] + 1) % len(state["free"])
                worker = min(range(len(threads)), key=threads.__getitem__)
                start = max(reached, threads[worker])
                threads[worker] = start + service
                state["rendered"] += 1
                state["wait_us"] += start - reached
            require(event["prepared_us"] == start + service and event["preparation_wait_us"] == start - reached,
                    "preparation differs from native renderer FIFO")
            prepared[ident] = event
        elif kind == "ready":
            require(ident in prepared and ident not in ready and ident not in claimed and at == event["prepared_us"],
                    "invalid ready publication")
            ready[ident] = event
            require(event["ready_count"] == len(ready), "ready queue count differs")
        elif kind == "claim":
            require(ident in ready and ident not in claimed and event["ready_count"] == len(ready), "claim lacks central ownership")
            largest = event["instance"] == "instance_0"
            require(event["largest_first"] == largest, "wrong server role")
            def key(item):
                item_id, entry = item
                return ((-1 if largest else 1) * entry["service_us"], entry["arrival_us"], item_id)
            require(min(ready.items(), key=key)[0] == ident, "claim violates request service priority")
            require(at >= event["prepared_us"], "claimed before preparation")
            metric = engine[ident]
            require(metric["handled_by"] == event["instance"] and
                    abs(metric["scheduling_delay_ms"] * 1000 - (at - row["arrival_us"])) < 1e-5,
                    "native placement/admission timestamp differs")
            require(abs(metric.get("gateway_queue_delay_ms", 0) * 1000 - (at-row["arrival_us"])) < 1e-5,
                    "central delay missing from native gateway metrics")
            capacity = event["capacity"]
            require(0 < capacity["batch_size"] <= 1024 and 0 < capacity["scheduled_tokens"] <= 16384 and
                    0 < capacity["used_kv_blocks"] <= capacity["promised_kv_blocks"] <= capacity["total_kv_blocks"],
                    "claim violates native capacity")
            max_batch = max(max_batch, capacity["batch_size"])
            del ready[ident]
            claimed[ident], active[ident] = event, ident
        elif kind == "complete":
            require(ident in claimed and ident not in completed and active.get(ident) == ident,
                    "completion lacks active ownership")
            require(at == row["completion_us"], "audit and replay completion differ")
            completed.add(ident)
            del active[ident]
        else:
            require(False, f"unknown split event {kind}")
    require(not ready and not active and prepared.keys() == claimed.keys() == completed == requests.keys(),
            "audit did not account for every request")
    for endpoint in endpoints:
        state = renderer_state[endpoint["instance"]]
        require(endpoint["rendered"] == state["rendered"] and endpoint["wait_us"] == state["wait_us"],
                "native renderer accounting differs")
    require(metrics["preemption_count"] == 0,
            "nonpreemption guarantee failed")
    require(max_batch > 1, "study did not exercise native batching")
    require(set(audit["reservations"]) == {f"instance_{i}" for i in range(8)}, "reservation endpoint coverage differs")
    for stats in audit["reservations"].values():
        require(stats["final_used_kv_blocks"] == stats["final_promised_blocks"] == 0 and 0 <= stats["peak_promised_blocks"] <= stats["total_kv_blocks"],
                "lifecycle promise leaked or exceeded capacity")
        require(stats["preemptions"] == stats["allocation_failures"] == 0, "native nonpreemption invariant failed")
    return {"claims": len(claimed), "maximum_batch_at_admission": max_batch,
            "reservation_admission_refusals": sum(r["admission_refusals"] for r in audit["reservations"].values()),
            "preparation_wait_us": sum(r["wait_us"] for r in endpoints), "renderers": endpoints,
            "reservations": audit["reservations"]}


def arrival_schedule(work):
    return sorted((r["trace_id"], r["birth_us"], r["arrival_us"]) for r in work["families"])


def spec(run):
    return RunSpec(run["id"], "loaded", run["router"], "fcfs", False, 4, run["arrival_seed"], 42,
                   Path(run["output"]), Path(run["config"]), run["config_sha256"])


def verify_run(run, study, classes):
    output = Path(run["output"])
    work, metrics, verification = (read_json(output / name) for name in ("workload.json", "metrics.json", "verification.json"))
    signature, edges = validate_run(spec(run), work, metrics, verification, TOTALS, classes["trace_sha256"], classes["families"])
    require(edges == EDGES, "dependency edge count differs")
    require({runtime_id(r) for r in work["sessions"]} == classes["sessions"].keys(), "session coverage differs")
    config = read_json(Path(run["config"]))
    require(read_json(output / "config.original.json") == config, "saved config differs")
    router = read_json(output / "config.resolved.json")["serving"]["router"]
    require(router["research_policy"] == run["router"] and router["parameters"] == config["serving"]["router"]["parameters"],
            "resolved router differs")
    control = read_json(Path(run["old_output"]) / "workload.json")
    require(arrival_schedule(work) == arrival_schedule(control) and signature == logical_work_signature(control["requests"])[0],
            "paired logical work or root arrivals differ")
    if run["router"] == "jsq":
        for name in ("workload.json", "metrics.json", "verification.json", "router-decisions.json"):
            actual, expected = read_json(output/name), read_json(Path(run["old_output"])/name)
            if name == "metrics.json":
                actual.get("catalog", {}).pop("path", None)
                expected.get("catalog", {}).pop("path", None)
            require(actual == expected, f"new JSQ differs from frozen control in {name}")
        result = {"old_jsq_parity": True}
    else:
        require(router["parameters"] == {"service_manifest": study["service_manifest"]}, "wrong service manifest")
        result = verify_audit(read_json(output/"router-decisions.json"), read_json(Path(study["service_manifest"])), work, metrics)
    return {"logical_work_sha256": signature, "dependency_edges": edges, "root_arrivals_match_old_control": True, **result}


def validate_provenance(study):
    check_hashes(study["frozen_files"])
    for info in study["dependencies"].values():
        root = Path(info["snapshot"])
        require({str(p.relative_to(root)) for p in root.rglob("*") if p.is_file()} == info["files"].keys(), "dependency file set changed")
        check_hashes({str(root / name): value for name, value in info["files"].items()})
    classes = read_json(Path(study["session_class_manifest"]))
    validate_manifest(classes)
    oracle = read_json(Path(study["service_manifest"]))
    require(oracle["trace_sha256"] == classes["trace_sha256"] == study["input_sha256"], "oracle trace differs")
    check_hashes(oracle["source_hashes"])
    from .oracle import engine_fingerprint
    require(oracle["binary_sha256"] == study["binary_sha256"], "oracle binary differs")
    for run in study["runs"]:
        require(engine_fingerprint(read_json(Path(run["config"]))) == oracle["engine_sha256"], "loaded engine differs from calibration")
    require(len(study["runs"]) == 10 and {(r["router"], r["arrival_seed"]) for r in study["runs"]} ==
            {(policy, seed) for policy in POLICIES for seed in SEEDS}, "paired run matrix differs")
