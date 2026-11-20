"""Shared identity, reference, and provenance checks for the reservation study."""

from pathlib import Path

from research.policy_study.analysis import (
    RunSpec, logical_work_signature, read_json, require, validate_run,
)
from research.policy_study.run import check_hashes, digest

SEEDS = tuple(range(42, 47))
POLICIES = ("jsq", "session-reservation")
RESERVED = ["instance_0", "instance_1"]
TOTALS = {"families": 287, "sessions": 2385, "requests": 58741,
          "prompt_tokens": 722128759, "output_tokens": 10121700,
          "zero_output_requests": 0}
EDGES = 60551
METRIC = "minimum_session_elapsed_us_across_cache_off_isolated_family_runs"
TIE_POLICY = "reference_elapsed_us_then_runtime_session_id"
REFERENCE_LABEL = "best-observed isolated-family session reference; siblings still compete"


def runtime_id(row: dict) -> str:
    require(bool(row["trace_id"]) and bool(row["session_id"]), "empty session identity")
    return row["trace_id"] + "/" + row["session_id"]


def reference_manifest(runs, trace_hash: str, short_count: int) -> dict:
    sessions, families = {}, {}
    expected_sessions = expected_families = None
    reference_count = 0
    for run_id, work in runs:
        reference_count += 1
        current_sessions, current_families = set(), set()
        for row in work["sessions"]:
            key = runtime_id(row)
            require(key not in current_sessions, "duplicate runtime session identity")
            current_sessions.add(key)
            duration = row["completion_us"] - row["arrival_us"]
            require(duration > 0, "nonpositive session reference duration")
            previous = sessions.get(key)
            if previous is None or (duration, run_id) < (previous["reference_elapsed_us"], previous["reference_run"]):
                sessions[key] = {"trace_id": row["trace_id"], "session_id": row["session_id"],
                                 "reference_elapsed_us": duration, "reference_run": run_id}
        for row in work["families"]:
            key, duration = row["trace_id"], row["makespan_us"]
            require(key not in current_families and duration > 0, "invalid family reference")
            current_families.add(key)
            previous = families.get(key)
            if previous is None or (duration, run_id) < (previous["reference_elapsed_us"], previous["reference_run"]):
                families[key] = {"reference_elapsed_us": duration, "reference_run": run_id}
        if expected_sessions is not None:
            require(current_sessions == expected_sessions and current_families == expected_families,
                    "reference run object coverage differs")
        expected_sessions, expected_families = current_sessions, current_families
    require(0 < short_count < len(sessions), "invalid short count")
    ordered = sorted(sessions, key=lambda key: (sessions[key]["reference_elapsed_us"], key))
    for index, key in enumerate(ordered):
        sessions[key]["class"] = "short" if index < short_count else "long"
    return {"format_version": 1, "trace_sha256": trace_hash,
            "ranking": {"metric": METRIC, "reference_label": REFERENCE_LABEL,
                        "reference_run_count": reference_count, "short_count": short_count,
                        "long_count": len(sessions) - short_count, "tie_policy": TIE_POLICY},
            "sessions": {key: sessions[key] for key in sorted(sessions)},
            "families": {key: families[key] for key in sorted(families)}}


def validate_manifest(manifest: dict) -> None:
    require(manifest["format_version"] == 1, "unsupported class manifest version")
    rank = manifest["ranking"]
    require(rank["metric"] == METRIC and rank["tie_policy"] == TIE_POLICY,
            "reference ranking contract differs")
    require(rank["reference_run_count"] == 35 and rank["short_count"] == 954
            and rank["long_count"] == 1431, "reference ranking counts differ")
    sessions = manifest["sessions"]
    require(len(sessions) == 2385 and len(manifest["families"]) == 287, "manifest coverage differs")
    for index, key in enumerate(sorted(sessions, key=lambda key: (sessions[key]["reference_elapsed_us"], key))):
        row = sessions[key]
        require(runtime_id(row) == key and row["reference_elapsed_us"] > 0 and row["reference_run"],
                f"invalid session reference {key}")
        require(row["class"] == ("short" if index < 954 else "long"), f"class/reference disagreement for {key}")


def spec(run: dict) -> RunSpec:
    return RunSpec(run["id"], "loaded", run["router"], "fcfs", False, 4,
                   run["arrival_seed"], 42, Path(run["output"]), Path(run["config"]), run["config_sha256"])


def arrival_schedule(work: dict) -> list:
    return sorted((row["trace_id"], row["birth_us"], row["arrival_us"])
                  for row in work["families"])


def assert_baseline_parity(output: Path, old_output: Path) -> None:
    for name in ("workload.json", "router-decisions.json", "verification.json", "metrics.json"):
        actual, expected = read_json(output / name), read_json(old_output / name)
        if name == "metrics.json":
            actual.get("catalog", {}).pop("path", None)
            expected.get("catalog", {}).pop("path", None)
        require(actual == expected, f"new JSQ differs from frozen control in {name}")


def verify_run(run: dict, study: dict, classes: dict) -> dict:
    output = Path(run["output"])
    config = read_json(Path(run["config"]))
    require(read_json(output / "config.original.json") == config, "saved original config differs")
    resolved = read_json(output / "config.resolved.json")
    router = resolved["serving"]["router"]
    require(router["research_policy"] == run["router"] and
            router["parameters"] == config["serving"]["router"]["parameters"], "resolved router differs")
    require(digest(output / "input.jsonl.zst") == digest(Path(config["trace"]["path"])),
            "run input differs from frozen trace")
    if run["router"] == "session-reservation":
        require(router["parameters"]["session_class_manifest"] == study["session_class_manifest"] and
                router["parameters"]["reserved_instances"] == RESERVED, "run uses a different class manifest")
    work, metrics, verification, decisions = [read_json(output / name) for name in
        ("workload.json", "metrics.json", "verification.json", "router-decisions.json")]
    signature, edges = validate_run(spec(run), work, metrics, verification, TOTALS,
                                   classes["trace_sha256"], classes["families"])
    require(edges == EDGES, "dependency edge count differs")
    sessions = {runtime_id(row): row for row in work["sessions"]}
    require(len(sessions) == len(work["sessions"]) and sessions.keys() == classes["sessions"].keys(),
            "loaded session coverage differs from manifest")
    require(all(row["completion_us"] > row["arrival_us"] and set(row["states"]) == {"completed"}
                for row in sessions.values()), "unfinished session")
    by_request = {row["request_id"]: row for row in work["requests"]}
    audit = {row["request_id"]: row for row in decisions["decisions"]}
    require(len(audit) == len(decisions["decisions"]) and audit.keys() == by_request.keys(),
            "router decision coverage differs")
    engine = {row["requestID"]: row for row in metrics["requests"]}
    cohort_accounting = {kind: {"sessions": sum(r["class"] == kind for r in classes["sessions"].values()),
                               "requests": 0, "prompt_tokens": 0, "output_tokens": 0}
                         for kind in ("short", "long")}
    for request_id, row in audit.items():
        key = runtime_id(by_request[request_id])
        require(key in classes["sessions"], "request session absent from class manifest")
        kind = classes["sessions"][key]["class"]
        accounting = cohort_accounting[kind]
        accounting["requests"] += 1
        for token_field in ("prompt_tokens", "output_tokens"):
            accounting[token_field] += by_request[request_id][token_field]
        require(engine[request_id]["session_id"] == key, "engine runtime session namespace differs")
        require(row["policy"] == run["router"], "decision router differs")
        if run["router"] == "session-reservation":
            require(row.get("session_id") == key and row.get("session_class") == kind,
                    "request class or runtime namespace differs")
            require(kind == "short" or row["instance"] not in RESERVED, "long request used reserved endpoint")
            require(row["mode"] == "session-reservation-" + kind, "decision mode differs")
        require(row["instance"] == engine[request_id]["handled_by"], "audit and engine placement differ")
    require(cohort_accounting["short"] == {"sessions": 954, "requests": 8252,
            "prompt_tokens": 91145173, "output_tokens": 1098502}, "short cohort work differs")
    control = read_json(Path(run["old_output"]) / "workload.json")
    require(arrival_schedule(work) == arrival_schedule(control), "paired root arrival schedules differ")
    require(signature == logical_work_signature(control["requests"])[0], "paired logical work differs")
    if run["router"] == "jsq":
        assert_baseline_parity(output, Path(run["old_output"]))
    return {"logical_work_sha256": signature, "dependency_edges": edges,
            "sessions": len(sessions), "requests": len(audit),
            "root_arrivals_match_old_control": True, "routing_verified": True,
            "old_jsq_parity": run["router"] == "jsq", "cohort_accounting": cohort_accounting}


def validate_provenance(study: dict) -> None:
    check_hashes(study["frozen_files"])
    for info in study["dependencies"].values():
        root = Path(info["snapshot"])
        actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()}
        require(actual == info["files"].keys(), f"dependency file set changed: {root}")
        check_hashes({str(root / name): expected for name, expected in info["files"].items()})
    classes = read_json(Path(study["session_class_manifest"]))
    validate_manifest(classes)
    require(classes["trace_sha256"] == study["input_sha256"], "manifest trace hash differs")
    require(len(study["runs"]) == 10 and {(r["router"], r["arrival_seed"]) for r in study["runs"]}
            == {(router, seed) for router in POLICIES for seed in SEEDS}, "paired run matrix differs")
    for run in study["runs"]:
        check_hashes({run["config"]: run["config_sha256"],
                      run["preparation_config"]: run["preparation_config_sha256"]})
