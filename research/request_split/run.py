"""Freeze and run ten paired cache-off JSQ/request-split experiments with native isolated-service calibration."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
import fcntl
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time

from research.policy_study.run import (
    completed, digest, validate_provenance as validate_old_provenance, write_json,
)
from research.semiclosed.run import REPOSITORY, execute, prepare
from .common import (
    EDGES, POLICIES, SEEDS, TOTALS, check_hashes, read_json, require,
    validate_manifest, validate_provenance, verify_run,
)

from .oracle import build_service_manifest

ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}
ARTIFACTS = ("metrics.json", "workload.json", "verification.json", "scenario.yaml",
             "config.resolved.json", "config.original.json", "commands.json",
             "commands.log", "router.json", "router-decisions.json", "split-verification.json",
             "input.jsonl.zst", "blis")


def initialize(study: Path, old_study: Path, binary: Path, reference_study: Path) -> None:
    require(not study.exists(), f"new output directory already exists: {study}")
    old = read_json(old_study / "study.json")
    validate_old_provenance(old_study, old)
    require(old["source_totals"] == TOTALS and old["dependency_edges"] == EDGES,
            "old study workload differs from experiment contract")
    isolated = sorted((r for r in old["runs"] if r["kind"] == "isolated" and not r["prefix_caching"]),
                      key=lambda r: r["id"])
    controls = {r["arrival_seed"]: r for r in old["runs"] if r["kind"] == "loaded"
                and r["router"] == "jsq" and r["scheduler"] == "fcfs"
                and not r["prefix_caching"] and r["mean_interarrival_s"] == 4}
    require(len(isolated) == 35 and set(controls) == set(SEEDS), "old reference/control matrix differs")
    frozen_files = {str(old_study / "study.json"): digest(old_study / "study.json")}
    for run in [*isolated, *controls.values()]:
        require(completed(run, old["binary_sha256"]), f"old run is not verified: {run['id']}")
        output = Path(run["output"])
        status = read_json(output / "study-status.json")
        frozen_files[str(output / "study-status.json")] = digest(output / "study-status.json")
        frozen_files.update({str(output / name): value for name, value in status["artifacts"].items()})
        frozen_files[run["config"]] = run["config_sha256"]
    reference = read_json(reference_study / "study.json")
    reference_path = Path(reference["session_class_manifest"])
    check_hashes({str(reference_path): reference["session_class_manifest_sha256"]})
    classes = read_json(reference_path)
    validate_manifest(classes)
    require(classes["trace_sha256"] == old["input_sha256"], "reference and control traces differ")
    for directory in ("bin", "configs", "logs", "provenance"):
        (study / directory).mkdir(parents=True, exist_ok=True)
    frozen_binary = study / "bin/blis"
    shutil.copy2(binary, frozen_binary)
    manifest_path = study / "provenance/session-classes.json"
    write_json(manifest_path, classes)
    service_path = study / "provenance/service.json"
    oracle, calibration_files = build_service_manifest(study / "oracle", frozen_binary, controls[SEEDS[0]], classes["trace_sha256"])
    write_json(service_path, oracle)
    frozen_files.update(calibration_files)
    runs = []
    for seed in SEEDS:
        for router in POLICIES:
            old_run = controls[seed]
            config = read_json(Path(old_run["config"]))
            require(config["run"]["seed"] == 42 and config["run"]["arrivals"] ==
                    {"mode": "poisson", "rate_per_s": .25, "seed": seed}, "old arrival contract differs")
            ident = f"loaded-cacheoff-{router}-fcfs-4s-seed{seed}"
            config["output"] = str(study / "runs" / ident)
            config["python"] = sys.executable
            config_path = study / "configs" / f"{ident}.json"
            base_path = config_path
            if router == "request-split":
                base_path = study / "configs" / f"{ident}.prepare.json"
                write_json(base_path, config)
                config["serving"]["router"] = {"research_policy": router, "parameters": {
                    "service_manifest": str(service_path)}}
            write_json(config_path, config)
            runs.append({"id": ident, "router": router, "arrival_seed": seed, "simulation_seed": 42,
                         "output": config["output"], "config": str(config_path),
                         "config_sha256": digest(config_path), "preparation_config": str(base_path),
                         "preparation_config_sha256": digest(base_path), "old_output": old_run["output"]})
    files = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"],
                                    cwd=REPOSITORY, env=ENV).split(b"\0")
    archive_path = study / "provenance/source.tar.gz"
    with tarfile.open(archive_path, "w:gz") as archive:
        for name in sorted(set(os.fsdecode(name) for name in files if name)):
            path = REPOSITORY / name
            if path.is_file() and not path.is_relative_to(study) and not name.startswith(".audit/"):
                archive.add(path, arcname=name, recursive=False)
    diff = study / "provenance/source.diff"
    diff.write_bytes(subprocess.check_output(["git", "diff", "HEAD"], cwd=REPOSITORY, env=ENV))
    helpers = [*sorted((REPOSITORY / "research/semiclosed").glob("*.py")),
               *sorted((REPOSITORY / "research/request_split").glob("*.py")),
               REPOSITORY / "research/policy_study/run.py", REPOSITORY / "research/policy_study/analysis.py",
               REPOSITORY / "research/session_reservation/common.py"]
    frozen_files.update({str(path): digest(path) for path in [frozen_binary, manifest_path, service_path, archive_path, diff, *helpers]})
    frozen_files.update(old["provenance_files"])
    for run in runs:
        frozen_files[run["config"]] = run["config_sha256"]
        frozen_files[run["preparation_config"]] = run["preparation_config_sha256"]
    write_json(study / "study.json", {
        "format_version": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
        "old_study": str(old_study), "binary": str(frozen_binary), "binary_sha256": digest(frozen_binary),
        "source_archive_sha256": digest(archive_path), "input_sha256": old["input_sha256"],
        "source_totals": TOTALS, "dependency_edges": EDGES,
        "session_class_manifest": str(manifest_path), "session_class_manifest_sha256": digest(manifest_path),
        "service_manifest": str(service_path), "service_manifest_sha256": digest(service_path),
        "frozen_files": frozen_files, "dependencies": old["dependencies"], "runs": runs,
        "reference": "Shared minima over 35 old cache-off isolated-family runs; siblings compete. Ratios below one remain valid.",
        "summary": "Median and min-max across five per-run mean/p50/p99 statistics; linear quantiles; paired seed deltas.",
    })
    print(f"Frozen 10 paired runs and 2385 session references in {study}", flush=True)


def adapted_plan(run: dict):
    plan = prepare(Path(run["preparation_config"]))
    original = Path(run["config"]).read_bytes()
    actual = json.loads(original)
    resolved = json.loads(json.dumps(plan.resolved))
    resolved["config_path"] = run["config"]
    commands = list(plan.commands)
    router_text = plan.research_router_config
    if run["router"] == "request-split":
        base = read_json(Path(run["preparation_config"]))
        expected = json.loads(json.dumps(actual))
        expected["serving"]["router"] = {"research_policy": "jsq", "parameters": {}}
        require(base == expected, "preparation config differs beyond the explicit router adapter")
        params = actual["serving"]["router"]["parameters"]
        require(set(params) == {"service_manifest"} and params["service_manifest"], "split adapter parameters differ")
        replay = list(commands[2])
        index = replay.index("--research-router") + 1
        require(replay[index] == "jsq", "adapter requires a standard JSQ base")
        replay[index] = "request-split"
        commands[2] = tuple(replay)
        router_text = json.dumps(params, indent=2) + "\n"
        resolved["serving"]["router"] = {"research_policy": run["router"], "parameters": params,
                                          "saved_config": str(plan.output / "router.json")}
    return replace(plan, original=original, resolved=resolved, commands=tuple(commands),
                   research_router_config=router_text)


def one(study: dict, run: dict) -> int:
    os.environ["GIT_OPTIONAL_LOCKS"] = "0"
    plan = adapted_plan(run)
    commands = list(plan.commands)
    commands[1] = (sys.executable, "-B", "-c", "import shutil,sys; shutil.copy2(sys.argv[1],sys.argv[2])",
                   study["binary"], str(plan.output / "blis"))
    return execute(replace(plan, commands=tuple(commands)))


def run_one(study: dict, run: dict, retry_failed: bool) -> dict:
    check_hashes({run["config"]: run["config_sha256"],
                  run["preparation_config"]: run["preparation_config_sha256"],
                  study["binary"]: study["binary_sha256"],
                  study["session_class_manifest"]: study["session_class_manifest_sha256"],
                  study["service_manifest"]: study["service_manifest_sha256"]})
    output = Path(run["output"])
    if completed(run, study["binary_sha256"]):
        return {"id": run["id"], "status": "reused"}
    if output.exists():
        require(retry_failed, f"incomplete output retained at {output}; inspect then use --retry-failed")
        archive = Path(study["directory"]) / "failed" / f"{run['id']}-{time.time_ns()}"
        archive.parent.mkdir(exist_ok=True)
        output.rename(archive)
    start = time.monotonic()
    log_path = Path(study["directory"]) / "logs" / f"{run['id']}.log"
    with log_path.open("w") as log:
        process = subprocess.run([sys.executable, "-B", "-m", "research.request_split.run", "_one",
                                  study["directory"], run["id"]], cwd=REPOSITORY, env=ENV,
                                 stdout=log, stderr=subprocess.STDOUT)
    require(process.returncode == 0, f"{run['id']} failed with {process.returncode}; see {log_path}")
    verified = verify_run(run, study, read_json(Path(study["session_class_manifest"])))
    write_json(output / "split-verification.json", verified)
    saved = {"verified": True, "binary_sha256": study["binary_sha256"],
             "config_sha256": run["config_sha256"], "elapsed_s": time.monotonic() - start,
             "artifacts": {name: digest(output / name) for name in ARTIFACTS}}
    write_json(output / "study-status.json", saved)
    return {"id": run["id"], "status": "verified", "elapsed_s": saved["elapsed_s"]}


def run_study(args) -> int:
    path = args.study.resolve()
    study = read_json(path / "study.json")
    validate_provenance(study)
    study["directory"] = str(path)
    selected = [r for r in study["runs"] if not args.match or args.match in r["id"]]
    if args.limit:
        selected = selected[:args.limit]
    require(bool(selected), "no runs selected")
    failures = []
    with (path / "runner.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(run_one, study, run, args.retry_failed): run for run in selected}
            for index, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                except Exception as error:
                    result = {"id": futures[future]["id"], "error": str(error)}
                    failures.append(result)
                print(f"[{index}/{len(selected)}] {json.dumps(result)}", flush=True)
    write_json(path / "last-invocation.json", {"selected": len(selected), "failures": failures})
    return int(bool(failures))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("study", type=Path)
    init.add_argument("--old-study", type=Path, required=True)
    init.add_argument("--binary", type=Path, required=True)
    init.add_argument("--reference-study", type=Path, default=REPOSITORY / "research/outputs/mega-session-reservation-4s")
    run = commands.add_parser("run")
    run.add_argument("study", type=Path)
    run.add_argument("--jobs", type=int, default=2)
    run.add_argument("--match")
    run.add_argument("--limit", type=int)
    run.add_argument("--retry-failed", action="store_true")
    worker = commands.add_parser("_one")
    worker.add_argument("study", type=Path)
    worker.add_argument("run_id")
    args = parser.parse_args()
    try:
        if args.command == "init":
            initialize(args.study.resolve(), args.old_study.resolve(), args.binary.resolve(), args.reference_study.resolve())
            return 0
        if args.command == "_one":
            study = read_json(args.study / "study.json")
            run = next(r for r in study["runs"] if r["id"] == args.run_id)
            return one(study, run)
        require(args.jobs > 0 and (args.limit is None or args.limit > 0), "jobs and limit must be positive")
        return run_study(args)
    except (OSError, ValueError, KeyError) as error:
        print(f"request split: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
