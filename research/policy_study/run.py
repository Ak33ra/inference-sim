"""Freeze, run and resume the paired eight-B200 mega-trace policy study."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import replace
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import time

from research.semiclosed.run import REPOSITORY, execute, prepare


CONDITIONS = {
    False: [("llmd-jsq", "fcfs"), ("jsq", "fcfs"), ("least-token", "fcfs"),
            ("random", "fcfs"), ("jsq", "sjf"), ("jsq", "mlfq"), ("jsq", "atlas")],
    True: [("llmd-jsq", "fcfs"), ("jsq", "fcfs"), ("least-token", "fcfs"),
           ("random", "fcfs"), ("agentix", "fcfs"), ("preble-e2", "fcfs"),
           ("jsq", "atlas"), ("agentix", "atlas")],
}
SEEDS = tuple(range(42, 47))


def digest(path: Path) -> str:
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def write_json(path: Path, value: object) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
    temporary.replace(path)


def snapshot_tree(source: Path, target: Path) -> dict:
    shutil.copytree(source, target)
    files = {str(path.relative_to(target)): digest(path)
             for path in sorted(target.rglob("*")) if path.is_file()}
    return {"source": str(source), "snapshot": str(target), "files": files}


def helper_sources() -> dict:
    paths = [Path(__file__).resolve(), *sorted((REPOSITORY / "research/semiclosed").glob("*.py"))]
    return {str(path): digest(path) for path in paths}


def check_hashes(files: dict) -> None:
    for path, expected in files.items():
        if digest(Path(path)) != expected:
            raise ValueError(f"frozen provenance changed: {path}")


def validate_provenance(study: Path, manifest: dict) -> None:
    files = {manifest["binary"]: manifest["binary_sha256"],
             str(study / "provenance/input.jsonl.zst"): manifest["input_compressed_sha256"],
             str(study / "provenance/source.tar.gz"): manifest["source_archive_sha256"],
             **manifest["helper_sources"], **manifest["provenance_files"]}
    for info in manifest["dependencies"].values():
        root = Path(info["snapshot"])
        actual = {str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()}
        if actual != info["files"].keys():
            raise ValueError(f"frozen dependency file set changed: {root}")
        files.update({str(root / name): expected for name, expected in info["files"].items()})
    check_hashes(files)


def initialize(study: Path, binary: Path, calibration: Path) -> None:
    if (study / "study.json").exists():
        raise ValueError("study.json already exists; use run to resume the frozen study")
    study.mkdir(parents=True, exist_ok=True)
    for directory in ("bin", "configs", "logs", "provenance"):
        (study / directory).mkdir(exist_ok=True)
    source = REPOSITORY / "traces/mega-commit0-oolong"
    trace_manifest = json.loads((source / "manifest.json").read_text())
    parameters = json.loads(calibration.read_text())["parameters"]
    frozen_binary = study / "bin/blis"
    if frozen_binary.exists():
        raise ValueError(f"frozen binary already exists: {frozen_binary}")
    shutil.copy2(binary, frozen_binary)
    shutil.copy2(calibration, study / "provenance/router-calibration.json")
    trace = study / "provenance/input.jsonl.zst"
    shutil.copy2(source / "input.jsonl.zst", trace)
    for name in ("manifest.json", "composition.json", "verification.json"):
        shutil.copy2(source / name, study / "provenance" / name)
    dependencies = {}
    for name, variable in (("catalog", "BLIS_CATALOG"), ("registry", "BLIS_REGISTRY")):
        dependencies[name] = snapshot_tree(Path(os.environ[variable]).resolve(), study / "provenance" / name)
    shutil.copy2(REPOSITORY / "defaults.yaml", study / "provenance/defaults.yaml")
    shutil.copy2(REPOSITORY / "research/configs/routers/jsq.yaml", study / "provenance/llmd-jsq.yaml")
    base = json.loads((REPOSITORY / "research/configs/semiclosed/mega-8b200-8s.json").read_text())
    base.update({name: info["snapshot"] for name, info in dependencies.items()})
    base["defaults"] = str(study / "provenance/defaults.yaml")
    base["trace"]["path"] = str(trace)
    base["python"] = sys.executable
    runs = []
    for seed in SEEDS:
        for kind in ("loaded", "isolated"):
            for rate in ((4, 8) if kind == "loaded" else (None,)):
                for caching, conditions in CONDITIONS.items():
                    for router, scheduler in conditions:
                        config = json.loads(json.dumps(base))
                        ident = f"{kind}-cache{'on' if caching else 'off'}-{router}-{scheduler}-{str(rate) + 's' if rate else 'solo'}-seed{seed}"
                        output = study / "runs" / ident
                        simulation_seed = 42 if kind == "loaded" else seed
                        config["output"] = str(output)
                        config["serving"]["engine"]["enable_prefix_caching"] = caching
                        config["serving"]["scheduler"] = {"name": scheduler}
                        config["serving"]["router"] = (
                            {"endpoint_picker_config": str(study / "provenance/llmd-jsq.yaml")}
                            if router == "llmd-jsq" else {"research_policy": router, "parameters":
                                parameters if router == "preble-e2" else {}})
                        config["run"] = {"seed": simulation_seed, "series_interval_s": 1 if kind == "loaded" else 0,
                            "arrivals": {"mode": "poisson", "rate_per_s": 1 / rate, "seed": seed}
                            if kind == "loaded" else {"mode": "fixed", "interval_s": 12000, "seed": seed}}
                        config_path = study / "configs" / f"{ident}.json"
                        write_json(config_path, config)
                        runs.append({"id": ident, "kind": kind, "router": router, "scheduler": scheduler,
                                     "prefix_caching": caching, "mean_interarrival_s": rate,
                                     "arrival_seed": seed, "simulation_seed": simulation_seed,
                                     "output": str(output), "config": str(config_path), "config_sha256": digest(config_path)})
    files = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=REPOSITORY).split(b"\0")
    with tarfile.open(study / "provenance/source.tar.gz", "w:gz") as archive:
        for name in sorted(set(os.fsdecode(name) for name in files if name)):
            path = REPOSITORY / name
            if path.is_file():
                archive.add(path, arcname=name, recursive=False)
    (study / "provenance/source.diff").write_bytes(subprocess.check_output(["git", "diff", "HEAD"], cwd=REPOSITORY))
    write_json(study / "study.json", {
        "version": 1, "created_utc": datetime.now(timezone.utc).isoformat(),
        "git_head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=REPOSITORY, text=True).strip(),
        "binary": str(frozen_binary), "binary_sha256": digest(frozen_binary),
        "source_archive_sha256": digest(study / "provenance/source.tar.gz"),
        "helper_sources": helper_sources(),
        "provenance_files": {str(path): digest(path) for path in (study / "provenance").iterdir() if path.is_file()},
        "input_sha256": trace_manifest["sha256"], "input_compressed_sha256": digest(trace),
        "source_totals": trace_manifest["source_totals"], "dependency_edges": 60551,
        "trace_manifest": str(study / "provenance/manifest.json"),
        "composition": str(study / "provenance/composition.json"), "dependencies": dependencies,
        "isolation": {"interval_s": 12000, "reference": "per-object minimum across all cache-matched isolated conditions and five simulation seeds; best observed, not a proven optimum"},
        "summary": "median and range of per-run statistics across five matched arrival seeds; full workload drained",
        "runs": runs,
    })
    print(f"Frozen {len(runs)} runs in {study / 'study.json'}", flush=True)


def one(config: Path, binary: Path) -> int:
    plan = prepare(config)
    commands = list(plan.commands)
    commands[1] = (sys.executable, "-B", "-c",
                   "import os,sys,shutil; src,dst=sys.argv[1:]; "
                   "os.link(src,dst) if os.stat(src).st_dev == os.stat(os.path.dirname(dst)).st_dev else shutil.copy2(src,dst)",
                   str(binary), str(plan.output / "blis"))
    return execute(replace(plan, commands=tuple(commands)))


def completed(run: dict, binary_hash: str) -> bool:
    output = Path(run["output"])
    status = output / "study-status.json"
    if not status.exists():
        return False
    saved = json.loads(status.read_text())
    if saved["binary_sha256"] != binary_hash or saved["config_sha256"] != run["config_sha256"]:
        raise ValueError(f"provenance changed for {run['id']}")
    for name, expected in saved["artifacts"].items():
        if digest(output / name) != expected:
            raise ValueError(f"saved artifact changed for {run['id']}/{name}")
    return saved["verified"]


def run_one(run: dict, manifest: dict, study: Path, retry_failed: bool) -> dict:
    output = Path(run["output"])
    check_hashes({run["config"]: run["config_sha256"], **manifest["helper_sources"]})
    if completed(run, manifest["binary_sha256"]):
        return {"id": run["id"], "status": "reused"}
    if output.exists():
        if not retry_failed:
            raise ValueError(f"incomplete output retained at {output}; inspect it, then pass --retry-failed to archive and retry")
        archived = study / "failed" / f"{run['id']}-{time.time_ns()}"
        archived.parent.mkdir(exist_ok=True)
        output.rename(archived)
    start = time.monotonic()
    with (study / "logs" / f"{run['id']}.log").open("w") as log:
        result = subprocess.run([sys.executable, "-B", "-m", "research.policy_study.run", "_one",
                                 run["config"], manifest["binary"]], cwd=REPOSITORY, stdout=log,
                                stderr=subprocess.STDOUT, env={**os.environ, "GIT_OPTIONAL_LOCKS": "0"})
    if result.returncode:
        raise ValueError(f"{run['id']} failed with status {result.returncode}; see {study / 'logs' / (run['id'] + '.log')}")
    verification = json.loads((output / "verification.json").read_text())
    if (not verification["verified"] or verification["source_totals"] != manifest["source_totals"]
            or verification["dependency_edges"] != manifest["dependency_edges"]):
        raise ValueError(f"verification differs for {run['id']}")
    saved = {"verified": True, "binary_sha256": manifest["binary_sha256"],
             "config_sha256": run["config_sha256"], "elapsed_s": time.monotonic() - start,
             "artifacts": {name: digest(output / name) for name in (
                 "metrics.json", "workload.json", "verification.json", "scenario.yaml", "config.resolved.json",
                 "config.original.json", "commands.json", "router.yaml", "router.json", "scheduler.json",
                 "router-decisions.json") if (output / name).is_file()}}
    write_json(output / "study-status.json", saved)
    return {"id": run["id"], "status": "verified", "elapsed_s": saved["elapsed_s"]}


def run_study(args) -> int:
    study = args.study.resolve()
    manifest = json.loads((study / "study.json").read_text())
    validate_provenance(study, manifest)
    selected = [run for run in manifest["runs"] if (args.kind == "all" or run["kind"] == args.kind)
                and (not args.match or args.match in run["id"])]
    if args.limit:
        selected = selected[:args.limit]
    if not selected:
        raise ValueError("no runs selected")
    failures = []
    with (study / "runner.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        with ThreadPoolExecutor(max_workers=args.jobs) as pool:
            futures = {pool.submit(run_one, run, manifest, study, args.retry_failed): run for run in selected}
            for index, future in enumerate(as_completed(futures), 1):
                try:
                    result = future.result()
                except Exception as error:
                    result = {"id": futures[future]["id"], "error": str(error)}
                    failures.append(result)
                print(f"[{index}/{len(selected)}] {json.dumps(result)}", flush=True)
    write_json(study / "last-invocation.json", {"selected": len(selected), "failures": failures})
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="freeze the binary, source, inputs and all configurations")
    init.add_argument("study", type=Path)
    init.add_argument("--binary", type=Path, required=True)
    init.add_argument("--calibration", type=Path, required=True)
    run = commands.add_parser("run", help="run or resume verified independent simulations")
    run.add_argument("study", type=Path)
    run.add_argument("--jobs", type=int, default=8)
    run.add_argument("--kind", choices=("all", "loaded", "isolated"), default="all")
    run.add_argument("--match", help="optional run-ID substring")
    run.add_argument("--limit", type=int)
    run.add_argument("--retry-failed", action="store_true")
    worker = commands.add_parser("_one", help="internal worker for a frozen run")
    worker.add_argument("config", type=Path)
    worker.add_argument("binary", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "init":
            initialize(args.study.resolve(), args.binary.resolve(), args.calibration.resolve())
            return 0
        if args.command == "_one":
            return one(args.config, args.binary)
        if args.jobs < 1 or (args.limit is not None and args.limit < 1):
            raise ValueError("jobs and limit must be positive")
        return run_study(args)
    except (OSError, ValueError, KeyError) as error:
        print(f"policy study: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
