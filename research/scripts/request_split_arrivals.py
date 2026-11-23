"""Derive paired JSQ/request-SPLIT experiments by changing family-root arrival rate."""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime, timezone
import fcntl
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import time

REPOSITORY = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPOSITORY))

from research.policy_study.analysis import RunSpec, logical_work_signature, read_json, require, validate_run, write_csv
from research.policy_study.run import check_hashes, completed, digest, write_json
from research.request_split.analysis import aggregate, object_rows, paired_objects, paired_statistics, per_run_statistics, request_rows
from research.request_split.common import EDGES, POLICIES, SEEDS, TOTALS, arrival_schedule, runtime_id, validate_provenance as validate_base, verify_audit
from research.request_split.run import ARTIFACTS
from research.semiclosed.verify_dag import verify as verify_dag

ENV = {**os.environ, "GIT_OPTIONAL_LOCKS": "0"}


def derived_config(source, output, mean):
    config = deepcopy(source)
    config["output"] = output
    config["run"]["arrivals"]["rate_per_s"] = 1 / mean
    return config


def initialize(directory, base_directory, mean):
    require(math.isfinite(mean) and mean > 0, "mean interarrival must be finite and positive")
    require(not directory.exists(), f"output directory already exists: {directory}")
    base_path = base_directory / "study.json"
    base = read_json(base_path)
    validate_base(base)
    require(base["source_totals"] == TOTALS and base["dependency_edges"] == EDGES, "base workload differs")
    frozen = {str(base_path): digest(base_path)}
    for run in base["runs"]:
        require(completed(run, base["binary_sha256"]), f"unverified base run: {run['id']}")
        config = read_json(Path(run["config"]))
        require(config["run"]["arrivals"] == {"mode": "poisson", "rate_per_s": .25, "seed": run["arrival_seed"]}
                and config["run"]["seed"] == 42, "base must be the four-second paired study")
        for name in ("study-status.json", "workload.json"):
            path = Path(run["output"]) / name
            frozen[str(path)] = digest(path)
    for name in ("configs", "logs"):
        (directory / name).mkdir(parents=True)
    runs = []
    for source in base["runs"]:
        run = deepcopy(source)
        run.pop("old_output", None)
        ident = f"loaded-cacheoff-{run['router']}-fcfs-{mean:g}s-seed{run['arrival_seed']}"
        run.update(id=ident, output=str(directory / "runs" / ident), base_run_id=source["id"],
                   base_output=source["output"], mean_interarrival_s=mean)
        for field in ("config", "preparation_config"):
            suffix = ".prepare.json" if field == "preparation_config" and run["router"] == "request-split" else ".json"
            path = directory / "configs" / (ident + suffix)
            config = derived_config(read_json(Path(source[field])), run["output"], mean)
            if not path.exists():
                write_json(path, config)
            require(read_json(path) == config, "configuration paths alias different content")
            run[field], run[field + "_sha256"] = str(path), digest(path)
            frozen[str(path)] = digest(path)
        runs.append(run)
    helpers = [Path(__file__).resolve(), *sorted((REPOSITORY / "research/request_split").glob("*.py")),
               *sorted((REPOSITORY / "research/semiclosed").glob("*.py")),
               REPOSITORY / "research/policy_study/analysis.py", REPOSITORY / "research/policy_study/run.py",
               REPOSITORY / "research/session_reservation/common.py"]
    inherited = ("binary", "binary_sha256", "input_sha256", "source_totals", "dependency_edges", "service_manifest",
                 "service_manifest_sha256", "session_class_manifest", "session_class_manifest_sha256", "reference", "summary")
    study = {key: base[key] for key in inherited}
    frozen.update({str(path): digest(path) for path in helpers + [Path(base[key]) for key in ("binary", "service_manifest", "session_class_manifest")]})
    study.update(format_version=1, created_utc=datetime.now(timezone.utc).isoformat(), base_study=str(base_directory),
                 mean_interarrival_s=mean, base_mean_interarrival_s=4, frozen_files=frozen, runs=runs,
                 comparison="Paired JSQ/FCFS and request-SPLIT at the new family-root rate; no numerical parity requirement to four-second controls.")
    write_json(directory / "study.json", study)
    print(f"Frozen ten paired runs at mean {mean:g}s in {directory}", flush=True)


def load_study(directory):
    study = read_json(directory / "study.json")
    check_hashes(study["frozen_files"])
    base = read_json(Path(study["base_study"]) / "study.json")
    validate_base(base)
    require(math.isfinite(study["mean_interarrival_s"]) and study["mean_interarrival_s"] > 0, "invalid arrival mean")
    require(len(study["runs"]) == 10 and {(r["router"], r["arrival_seed"]) for r in study["runs"]} ==
            {(p, seed) for p in POLICIES for seed in SEEDS}, "paired matrix differs")
    sources = {r["id"]: r for r in base["runs"]}
    for run in study["runs"]:
        source = sources[run["base_run_id"]]
        require(run["mean_interarrival_s"] == study["mean_interarrival_s"], "run arrival mean differs from study")
        require((run["router"], run["arrival_seed"], run["simulation_seed"], run["base_output"]) ==
                (source["router"], source["arrival_seed"], source["simulation_seed"], source["output"]), "base run identity differs")
        for field in ("config", "preparation_config"):
            require(digest(Path(run[field])) == run[field + "_sha256"], "run configuration hash differs")
            require(read_json(Path(run[field])) == derived_config(read_json(Path(source[field])), run["output"], study["mean_interarrival_s"]),
                    "derived configuration changed beyond arrival rate and output")
    for key in ("binary", "binary_sha256", "service_manifest", "service_manifest_sha256", "session_class_manifest",
                "session_class_manifest_sha256", "input_sha256", "source_totals", "dependency_edges"):
        require(study[key] == base[key], f"derived study changed {key}")
    return study


def verify_one(study, run):
    output = Path(run["output"])
    work, metrics, verification = (read_json(output / name) for name in ("workload.json", "metrics.json", "verification.json"))
    classes = read_json(Path(study["session_class_manifest"]))
    spec = RunSpec(run["id"], "loaded", run["router"], "fcfs", False, study["mean_interarrival_s"],
                   run["arrival_seed"], 42, output, Path(run["config"]), run["config_sha256"])
    signature, edges = validate_run(spec, work, metrics, verification, TOTALS, study["input_sha256"], classes["families"])
    require(edges == EDGES and {runtime_id(r) for r in work["sessions"]} == classes["sessions"].keys(), "DAG or session coverage differs")
    require(verify_dag(output / "input.jsonl.zst", output / "workload.json", output / "metrics.json") == verification,
            "saved causal verification differs")
    base_work = read_json(Path(run["base_output"]) / "workload.json")
    require(signature == logical_work_signature(base_work["requests"])[0], "logical work differs from four-second source")
    base_births = {r["trace_id"]: r["birth_us"] for r in base_work["families"]}
    if study["mean_interarrival_s"] == 2:
        scaled = all(abs(2 * r["birth_us"] - base_births[r["trace_id"]]) <= 1 for r in work["families"])
    else:
        scale = study["mean_interarrival_s"] / 4
        scaled = all(abs(r["birth_us"] - base_births[r["trace_id"]] * scale) <= max(1, (1 + scale) / 2)
                     for r in work["families"])
    require(scaled, "seeded Poisson schedule does not scale with the changed rate")
    config = read_json(Path(run["config"]))
    require(read_json(output / "config.original.json") == config, "saved input config differs")
    resolved = read_json(output / "config.resolved.json")
    require(resolved["serving"]["router"]["research_policy"] == run["router"] and
            resolved["serving"]["router"]["parameters"] == config["serving"]["router"]["parameters"], "resolved router differs")
    require(metrics["preemption_count"] == metrics["cache_hit_rate"] == 0, "preemption or cache reuse occurred")
    engines = metrics["server_series"]["engines"]
    require(len(engines) == 8 and {e["id"] for e in engines} == {f"instance_{i}" for i in range(8)}, "engine coverage differs")
    physical = {key: sum(sum(e[key]) for e in engines) for key in
                ("prompt_tokens_local_compute", "prompt_tokens_local_cache_hit", "prompt_tokens_external_kv_transfer", "generation_tokens")}
    require(physical == {"prompt_tokens_local_compute": TOTALS["prompt_tokens"], "prompt_tokens_local_cache_hit": 0,
                         "prompt_tokens_external_kv_transfer": 0, "generation_tokens": TOTALS["output_tokens"]}, "physical token accounting differs")
    require(all(e[key][-1] == 0 for e in engines for key in
                ("kv_cache_usage_last", "num_requests_running_last", "num_requests_waiting_last")), "engines did not drain")
    audit = (verify_audit(read_json(output / "router-decisions.json"), read_json(Path(study["service_manifest"])), work, metrics)
             if run["router"] == "request-split" else {})
    return {"logical_work_sha256": signature, "dependency_edges": edges, "physical_tokens": physical,
            "poisson_births_scale_from_base": True, "base_numerical_parity_required": False, **audit}


def paired_schedules(study, require_all):
    result = []
    for seed in SEEDS:
        pair = [r for r in study["runs"] if r["arrival_seed"] == seed]
        ready = all(completed(r, study["binary_sha256"]) for r in pair)
        require(ready or not require_all, f"incomplete pair for seed {seed}")
        if ready:
            schedules = [arrival_schedule(read_json(Path(r["output"]) / "workload.json")) for r in pair]
            require(schedules[0] == schedules[1], f"paired root schedules differ for seed {seed}")
            result.append({"arrival_seed": seed, "families": len(schedules[0]), "root_schedules_equal": True})
    return result


def run_one(directory, study, run, retry):
    if completed(run, study["binary_sha256"]):
        return {"id": run["id"], "status": "reused"}
    output = Path(run["output"])
    if output.exists():
        require(retry, f"incomplete output retained at {output}; inspect then use --retry-failed")
        archive = directory / "failed" / f"{run['id']}-{time.time_ns()}"
        archive.parent.mkdir(exist_ok=True)
        output.rename(archive)
    start = time.monotonic()
    log_path = directory / "logs" / f"{run['id']}-{time.time_ns()}.log"
    with log_path.open("x") as log:
        process = subprocess.run([sys.executable, "-B", "-m", "research.request_split.run", "_one", str(directory), run["id"]],
                                 cwd=REPOSITORY, env=ENV, stdout=log, stderr=subprocess.STDOUT)
    require(process.returncode == 0, f"{run['id']} failed; retained log {log_path}")
    write_json(output / "split-verification.json", verify_one(study, run))
    write_json(output / "study-status.json", {"verified": True, "binary_sha256": study["binary_sha256"],
               "config_sha256": run["config_sha256"], "elapsed_s": time.monotonic() - start, "log": str(log_path),
               "artifacts": {name: digest(output / name) for name in ARTIFACTS}})
    return {"id": run["id"], "status": "verified", "elapsed_s": time.monotonic() - start}


def run_study(directory, jobs, match, retry):
    require(jobs > 0, "jobs must be positive")
    with (directory / "runner.lock").open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        study = load_study(directory)
        selected = [r for r in study["runs"] if not match or match in r["id"]]
        require(selected, "no matching runs")
        failures = []
        with ThreadPoolExecutor(max_workers=jobs) as pool:
            futures = {pool.submit(run_one, directory, study, run, retry): run for run in selected}
            for future in as_completed(futures):
                try:
                    result = future.result()
                except Exception as error:
                    result = {"id": futures[future]["id"], "error": str(error)}
                    failures.append(result)
                print(json.dumps(result), flush=True)
        write_json(directory / "last-invocation.json", {"selected": len(selected), "failures": failures,
                   "paired_schedules": paired_schedules(study, require_all=False)})
    return int(bool(failures))


def report(study, summary):
    mean = study["mean_interarrival_s"]
    lines = [f"# Request SPLIT at {mean:g}-second mean family arrivals", "",
             f"The fully aggregated Commit0+Oolong trace runs on eight B200 replicas with cache off and native FCFS batching. "
             f"Family roots use Poisson arrivals with mean {mean:g}s, paired seeds 42–46 and simulator seed 42.", "",
             "The frozen four-second study supplies the unchanged binary, exact isolated request-service oracle and session/family slowdown references. "
             "Only arrival rate and output paths change. Numerical parity to four-second JSQ results is not required at this load.", "",
             "Replica 0 selects the longest ready request; the other seven select the shortest from one queue. Native nonpreemptive batching remains active. "
             "Central preparation uses round-robin endpoints; JSQ couples preparation to GPU routing.", "",
             "Cells show median [min, max] across five per-run mean/p50/p99 statistics. Paired changes compare matching arrival seeds. "
             "Slowdown uses shared best-observed isolated-family references, including sibling contention; values below one remain valid.", "",
             "| Unit | Metric | Statistic | JSQ | Request SPLIT | Paired change (%) |", "|---|---|---|---:|---:|---:|"]
    index = {(r["router"], r["unit"], r["metric"], r["statistic"]): r for r in summary["aggregate"]}
    def cell(row):
        return f"{row['median']:.4f} [{row['min']:.4f}, {row['max']:.4f}]"
    for change in summary["paired_percent_changes"]:
        key = change["unit"], change["metric"], change["statistic"]
        lines.append(f"| {' | '.join(key)} | {cell(index['jsq', *key])} | {cell(index['request-split', *key])} | {cell(change)} |")
    lines += ["", "All ten runs passed logical and physical work accounting, causal DAG checks, zero preemption and final engine drain. "
              "Each seed pair has identical root schedules. Request-SPLIT audits independently reconstruct preparation, ready-set extrema and admission. "
              "Raw request/session/family rows, paired deltas and verification evidence accompany this report."]
    return "\n".join(lines) + "\n"


def analyze(directory):
    study = load_study(directory)
    pairs = paired_schedules(study, require_all=True)
    classes, oracle = (read_json(Path(study[key])) for key in ("session_class_manifest", "service_manifest"))
    sessions, families, requests, statistics, evidence = [], [], [], [], []
    for run in study["runs"]:
        evidence.append({"run_id": run["id"], **verify_one(study, run)})
        output = Path(run["output"])
        work, metrics, audit = (read_json(output / name) for name in ("workload.json", "metrics.json", "router-decisions.json"))
        ss, fs = object_rows(run, work, classes)
        sessions.extend(ss)
        families.extend(fs)
        requests.extend(request_rows(run, work, metrics, oracle, audit))
        statistics.extend(per_run_statistics(run, ss, fs))
    stats = aggregate(statistics, ("router", "unit", "metric", "statistic"), "value")
    deltas = paired_statistics(statistics)
    changes = aggregate(deltas, ("unit", "metric", "statistic"), "percent_change")
    summary = {"format_version": 1, "mean_interarrival_s": study["mean_interarrival_s"], "base_study": study["base_study"],
               "reference": study["reference"], "aggregate": stats, "paired_percent_changes": changes,
               "verification": evidence, "paired_schedules": pairs,
               "counts": {"runs": 10, "request_rows": len(requests), "session_rows": len(sessions), "family_rows": len(families)}}
    output = directory / "analysis"
    output.mkdir(exist_ok=True)
    for name, rows in (("sessions", sessions), ("families", families), ("requests", requests), ("per-run", statistics),
                       ("aggregate", stats), ("paired-deltas", deltas), ("paired-changes-summary", changes),
                       ("paired-sessions", paired_objects(sessions, "runtime_session_id")), ("paired-families", paired_objects(families, "trace_id"))):
        write_csv(output / f"{name}.csv", rows)
    write_json(output / "summary.json", summary)
    (output / "report.md").write_text(report(study, summary))
    print(output, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init")
    init.add_argument("study", type=Path)
    init.add_argument("--base-study", type=Path, required=True)
    init.add_argument("--mean-interarrival-s", type=float, default=2)
    run = commands.add_parser("run")
    run.add_argument("study", type=Path)
    run.add_argument("--jobs", type=int, default=2)
    run.add_argument("--match")
    run.add_argument("--retry-failed", action="store_true")
    analysis = commands.add_parser("analyze")
    analysis.add_argument("study", type=Path)
    args = parser.parse_args()
    try:
        directory = args.study.resolve()
        if args.command == "init":
            initialize(directory, args.base_study.resolve(), args.mean_interarrival_s)
        elif args.command == "run":
            return run_study(directory, args.jobs, args.match, args.retry_failed)
        else:
            analyze(directory)
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"request split arrivals: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
