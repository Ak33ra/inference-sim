#!/usr/bin/env python3
"""Compare akira's session-SPLIT policy in the three-tier routing setting.

Run from the repository root after creating a worktree for the ``akira`` branch::

    git worktree add --detach /tmp/inference-sim-session-split akira
    python3 -B research/writeups/three-tier-routing/session_split_compare.py \
      --split-repo /tmp/inference-sim-session-split --jobs 10

The experiment uses the same 164 Commit0 plus 25 Oolong families, per-seed
shuffle, Poisson rate, serving configuration, trim, and optrouting slowdown
denominators as the three-tier experiment.  Five isolated JSQ runs supply the
remaining-session oracle required by session SPLIT.  Loaded llm-d JSQ controls
must reproduce the already-recorded three-tier controls exactly.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import csv
import hashlib
import json
import math
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import sys


HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]
SEEDS = tuple(range(42, 47))
RATE = 0.223837
TRIM = 0.10
GPUS = 8
BUCKETS = ("w00-04", "w05-09", "w10-14", "w15-19", "w20-24",
           "w25-29", "w30-34", "w65-69")
POLICIES = ("jsq", "session-split")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def source_families(work: Path) -> list[str]:
    """Rebuild the exact optrouting-covered population; exclude all SWE-bench IDs."""
    path = work / "families.jsonl"
    if path.exists():
        lines = path.read_text().splitlines()
    else:
        lines = []
        for bucket in BUCKETS:
            source = REPO / "traces/commit0-fanout-buckets" / bucket / "traces.jsonl.zst"
            text = subprocess.run(["zstd", "-dc", str(source)], check=True,
                                  capture_output=True, text=True).stdout
            lines.extend(line for line in text.splitlines()
                         if json.loads(line)["id"].startswith("commit0"))
        text = subprocess.run(["zstd", "-dc", str(REPO / "natural-oolong-fanout.jsonl.zst")],
                              check=True, capture_output=True, text=True).stdout
        lines.extend(text.splitlines())
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("\n".join(lines) + "\n")
    ids = [json.loads(line)["id"] for line in lines]
    require(len(lines) == len(set(ids)) == 189, "matched population must contain 189 unique families")
    require(sum(i.startswith("commit0") for i in ids) == 164, "Commit0 coverage differs")
    require(not any(i.startswith("swebench") for i in ids), "SWE-bench leaked into matched population")
    return lines


def traces(work: Path, lines: list[str]) -> dict[int, Path]:
    result = {}
    for seed in SEEDS:
        shuffled = list(lines)
        random.Random(seed).shuffle(shuffled)
        path = work / f"families-s{seed}.jsonl"
        path.write_text("\n".join(shuffled) + "\n")
        result[seed] = path
    return result


def base_config(split_repo: Path, trace: Path, output: Path, seed: int, arrivals: dict) -> dict:
    config = json.loads((split_repo / "research/configs/semiclosed/commit0-overlap-jsq-8replicas.json").read_text())
    config["trace"] = {"format": "weka-dag", "path": str(trace)}
    config["serving"]["cluster"]["replicas"] = GPUS
    config["run"]["seed"] = seed
    config["run"]["series_interval_s"] = 0
    config["run"]["arrivals"] = arrivals
    config["output"] = str(output)
    config["python"] = sys.executable
    return config


def completed(output: Path) -> bool:
    try:
        work = json.loads((output / "workload.json").read_text())
        return bool(work["complete"] and work["accounting_match"] and
                    work["states"].get("failed", 0) == 0 and
                    (output / "verification.json").exists())
    except (OSError, ValueError, KeyError):
        return False


def run_command(command: list[str], *, cwd: Path, env: dict[str, str], log: Path) -> None:
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("w") as handle:
        process = subprocess.run(command, cwd=cwd, env=env, stdout=handle,
                                 stderr=subprocess.STDOUT, text=True)
    if process.returncode:
        tail = "\n".join(log.read_text(errors="replace").splitlines()[-30:])
        raise RuntimeError(f"command failed ({process.returncode}); see {log}\n{tail}")


def run_regular(split_repo: Path, config_path: Path, env: dict[str, str], log: Path) -> None:
    run_command([str(split_repo / "research/scripts/semi_closed.sh"), "run", str(config_path)],
                cwd=split_repo, env=env, log=log)


def run_split(split_repo: Path, preparation: Path, actual: Path,
              env: dict[str, str], log: Path) -> None:
    adapter = """
import sys
from research.session_split.run import adapted_plan
from research.semiclosed.run import execute
run = {'preparation_config': sys.argv[1], 'config': sys.argv[2],
       'router': 'session-split'}
raise SystemExit(execute(adapted_plan(run)))
"""
    run_command([sys.executable, "-B", "-c", adapter, str(preparation), str(actual)],
                cwd=split_repo, env=env, log=log)


def execute_one(kind: str, seed: int, trace: Path, split_repo: Path, work: Path,
                manifest: Path | None, env: dict[str, str], rate: float) -> Path:
    name = f"{kind}-s{seed}"
    output = work / "runs" / name
    if completed(output):
        return output
    if output.exists():
        shutil.rmtree(output)
    arrivals = ({"mode": "fixed", "interval_s": 12000, "seed": 42}
                if kind == "isolated-jsq" else
                {"mode": "poisson", "rate_per_s": rate, "seed": seed})
    config = base_config(split_repo, trace, output, seed, arrivals)
    config_dir = work / "configs"
    config_dir.mkdir(parents=True, exist_ok=True)
    log = work / "logs" / f"{name}.log"
    if kind in ("isolated-jsq", "jsq"):
        config["serving"]["router"] = {"endpoint_picker_config":
            str(split_repo / "research/configs/routers/jsq.yaml")}
        path = config_dir / f"{name}.json"
        write_json(path, config)
        run_regular(split_repo, path, env, log)
    else:
        require(manifest is not None, "session SPLIT requires an oracle manifest")
        preparation_config = json.loads(json.dumps(config))
        preparation_config["serving"]["router"] = {"research_policy": "jsq", "parameters": {}}
        actual_config = json.loads(json.dumps(config))
        actual_config["serving"]["router"] = {"research_policy": "session-split", "parameters": {
            "remaining_manifest": str(manifest)}}
        preparation = config_dir / f"{name}.prepare.json"
        actual = config_dir / f"{name}.json"
        write_json(preparation, preparation_config)
        write_json(actual, actual_config)
        run_split(split_repo, preparation, actual, env, log)
    require(completed(output), f"run did not complete cleanly: {output}")
    (output / "blis").unlink(missing_ok=True)
    return output


def runtime_session(row: dict) -> str:
    return row["trace_id"] + "/" + row["session_id"]


def build_manifest(isolated: dict[int, Path], trace: Path, path: Path) -> dict:
    """Choose each session's fastest isolated JSQ timeline, then freeze remaining time per turn."""
    works = {seed: json.loads((output / "workload.json").read_text())
             for seed, output in isolated.items()}
    selected: dict[str, tuple[int, int]] = {}
    sessions_by_run: dict[int, dict[str, dict]] = {}
    requests_by_run: dict[int, dict[str, list[dict]]] = {}
    for seed, work in works.items():
        require(len(work["families"]) == 189 and len(work["requests"]) == 56619,
                f"isolated seed {seed} population differs")
        sessions = {runtime_session(row): row for row in work["sessions"]}
        requests: dict[str, list[dict]] = {}
        for row in work["requests"]:
            requests.setdefault(runtime_session(row), []).append(row)
        require(set(sessions) == set(requests), f"isolated seed {seed} session coverage differs")
        sessions_by_run[seed], requests_by_run[seed] = sessions, requests
        for key, row in sessions.items():
            elapsed = row["completion_us"] - row["arrival_us"]
            candidate = (elapsed, seed)
            if key not in selected or candidate < selected[key]:
                selected[key] = candidate
    require(len(selected) == 2239, "isolated session coverage differs")
    requests = {}
    source_hashes = {}
    for seed, output in isolated.items():
        source = output / "workload.json"
        source_hashes[str(source)] = digest(source)
    for key, (elapsed, seed) in sorted(selected.items()):
        session = sessions_by_run[seed][key]
        rows = sorted(requests_by_run[seed][key], key=lambda row: row["step"])
        previous = None
        for row in rows:
            remaining = session["completion_us"] - row["arrival_us"]
            require(remaining > 0, f"nonpositive remaining time for {row['request_id']}")
            if previous is None:
                require(remaining == elapsed, f"first-turn reference differs for {key}")
            else:
                require(row["step"] > previous["step"] and
                        row["arrival_us"] >= previous["completion_us"],
                        f"nonsequential reference session {key}")
                require({"request_id": previous["request_id"], "on": "completion"}
                        in row["prerequisites"], f"missing sequential dependency for {key}")
            ident = row["request_id"]
            require(ident not in requests, f"duplicate request identity {ident}")
            requests[ident] = {
                "trace_id": row["trace_id"], "session_id": row["session_id"],
                "runtime_session_id": key, "step": row["step"], "remaining_us": remaining,
                "reference_run": f"isolated-jsq-s{seed}", "reference_elapsed_us": elapsed,
                "reference_arrival_us": row["arrival_us"],
                "reference_session_completion_us": session["completion_us"],
                "previous_request_id": previous["request_id"] if previous else "",
            }
            previous = row
    require(len(requests) == 56619, "remaining manifest request coverage differs")
    manifest = {
        "format_version": 1, "trace_sha256": digest(trace),
        "reference_label": "fastest of five isolated llm-d JSQ session timelines; cache off",
        "source_hashes": source_hashes, "requests": dict(sorted(requests.items())),
    }
    write_json(path, manifest)
    return manifest


def verify_split_audit(output: Path, manifest: dict) -> dict:
    audit = json.loads((output / "router-decisions.json").read_text())
    require(audit["format_version"] == 1 and audit["policy"] == "session-split",
            "invalid session-SPLIT audit")
    ready: dict[str, dict] = {}
    counts: dict[str, int] = {}
    max_ready = 0
    claims_with_choice = 0
    claim_ready_total = 0
    for event in audit["events"]:
        kind, ident = event["kind"], event["request_id"]
        counts[kind] = counts.get(kind, 0) + 1
        require(event["remaining_us"] == manifest["requests"][ident]["remaining_us"],
                f"oracle value differs for {ident}")
        if kind == "ready":
            require(ident not in ready, f"duplicate ready event {ident}")
            ready[ident] = event
            max_ready = max(max_ready, len(ready))
        elif kind == "claim":
            require(ident in ready, f"claimed request was not ready: {ident}")
            claims_with_choice += event["ready_count"] > 1
            claim_ready_total += event["ready_count"]
            largest = event["instance"] == "instance_0"
            def order(item: tuple[str, dict]) -> tuple:
                request_id, value = item
                priority = value["remaining_us"]
                return (-priority if largest else priority, value["arrival_us"], request_id)
            expected = min(ready.items(), key=order)[0]
            require(ident == expected, f"SPLIT extreme selection differs: {ident} != {expected}")
            del ready[ident]
    require(not ready, "session-SPLIT audit ended with ready requests")
    require(all(counts.get(kind) == 56619 for kind in ("prepare", "ready", "claim", "complete")),
            f"session-SPLIT lifecycle counts differ: {counts}")
    for instance, values in audit["reservations"].items():
        for field in ("final_used_kv_blocks", "final_promised_blocks", "admission_refusals",
                      "preemptions", "allocation_failures"):
            if field in values:
                require(values[field] == 0, f"{instance} {field} did not drain")
    return {"events": len(audit["events"]), "counts": counts, "max_ready": max_ready,
            "claims_with_choice": claims_with_choice,
            "fraction_claims_with_choice": claims_with_choice / counts["claim"],
            "mean_ready_at_claim": claim_ready_total / counts["claim"],
            "renderers": audit["renderers"]}


def optima() -> dict[str, float]:
    values = {}
    for path in (REPO / "research/outputs/optrouting/data/root_e2e.csv",
                 REPO / "research/outputs/optrouting/data/oolong/root_e2e.csv"):
        with path.open() as handle:
            values.update({row["family"]: float(row["optimal_s"]) for row in csv.DictReader(handle)})
    require(len(values) == 189, "optrouting family references differ")
    return values


def family_classes(opt: dict[str, float]) -> dict[str, str]:
    order = sorted(opt, key=lambda key: (opt[key], key))
    n1, n2 = round(.4 * len(order)), round(.6 * len(order))
    return {key: "P1" if index < n1 else "P2" if index < n2 else "P3"
            for index, key in enumerate(order)}


def statistic(values) -> dict:
    values = sorted(values)
    return {"n": len(values), "median": statistics.median(values),
            "mean": statistics.fmean(values),
            "p99": statistics.quantiles(values, n=100, method="inclusive")[98]}


def analyse(outputs: dict[tuple[str, int], Path], out: Path, audit: dict[int, dict],
            comparison_path: Path, rate: float) -> dict:
    opt, classes = optima(), family_classes(optima())
    rows = []
    results = {}
    for policy in POLICIES:
        per_seed = {}
        for seed in SEEDS:
            work = json.loads((outputs[(policy, seed)] / "workload.json").read_text())
            require(work["complete"] and work["accounting_match"] and len(work["families"]) == 189,
                    f"loaded run differs: {policy} seed {seed}")
            families = sorted(work["families"], key=lambda row: (row["birth_us"], row["trace_id"]))
            cut = math.floor(TRIM * len(families))
            seed_rows = []
            for index, family in enumerate(families):
                ident = family["trace_id"]
                require(ident in opt, f"family lacks optrouting reference: {ident}")
                row = {"policy": policy, "seed": seed, "family": ident,
                       "class": classes[ident], "arrival_s": family["birth_us"] / 1e6,
                       "e2e_s": family["makespan_us"] / 1e6, "optimal_s": opt[ident],
                       "slowdown": family["makespan_us"] / 1e6 / opt[ident],
                       "kept": cut <= index < len(families) - cut}
                rows.append(row)
                if row["kept"]:
                    seed_rows.append(row)
            per_seed[str(seed)] = {"mean": statistics.fmean(row["slowdown"] for row in seed_rows)}
            if policy == "session-split":
                per_seed[str(seed)]["audit"] = audit[seed]
        kept = [row for row in rows if row["policy"] == policy and row["kept"]]
        results[policy] = {"pooled": statistic(row["slowdown"] for row in kept),
                           "per_seed": per_seed,
                           "by_class": {label: statistic(row["slowdown"] for row in kept
                                                         if row["class"] == label)
                                        for label in ("P1", "P2", "P3")}}
    recorded = json.loads(comparison_path.read_text())
    for policy in ("jsq", "tier-40-60"):
        recorded_rate = recorded[policy].get("rate_per_s", RATE)
        require(math.isclose(recorded_rate, rate),
                f"comparison results use rate {recorded_rate}, expected {rate}")
    for metric in ("median", "mean", "p99"):
        require(math.isclose(results["jsq"]["pooled"][metric], recorded["jsq"]["pooled"][metric],
                             rel_tol=1e-12), f"akira JSQ does not reproduce recorded {metric}")
    for seed in SEEDS:
        require(math.isclose(results["jsq"]["per_seed"][str(seed)]["mean"],
                             recorded["jsq"]["per_seed"][str(seed)]["mean"], rel_tol=1e-12),
                f"akira JSQ does not reproduce seed {seed}")
    results["recorded_three_tier"] = recorded["tier-40-60"]
    results["rate_per_s"] = rate
    with (out / "families.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    write_json(out / "results.json", results)
    return results


def print_results(results: dict) -> None:
    jsq = results["jsq"]["pooled"]
    split = results["session-split"]["pooled"]
    tier = results["recorded_three_tier"]["pooled"]
    change = lambda value, base: 100 * (value / base - 1)
    print("policy          median   mean     p99      (pooled, seeds 42-46, n = 765)")
    for name, row in (("JSQ", jsq), ("three-tier", tier), ("session SPLIT", split)):
        print(f"{name:<15} {row['median']:.3f}    {row['mean']:.3f}    {row['p99']:.3f}")
    print("session SPLIT vs JSQ: " + ", ".join(
        f"{metric} {change(split[metric], jsq[metric]):+.1f}%" for metric in ("median", "mean", "p99")))
    print("session SPLIT vs three-tier: " + ", ".join(
        f"{metric} {change(split[metric], tier[metric]):+.1f}%" for metric in ("median", "mean", "p99")))
    for seed in SEEDS:
        sm = results["session-split"]["per_seed"][str(seed)]["mean"]
        jm = results["jsq"]["per_seed"][str(seed)]["mean"]
        tm = results["recorded_three_tier"]["per_seed"][str(seed)]["mean"]
        print(f"seed {seed}: SPLIT {sm:.3f}, JSQ {jm:.3f} ({change(sm, jm):+.1f}%), "
              f"three-tier {tm:.3f} ({change(sm, tm):+.1f}%)")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--split-repo", type=Path, required=True,
                        help="worktree at the akira branch containing session SPLIT")
    parser.add_argument("--out", type=Path,
                        default=REPO / "research/outputs/three-tier-routing-session-split")
    parser.add_argument("--jobs", type=int, default=10)
    parser.add_argument("--rate", type=float, default=RATE,
                        help=f"Poisson root-family rate/s (default: {RATE})")
    parser.add_argument("--three-tier-results", type=Path,
                        default=REPO / "research/outputs/three-tier-routing/results.json")
    parser.add_argument("--remaining-manifest", type=Path,
                        help="reuse a previously verified session oracle instead of isolated runs")
    args = parser.parse_args()
    split_repo, out = args.split_repo.resolve(), args.out.resolve()
    require((split_repo / "sim/cluster/session_split.go").exists(),
            f"session SPLIT implementation not found in {split_repo}")
    require(args.jobs > 0 and math.isfinite(args.rate) and args.rate > 0,
            "--jobs and --rate must be positive")
    catalog = REPO / "research/outputs/deps/blis-catalog"
    registry = REPO / "research/outputs/deps/blis-registry"
    require(catalog.is_dir() and registry.is_dir(),
            "run three-tier replicate.sh once to provision the pinned catalog and registry")
    env = {**os.environ, "BLIS_CATALOG": str(catalog), "BLIS_REGISTRY": str(registry),
           "GOWORK": "off", "GIT_OPTIONAL_LOCKS": "0"}
    work = out / "work"
    work.mkdir(parents=True, exist_ok=True)
    lines = source_families(work)
    shuffled = traces(work, lines)
    isolated_trace = work / "families.jsonl"
    if args.remaining_manifest:
        manifest_path = args.remaining_manifest.resolve()
        manifest = json.loads(manifest_path.read_text())
        require(len(manifest.get("requests", {})) == 56619,
                "reused remaining manifest coverage differs")
    else:
        with cf.ThreadPoolExecutor(max_workers=min(args.jobs, len(SEEDS))) as pool:
            futures = {pool.submit(execute_one, "isolated-jsq", seed, isolated_trace,
                                   split_repo, work, None, env, args.rate): seed for seed in SEEDS}
            isolated = {}
            for future in cf.as_completed(futures):
                seed = futures[future]
                isolated[seed] = future.result()
                print(f"isolated JSQ seed {seed} complete", flush=True)
        manifest_path = out / "remaining.json"
        manifest = build_manifest(isolated, isolated_trace, manifest_path)
    jobs = [(policy, seed) for policy in POLICIES for seed in SEEDS]
    outputs = {}
    with cf.ThreadPoolExecutor(max_workers=min(args.jobs, len(jobs))) as pool:
        futures = {pool.submit(execute_one, policy, seed, shuffled[seed], split_repo,
                               work, manifest_path, env, args.rate): (policy, seed)
                   for policy, seed in jobs}
        for future in cf.as_completed(futures):
            key = futures[future]
            outputs[key] = future.result()
            print(f"loaded {key[0]} seed {key[1]} complete", flush=True)
    audit = {seed: verify_split_audit(outputs[("session-split", seed)], manifest)
             for seed in SEEDS}
    results = analyse(outputs, out, audit, args.three_tier_results.resolve(), args.rate)
    print_results(results)
    print(f"MATCHED COMPARISON COMPLETE: {out}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, RuntimeError) as error:
        print(f"session SPLIT comparison: {error}", file=sys.stderr)
        raise SystemExit(2)
