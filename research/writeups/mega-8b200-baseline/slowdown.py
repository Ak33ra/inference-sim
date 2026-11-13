"""Compare loaded family and fork durations with the published optrouting baselines."""

import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path
import statistics
import sys

from analyze import distribution

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))
from research.outputs.optrouting.optimal_isolated import observed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="*", type=Path, default=[
        ROOT / f"research/outputs/semiclosed/mega-8b200-poisson-12s-seed{seed}"
        for seed in range(42, 47)])
    parser.add_argument("--output", type=Path, default=
                        ROOT / "research/outputs/analysis/mega-8b200-baseline/slowdown")
    args = parser.parse_args()
    trace = ROOT / "traces/mega-commit0-oolong"
    manifest = json.loads((trace / "manifest.json").read_text())
    composition = json.loads((trace / "composition.json").read_text())
    names = {f["family_id"]: f["source_family_id"] for f in composition["families"]}
    assert len(set(names.values())) == len(names), "analysis expects one copy of each source family"
    baselines, hashes = {}, {}
    for dataset, directory in (("commit0", "data"), ("oolong", "data/oolong")):
        for kind, filename in (("family", "root_e2e.csv"), ("fork", "forks.csv")):
            path = ROOT / "research/outputs/optrouting" / directory / filename
            hashes[str(path.relative_to(ROOT))] = hashlib.sha256(path.read_bytes()).hexdigest()
            with path.open() as handle:
                for row in csv.DictReader(handle):
                    key = (kind, row["family"], int(row["spawn_request"]) if kind == "fork" else None)
                    assert key not in baselines
                    baselines[key] = {**row, "dataset": dataset}

    details, runs = [], []
    for path in args.runs:
        path = path.resolve()
        workload = json.loads((path / "workload.json").read_text())
        proof = json.loads((path / "verification.json").read_text())
        assert workload["complete"] and workload["accounting_match"] and proof["verified"]
        assert proof["source_totals"] == manifest["source_totals"]
        assert workload["source"]["source"]["sha256"] == manifest["sha256"]
        assert set(f["trace_id"] for f in workload["families"]) == set(names)
        forks, families = observed(path / "workload.json")
        families = {names[k]: v for k, v in families.items()}
        forks = {(names[k[0]], k[1]): v for k, v in forks.items()}
        requests = {r["request_id"]: r for r in workload["requests"]}
        sessions = {s["session_id"]: s for s in workload["sessions"]}
        children = collections.defaultdict(set)
        spawns = {}
        for r in requests.values():
            assert r["state"] == "completed"
            if r["session_id"] == r["trace_id"]:
                continue
            for parent in r["prerequisites"]:
                spawn = requests[parent["request_id"]]
                if spawn["session_id"] == r["trace_id"]:
                    key = (names[r["trace_id"]], int(spawn["request_id"].rsplit("/", 1)[1]))
                    children[key].add(r["session_id"])
                    spawns[key] = spawn
        selected = []
        for (kind, family, spawn_id), baseline in baselines.items():
            optimal = float(baseline["optimal_s"])
            assert optimal > 0
            if kind == "fork":
                key = (family, spawn_id)
                loaded = forks[key]
                assert len(children[key]) == int(baseline["children"])
                independent = (max(sessions[s]["completion_us"] for s in children[key])
                               - spawns[key]["completion_us"]) / 1e6
                assert abs(loaded - independent) < 1e-9
            else:
                loaded = families[family]
            selected.append({"run": str(path), "seed": workload["arrivals"]["seed"],
                             "dataset": baseline["dataset"], "kind": kind, "family": family,
                             "spawn_request": spawn_id, "children": int(baseline.get("children", 0)),
                             "optimal_s": optimal, "loaded_s": loaded, "slowdown": loaded / optimal})
        details.extend(selected)
        groups = {}
        for dataset in ("all", "commit0", "oolong"):
            subset = [r for r in selected if dataset == "all" or r["dataset"] == dataset]
            groups[dataset] = {}
            for group in ("families", "forks_all", "forks_multi_child"):
                rows = [r for r in subset if (r["kind"] == "family" if group == "families"
                        else r["kind"] == "fork" and (group == "forks_all" or r["children"] > 1))]
                groups[dataset][group] = {**distribution(r["slowdown"] for r in rows),
                                         "below_one": sum(r["slowdown"] < 1 for r in rows),
                                         "max_deficit_s": max(0, max(r["optimal_s"] - r["loaded_s"] for r in rows))}
        runs.append({"output": str(path), "seed": workload["arrivals"]["seed"], "statistics": groups})

    aggregate = {}
    for dataset, groups in runs[0]["statistics"].items():
        aggregate[dataset] = {}
        for group, stats in groups.items():
            aggregate[dataset][group] = {}
            for stat in stats:
                values = [run["statistics"][dataset][group][stat] for run in runs]
                aggregate[dataset][group][stat] = {
                    "median": statistics.median(values), "min": min(values), "max": max(values)}
    args.output.mkdir(parents=True, exist_ok=True)
    for kind in ("family", "fork"):
        with (args.output / f"{kind}-slowdowns.csv").open("w") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(details[0]))
            writer.writeheader()
            writer.writerows(r for r in details if r["kind"] == kind)
    result = {"definition": "loaded duration / published optimal_s; forks start at spawn completion",
              "aggregation": "per-object ratios, per-run statistics, then median and range across runs",
              "input_sha256": manifest["sha256"], "baseline_sha256": hashes,
              "unmatched_families": sorted(set(names.values()) - {k[1] for k in baselines if k[0] == "family"}),
              "runs": runs, "aggregate": aggregate}
    (args.output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(aggregate["all"], indent=2))


if __name__ == "__main__":
    main()
