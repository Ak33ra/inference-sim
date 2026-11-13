"""Compare the saved 8s and 12s Poisson mega-trace experiments."""

import csv
import json
from pathlib import Path
import statistics


ROOT = Path(__file__).resolve().parents[3]
OUTPUT = ROOT / "research/outputs/analysis/mega-8b200-8s"
BASELINE = ROOT / "research/outputs/analysis/mega-8b200-baseline"


def summarize(values):
    values = list(values)
    return {"median": statistics.median(values), "min": min(values), "max": max(values)}


def main():
    runs = {
        "12s": json.loads((BASELINE / "baseline-summary.json").read_text()),
        "8s": json.loads((OUTPUT / "summary.json").read_text()),
    }
    slowdown = {gap: json.loads((directory / "slowdown/summary.json").read_text())
                for gap, directory in (("12s", BASELINE), ("8s", OUTPUT))}
    for gap, values in runs.items():
        assert sorted(r["arrivals"]["seed"] for r in values) == list(range(42, 47))
        assert all(r["arrivals"]["rate_per_s"] == 1 / int(gap[:-1]) for r in values)
        assert all(r["verified"] for r in values)
    old_by_seed = {r["arrivals"]["seed"]: r for r in runs["12s"]}
    for new in runs["8s"]:
        old = old_by_seed[new["arrivals"]["seed"]]
        for key in ("source_totals", "input_sha256", "provenance"):
            assert old[key] == new[key], (key, new["arrivals"]["seed"])
        a = json.loads((ROOT / old["output"] / "config.resolved.json").read_text())
        b = json.loads((ROOT / new["output"] / "config.resolved.json").read_text())
        a["serving"]["router"].pop("saved_config")
        b["serving"]["router"].pop("saved_config")
        for key in ("serving", "capacity", "catalog", "registry", "defaults"):
            assert a[key] == b[key], key
        a["run"]["arrivals"].pop("rate_per_s")
        b["run"]["arrivals"].pop("rate_per_s")
        assert a["run"] == b["run"]
        old_workload = json.loads((ROOT / old["output"] / "workload.json").read_text())
        new_workload = json.loads((ROOT / new["output"] / "workload.json").read_text())
        old_births = {f["trace_id"]: f["birth_us"] for f in old_workload["families"]}
        new_births = {f["trace_id"]: f["birth_us"] for f in new_workload["families"]}
        assert old_births.keys() == new_births.keys()
        assert all(abs(new_births[k] - old_births[k] * 2 / 3) <= 1 for k in old_births)
    assert slowdown["12s"]["baseline_sha256"] == slowdown["8s"]["baseline_sha256"]

    metrics = [
        ("request_active_pct", ("request_active_fraction",), 100),
        ("full_run_active_pct", ("full_run_active_fraction",), 100),
        ("ttft_mean_ms", ("all_ttft_ms", "mean"), 1),
        ("ttft_median_ms", ("all_ttft_ms", "p50"), 1),
        ("ttft_p99_ms", ("all_ttft_ms", "p99"), 1),
        ("scheduling_p95_ms", ("window_scheduling_ms", "p95"), 1),
        ("family_mean_s", ("family_makespan_s", "mean"), 1),
        ("family_median_s", ("family_makespan_s", "p50"), 1),
        ("family_p90_s", ("family_makespan_s", "p90"), 1),
        ("family_p99_s", ("family_makespan_s", "p99"), 1),
        ("peak_active_families", ("peak_active_families",), 1),
        ("waiting_per_replica", ("window_waiting_per_replica", "mean"), 1),
    ]
    aggregate, per_seed = {"12s": {}, "8s": {}}, []
    for gap, values in runs.items():
        for run in values:
            row = {"gap": gap, "seed": run["arrivals"]["seed"]}
            for name, keys, scale in metrics:
                value = run
                for key in keys:
                    value = value[key]
                row[name] = value * scale
            per_seed.append(row)
        for name, _, _ in metrics:
            aggregate[gap][name] = summarize(r[name] for r in per_seed if r["gap"] == gap)
        aggregate[gap]["slowdown"] = slowdown[gap]["aggregate"]["all"]
    with (OUTPUT / "comparison.csv").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(per_seed[0]))
        writer.writeheader()
        writer.writerows(per_seed)
    (OUTPUT / "comparison.json").write_text(json.dumps(aggregate, indent=2) + "\n")
    for name, _, _ in metrics:
        print(name, *(f"{gap}: {aggregate[gap][name]['median']:.6g} "
                      f"[{aggregate[gap][name]['min']:.6g}, {aggregate[gap][name]['max']:.6g}]"
                      for gap in ("12s", "8s")))
    for group in ("families", "forks_multi_child", "forks_all"):
        for stat in ("mean", "p50", "p90", "p99"):
            print(group, stat, *(f"{gap}: {aggregate[gap]['slowdown'][group][stat]['median']:.6g}"
                                 for gap in ("12s", "8s")))


if __name__ == "__main__":
    main()
