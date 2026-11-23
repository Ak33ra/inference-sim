"""Analyze five paired seeds with shared best-observed isolated-family references."""

from __future__ import annotations

import argparse
from collections import defaultdict
from pathlib import Path
import statistics
import sys

from research.policy_study.analysis import distribution, write_csv
from research.policy_study.run import completed, write_json
from .common import (read_json, require, runtime_id,
                     validate_provenance, verify_run)

STATISTICS = ("mean", "p50", "p99")


def object_rows(run: dict, work: dict, manifest: dict) -> tuple[list[dict], list[dict]]:
    common = {"run_id": run["id"], "router": run["router"], "arrival_seed": run["arrival_seed"]}
    sessions, families = [], []
    for row in work["sessions"]:
        key = runtime_id(row)
        reference = manifest["sessions"][key]
        elapsed = row["completion_us"] - row["arrival_us"]
        sessions.append({**common, "runtime_session_id": key, "trace_id": row["trace_id"],
                         "session_id": row["session_id"], "class": reference["class"],
                         "arrival_us": row["arrival_us"], "completion_us": row["completion_us"],
                         "elapsed_us": elapsed, "reference_elapsed_us": reference["reference_elapsed_us"],
                         "reference_run": reference["reference_run"],
                         "slowdown": elapsed / reference["reference_elapsed_us"]})
    for row in work["families"]:
        reference = manifest["families"][row["trace_id"]]
        families.append({**common, "trace_id": row["trace_id"], "birth_us": row["birth_us"],
                         "arrival_us": row["arrival_us"], "completion_us": row["completion_us"],
                         "elapsed_us": row["makespan_us"], "reference_elapsed_us": reference["reference_elapsed_us"],
                         "reference_run": reference["reference_run"],
                         "slowdown": row["makespan_us"] / reference["reference_elapsed_us"]})
    return sessions, families


def per_run_statistics(run: dict, sessions: list[dict], families: list[dict]) -> list[dict]:
    result = []
    for unit, cohort, rows in [("session", "all", sessions),
                                ("session", "short", [r for r in sessions if r["class"] == "short"]),
                                ("session", "long", [r for r in sessions if r["class"] == "long"]),
                                ("family", "all", families)]:
        stats = distribution(row["slowdown"] for row in rows)
        for name in STATISTICS:
            result.append({"run_id": run["id"], "router": run["router"], "arrival_seed": run["arrival_seed"],
                           "unit": unit, "cohort": cohort, "statistic": name,
                           "count": len(rows), "value": stats[name],
                           "below_one_count": sum(row["slowdown"] < 1 for row in rows)})
    return result


def aggregate(rows: list[dict], keys: tuple[str, ...], value: str) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[key] for key in keys)].append(row[value])
    return [{**dict(zip(keys, key)), "runs": len(values), "median": statistics.median(values),
             "min": min(values), "max": max(values)} for key, values in sorted(groups.items())]


def paired_statistics(rows: list[dict]) -> list[dict]:
    by_key = {(row["router"], row["arrival_seed"], row["unit"], row["cohort"], row["statistic"]): row
              for row in rows}
    result = []
    for key, candidate in sorted(by_key.items()):
        router, seed, unit, cohort, statistic = key
        if router != "session-reservation":
            continue
        baseline = by_key[("jsq", seed, unit, cohort, statistic)]["value"]
        value = candidate["value"]
        result.append({"arrival_seed": seed, "unit": unit, "cohort": cohort, "statistic": statistic,
                       "baseline": baseline, "candidate": value, "ratio": value / baseline,
                       "percent_change": (value / baseline - 1) * 100})
    return result


def paired_sessions(rows: list[dict]) -> list[dict]:
    by_key = {(row["router"], row["arrival_seed"], row["runtime_session_id"]): row for row in rows}
    result = []
    for key, candidate in sorted(by_key.items()):
        router, seed, session = key
        if router != "session-reservation":
            continue
        base = by_key[("jsq", seed, session)]
        ratio = candidate["slowdown"] / base["slowdown"]
        result.append({"arrival_seed": seed, "runtime_session_id": session,
                       "trace_id": candidate["trace_id"], "session_id": candidate["session_id"],
                       "class": candidate["class"], "reference_elapsed_us": candidate["reference_elapsed_us"],
                       "baseline_elapsed_us": base["elapsed_us"], "candidate_elapsed_us": candidate["elapsed_us"],
                       "baseline_slowdown": base["slowdown"], "candidate_slowdown": candidate["slowdown"],
                       "paired_ratio": ratio, "percent_change": (ratio - 1) * 100})
    return result


def markdown(summary: dict) -> str:
    lines = ["# Session reservation experiment", "",
             "Eight B200 replicas, FCFS, prefix caching off, 4-second Poisson root gaps. "
             "Arrival seeds 42–46 are paired; every simulation uses seed 42 and drains all work.", "",
             "The shortest 954 of 2,385 sessions can use all eight replicas. The remaining 1,431 "
             "cannot use instance_0 or instance_1. Classes use elapsed session lifetime, including tool and dependency waits.", "",
             "Slowdown divides loaded lifetime by the shared minimum observed across 35 old cache-off isolated-family runs. "
             "Sibling sessions still compete in those references. This is an approximate best-observed reference, "
             "not a session-alone optimum; ratios below one are retained. No candidate reference is fitted.", "",
             "Each cell gives the median [minimum, maximum] of five per-run statistics. Quantiles use linear interpolation.", "",
             "| Unit / cohort | Statistic | JSQ | Reservation | Paired change (%) |", "|---|---|---:|---:|---:|"]
    aggregate_index = {(r["router"], r["unit"], r["cohort"], r["statistic"]): r for r in summary["aggregate"]}
    change_index = {(r["unit"], r["cohort"], r["statistic"]): r for r in summary["paired_percent_changes"]}
    def cell(row):
        return f"{row['median']:.4f} [{row['min']:.4f}, {row['max']:.4f}]"
    for unit, cohort in [("session", "all"), ("session", "short"), ("session", "long"), ("family", "all")]:
        for statistic in STATISTICS:
            base = aggregate_index[("jsq", unit, cohort, statistic)]
            candidate = aggregate_index[("session-reservation", unit, cohort, statistic)]
            changes = change_index[(unit, cohort, statistic)]
            lines.append(f"| {unit} / {cohort} | {statistic} | {cell(base)} | {cell(candidate)} | {cell(changes)} |")
    lines += ["", "Negative percent changes mean lower slowdown. Paired changes compare each arrival seed's cohort "
              "statistic, then summarize the five changes. They are separate from the ratio of aggregate medians "
              "saved in aggregate-median-ratios.csv. Paired session ratios are in paired-sessions.csv.", "",
              "Families are reported overall because a family can contain both session classes. "
              "Its reference remains the minimum family makespan across the same 35 isolated-family runs.", "",
              "Both conditions passed complete-work accounting, identical paired root arrivals and logical work, "
              "and request-by-request routing verification. The new JSQ controls reproduce the old workload, "
              "decision audit, verification and metrics, allowing only catalog.path to differ.", ""]
    return "\n".join(lines)


def analyze(path: Path) -> Path:
    study = read_json(path / "study.json")
    validate_provenance(study)
    manifest = read_json(Path(study["session_class_manifest"]))
    sessions, families, per_run = [], [], []
    for run in study["runs"]:
        require(completed(run, study["binary_sha256"]), f"incomplete run: {run['id']}")
        verify_run(run, study, manifest)
        session_rows, family_rows = object_rows(run, read_json(Path(run["output"]) / "workload.json"), manifest)
        sessions.extend(session_rows)
        families.extend(family_rows)
        per_run.extend(per_run_statistics(run, session_rows, family_rows))
    stats = aggregate(per_run, ("router", "unit", "cohort", "statistic"), "value")
    deltas = paired_statistics(per_run)
    changes = aggregate(deltas, ("unit", "cohort", "statistic"), "percent_change")
    indexed = {(row["router"], row["unit"], row["cohort"], row["statistic"]): row for row in stats}
    ratios = []
    for row in stats:
        if row["router"] == "session-reservation":
            key = ("jsq", row["unit"], row["cohort"], row["statistic"])
            ratio = row["median"] / indexed[key]["median"]
            ratios.append({"unit": row["unit"], "cohort": row["cohort"], "statistic": row["statistic"],
                           "ratio_of_aggregate_medians": ratio, "percent_change": (ratio - 1) * 100})
    summary = {"format_version": 1, "reference": study["reference"],
               "classification_sha256": study["session_class_manifest_sha256"],
               "aggregate": stats, "paired_percent_changes": changes,
               "aggregate_median_ratios": ratios,
               "counts": {"runs": len(study["runs"]), "session_rows": len(sessions), "family_rows": len(families)}}
    output = path / "analysis"
    output.mkdir(exist_ok=True)
    for name, rows in (("sessions", sessions), ("families", families), ("per-run", per_run),
                       ("aggregate", stats), ("paired-deltas", deltas), ("paired-changes-summary", changes),
                       ("paired-sessions", paired_sessions(sessions)), ("aggregate-median-ratios", ratios)):
        write_csv(output / f"{name}.csv", rows)
    write_json(output / "summary.json", summary)
    (output / "report.md").write_text(markdown(summary))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", type=Path)
    args = parser.parse_args()
    try:
        print(analyze(args.study.resolve()))
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(f"session reservation analysis: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
