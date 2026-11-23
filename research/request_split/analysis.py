"""Paired elapsed makespan and slowdown tables for the five request-SPLIT seeds."""

import argparse
from pathlib import Path
from collections import defaultdict
import statistics
import sys

from research.policy_study.analysis import distribution, write_csv
from research.policy_study.run import completed, write_json
from .common import read_json, require, runtime_id, validate_provenance, verify_run

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


def request_rows(run, work, metrics, oracle, audit):
    engine = {r["requestID"]: r for r in metrics["requests"]}
    claims = {e["request_id"]: e for e in audit.get("events", []) if e["kind"] == "claim"}
    rows = []
    for row in work["requests"]:
        ident = row["request_id"]
        metric = engine[ident]
        shape = oracle["shapes"][oracle["requests"][ident]["shape"]]
        scheduled = row["arrival_us"] + round(metric["scheduling_delay_ms"] * 1000)
        elapsed = row["completion_us"] - row["arrival_us"]
        claim = claims.get(ident)
        rows.append({"run_id": run["id"], "router": run["router"], "arrival_seed": run["arrival_seed"],
                     "request_id": ident, "trace_id": row["trace_id"], "runtime_session_id": runtime_id(row),
                     "step": row["step"], "prompt_tokens": row["prompt_tokens"], "output_tokens": row["output_tokens"],
                     "arrival_us": row["arrival_us"], "completion_us": row["completion_us"], "elapsed_us": elapsed,
                     "isolated_service_us": shape["service_us"], "isolated_ratio": elapsed / shape["service_us"],
                     "admission_us": scheduled, "preparation_service_us": shape["preparation_us"],
                     "additional_admission_wait_us": scheduled-row["arrival_us"]-shape["preparation_us"],
                     "post_admission_client_us": row["completion_us"]-scheduled,
                     "post_first_token_us": elapsed-row["ttft_us"], "handled_by": metric["handled_by"],
                     "preparation_wait_us": claim["preparation_wait_us"] if claim else None,
                     "central_wait_us": claim["at_us"]-claim["prepared_us"] if claim else None,
                     "ready_count_at_claim": claim["ready_count"] if claim else None,
                     "batch_size_at_claim": claim["capacity"]["batch_size"] if claim else None,
                     "batch_tokens_at_claim": claim["capacity"]["scheduled_tokens"] if claim else None})
    return rows


def aggregate(rows: list[dict], keys: tuple[str, ...], value: str) -> list[dict]:
    groups = defaultdict(list)
    for row in rows:
        groups[tuple(row[key] for key in keys)].append(row[value])
    return [{**dict(zip(keys, key)), "runs": len(values), "median": statistics.median(values),
             "min": min(values), "max": max(values)} for key, values in sorted(groups.items())]



def per_run_statistics(run, sessions, families):
    rows = []
    for unit, objects in (("session", sessions), ("family", families)):
        for metric in ("elapsed_us", "slowdown"):
            stats = distribution(row[metric] for row in objects)
            for name in ("mean", "p50", "p99"):
                rows.append({"run_id": run["id"], "router": run["router"], "arrival_seed": run["arrival_seed"],
                             "unit": unit, "metric": metric, "statistic": name, "count": len(objects), "value": stats[name]})
    return rows


def paired_statistics(rows):
    by_key = {(r["router"], r["arrival_seed"], r["unit"], r["metric"], r["statistic"]): r for r in rows}
    result = []
    for key, candidate in sorted(by_key.items()):
        router, seed, unit, metric, statistic = key
        if router != "request-split":
            continue
        baseline = by_key[("jsq", seed, unit, metric, statistic)]["value"]
        result.append({"arrival_seed": seed, "unit": unit, "metric": metric, "statistic": statistic,
                       "baseline": baseline, "candidate": candidate["value"],
                       "ratio": candidate["value"] / baseline,
                       "percent_change": 100 * (candidate["value"] / baseline - 1)})
    return result


def paired_objects(rows, identity):
    by_key = {(r["router"], r["arrival_seed"], r[identity]): r for r in rows}
    result = []
    for key, candidate in sorted(by_key.items()):
        router, seed, ident = key
        if router != "request-split":
            continue
        base = by_key[("jsq", seed, ident)]
        ratio = candidate["elapsed_us"] / base["elapsed_us"]
        result.append({"arrival_seed": seed, identity: ident, "reference_run": candidate["reference_run"],
                       "reference_elapsed_us": candidate["reference_elapsed_us"],
                       "baseline_elapsed_us": base["elapsed_us"], "candidate_elapsed_us": candidate["elapsed_us"],
                       "baseline_slowdown": base["slowdown"], "candidate_slowdown": candidate["slowdown"],
                       "paired_ratio": ratio, "percent_change": 100 * (ratio - 1)})
    return result


def markdown(summary):
    lines = ["# Request SPLIT experiment", "",
             "Eight B200 replicas run GPT-OSS-120B MXFP4 TP1 with prefix caching off and native FCFS batching. "
             "Family roots have four-second mean Poisson gaps, paired arrival seeds 42 through 46, and simulation seed 42.", "",
             "Replica 0 claims the ready request with the longest isolated service; replicas 1 through 7 claim the shortest. "
             "Requests finish without preemption, with logical lifecycle KV reservations. Dependency-blocked requests are absent "
             "from the queue; ready requests that cannot fit remain central.", "",
             "Preparation retains each replica's native renderer workers. SPLIT routes preparation round-robin across endpoints "
             "before central GPU admission; JSQ routes preparation with its chosen GPU. This ingress balancing change is part "
             "of the central architecture. Both policies charge the unchanged kernel preparation service once per request.", "",
             "Request size is exact client elapsed time for its input/output lengths under native single-request replay. It includes "
             "preparation and output host service, without tool delay or queue waiting. Priorities stay fixed after preparation. "
             "Session/family slowdown uses the shared best-observed isolated-family references, including sibling contention; "
             "ratios below one are retained.", "",
             "Cells show the median [minimum, maximum] across five per-run statistics. Quantiles use linear interpolation. "
             "Negative paired changes mean lower values.", "",
             "| Unit | Metric | Statistic | JSQ | SPLIT | Paired change (%) |",
             "|---|---|---|---:|---:|---:|"]
    indexed = {(r["router"], r["unit"], r["metric"], r["statistic"]): r for r in summary["aggregate"]}
    changes = {(r["unit"], r["metric"], r["statistic"]): r for r in summary["paired_percent_changes"]}
    def cell(row):
        return f"{row['median']:.4f} [{row['min']:.4f}, {row['max']:.4f}]"
    for key, change in sorted(changes.items()):
        unit, metric, statistic = key
        lines.append(f"| {unit} | {metric} | {statistic} | {cell(indexed[('jsq', *key)])} | "
                     f"{cell(indexed[('request-split', *key)])} | {cell(change)} |")
    lines += ["", "Every run passed full work accounting and paired root/DAG checks. New JSQ controls exactly reproduced "
              "the frozen controls after catalog.path normalization. The SPLIT audit independently reconstructs every ready-set "
              "minimum/maximum and verifies native renderer queues, admission timestamps, central delay, batching and zero preemption.", "",
              "Raw request diagnostics, per-session and per-family elapsed and slowdown rows, paired object ratios, per-run statistics, "
              "paired seed changes, reservation refusal counts and peak promises are saved beside this report."]
    return "\n".join(lines) + "\n"


def analyze(path):
    study = read_json(path / "study.json")
    validate_provenance(study)
    manifest = read_json(Path(study["session_class_manifest"]))
    sessions, families, requests, per_run, evidence = [], [], [], [], []
    oracle = read_json(Path(study["service_manifest"]))
    for run in study["runs"]:
        require(completed(run, study["binary_sha256"]), f"incomplete run: {run['id']}")
        evidence.append({"run_id": run["id"], **verify_run(run, study, manifest)})
        ss, fs = object_rows(run, read_json(Path(run["output"]) / "workload.json"), manifest)
        requests.extend(request_rows(run, read_json(Path(run["output"]) / "workload.json"),
                                     read_json(Path(run["output"]) / "metrics.json"), oracle,
                                     read_json(Path(run["output"]) / "router-decisions.json")))
        sessions.extend(ss)
        families.extend(fs)
        per_run.extend(per_run_statistics(run, ss, fs))
    stats = aggregate(per_run, ("router", "unit", "metric", "statistic"), "value")
    paired = paired_statistics(per_run)
    changes = aggregate(paired, ("unit", "metric", "statistic"), "percent_change")
    summary = {"format_version": 1, "reference": study["reference"], "aggregate": stats,
               "paired_percent_changes": changes, "verification": evidence,
               "counts": {"runs": len(study["runs"]), "session_rows": len(sessions), "family_rows": len(families), "request_rows": len(requests)}}
    output = path / "analysis"
    output.mkdir(exist_ok=True)
    for name, rows in (("sessions", sessions), ("families", families), ("requests", requests), ("per-run", per_run),
                       ("aggregate", stats), ("paired-deltas", paired), ("paired-changes-summary", changes),
                       ("paired-sessions", paired_objects(sessions, "runtime_session_id")),
                       ("paired-families", paired_objects(families, "trace_id"))):
        write_csv(output / f"{name}.csv", rows)
    write_json(output / "summary.json", summary)
    (output / "report.md").write_text(markdown(summary))
    return output


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("study", type=Path)
    p.add_argument("--report", type=Path)
    args = p.parse_args()
    try:
        output = analyze(args.study.resolve())
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text((output / "report.md").read_text())
        print(output)
        return 0
    except (OSError, ValueError, KeyError) as error:
        print(f"request split analysis: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
