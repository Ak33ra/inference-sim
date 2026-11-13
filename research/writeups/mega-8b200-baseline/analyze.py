"""Measure the complete mega workload and its load during root arrivals."""

import collections
import json
import math
from pathlib import Path
import statistics
import subprocess


ROOT = Path(__file__).resolve().parents[3]
TRACE = ROOT / "traces/mega-commit0-oolong"


def distribution(values):
    values = sorted(values)
    if not values:
        return {"count": 0}
    def percentile(q):
        i = (len(values) - 1) * q
        lo = int(i)
        hi = min(lo + 1, len(values) - 1)
        return values[lo] + (values[hi] - values[lo]) * (i - lo)
    return {"count": len(values), "mean": statistics.mean(values), "min": values[0],
            "p50": percentile(.5), "p90": percentile(.9), "p95": percentile(.95),
            "p99": percentile(.99), "max": values[-1]}


def occupied(intervals, start, end):
    covered, previous = 0, start
    for a, b in sorted(intervals):
        a, b = max(start, a), min(end, b)
        if b > a:
            covered += max(0, b - max(a, previous))
            previous = max(previous, b)
    return covered


def fork_groups():
    data = subprocess.run(["zstd", "-dc", str(TRACE / "input.jsonl.zst")], capture_output=True, check=True).stdout
    groups, excluded = [], collections.Counter()
    for family in map(json.loads, data.splitlines()):
        sessions = {}
        def collect(sid, entries, parent=None):
            sessions[sid] = ([r for r in entries if r["type"] != "subagent"], parent)
            for entry in entries:
                if entry["type"] == "subagent":
                    collect(entry["agent_id"], entry["requests"], sid)
        collect(family["id"], family["requests"])
        indexed = collections.defaultdict(list)
        for sid, (requests, parent) in sessions.items():
            if parent is None:
                continue
            if len(requests[0]["parent_ids"]) != 1:
                excluded["child_with_multiple_or_no_spawn_parents"] += 1
                continue
            if not all(requests[i - 1]["req_id"] in requests[i]["parent_ids"] for i in range(1, len(requests))):
                excluded["child_without_sequential_chain"] += 1
                continue
            joins = [r["req_id"] for r in sessions[parent][0] if requests[-1]["req_id"] in r["parent_ids"]]
            if len(joins) != 1:
                excluded["child_without_single_explicit_join"] += 1
                continue
            indexed[requests[0]["parent_ids"][0], joins[0]].append(sid)
        for (spawn, join), children in indexed.items():
            if len(children) < 2:
                excluded["single_child_group"] += 1
            else:
                groups.append({"family": family["id"], "spawn": spawn, "join": join, "children": children})
    return groups, dict(excluded)


def analyze(path):
    path = Path(path)
    workload = json.loads((path / "workload.json").read_text())
    metrics = json.loads((path / "metrics.json").read_text())
    verification = json.loads((path / "verification.json").read_text())
    config = json.loads((path / "config.resolved.json").read_text())
    manifest = json.loads((TRACE / "manifest.json").read_text())
    assert workload["complete"] and workload["accounting_match"] and verification["verified"]
    assert verification["source_totals"] == manifest["source_totals"]
    assert workload["source"]["source"]["sha256"] == manifest["sha256"]
    assert metrics["completed_requests"] == manifest["source_totals"]["requests"]
    assert metrics["total_input_tokens"] == manifest["source_totals"]["prompt_tokens"]
    assert metrics["total_output_tokens"] == manifest["source_totals"]["output_tokens"]
    for key in ("still_queued", "still_running", "dropped_unservable", "length_capped_requests",
                "timed_out_requests", "cancelled_requests", "context_overflow_requests", "failed_requests"):
        assert metrics[key] == 0, (path, key, metrics[key])
    assert metrics["preemption_count"] == 0, "preemptions invalidate first-scheduling interval reconstruction"
    assert config["serving"]["cluster"]["replicas"] == 8
    intervals = collections.defaultdict(list)
    for request in metrics["requests"]:
        begin = request["arrived_at"] + request["scheduling_delay_ms"] / 1000
        end = request["arrived_at"] + request["e2e_ms"] / 1000
        assert begin <= end
        intervals[request["handled_by"]].append((begin, end))
    assert len(intervals) == 8
    last_birth = max(f["birth_us"] for f in workload["families"]) / 1e6
    end = workload["end_us"] / 1e6
    series = metrics["server_series"]
    bin_width = series["interval_s"]
    start_bin = math.ceil(.1 * last_birth / bin_width)
    end_bin = math.floor(.9 * last_birth / bin_width)
    window_start, window_end = start_bin * bin_width, end_bin * bin_width
    assert window_end > window_start
    cohort = [r for r in metrics["requests"] if window_start <= r["arrived_at"] < window_end]
    active = {replica: occupied(v, window_start, window_end) / (window_end - window_start)
              for replica, v in sorted(intervals.items())}
    waiting = [sum(e["num_requests_waiting"][i] for e in series["engines"]) / 8
               for i in range(start_bin, end_bin)]
    running = [sum(e["num_requests_running"][i] for e in series["engines"]) / 8
               for i in range(start_bin, end_bin)]
    blocks = []
    for i in range(4):
        a, b = i * last_birth / 4, (i + 1) * last_birth / 4
        families = [f for f in workload["families"] if a <= f["birth_us"] / 1e6 < b or i == 3 and f["birth_us"] / 1e6 == b]
        blocks.append({"start_s": a, "end_s": b, "active_fraction":
                       sum(occupied(v, a, b) for v in intervals.values()) / (8 * (b - a)),
                       "root_families": len(families), "source_counts": dict(collections.Counter(
                           f["trace_id"].split("/")[0] for f in families))})
    grouped = collections.defaultdict(list)
    for r in workload["requests"]:
        grouped[r["trace_id"], r["session_id"]].append(r)
    groups, excluded = fork_groups()
    forks = []
    for g in groups:
        children = [grouped[g["family"], child] for child in g["children"]]
        origin = min(r["arrival_us"] for child in children for r in child)
        finishes = [(max(r["completion_us"] for r in child) - origin) / 1e6 for child in children]
        span, mean, median = max(finishes), statistics.mean(finishes), statistics.median(finishes)
        forks.append({**g, "makespan_s": span, "mean_finish_s": mean, "median_finish_s": median,
                      "ratio_mean": span / mean, "ratio_median": span / median,
                      "gap_mean_s": span - mean, "gap_median_s": span - median})
    per_source = collections.defaultdict(list)
    for family in workload["families"]:
        per_source[family["trace_id"].split("/")[0]].append(family["makespan_us"] / 1e6)
    return {"output": str(path), "arrivals": workload["arrivals"], "verified": True,
            "source_totals": verification["source_totals"], "input_sha256": manifest["sha256"],
            "last_root_arrival_s": last_birth, "drained_at_s": end,
            "measurement_window_s": [window_start, window_end], "window_requests": len(cohort),
            "request_active_fraction": statistics.mean(active.values()), "per_replica_active_fraction": active,
            "full_run_active_fraction": sum(occupied(v, 0, end) for v in intervals.values()) / (8 * end),
            "window_running_per_replica": distribution(running), "window_waiting_per_replica": distribution(waiting),
            "quarters": blocks, "window_scheduling_ms": distribution(r["scheduling_delay_ms"] for r in cohort),
            "window_ttft_ms": distribution(r["ttft_ms"] for r in cohort),
            "all_scheduling_ms": distribution(r["scheduling_delay_ms"] for r in metrics["requests"]),
            "all_ttft_ms": distribution(r["ttft_ms"] for r in metrics["requests"]),
            "family_makespan_s": distribution(f["makespan_us"] / 1e6 for f in workload["families"]),
            "family_makespan_by_source_s": {source: distribution(values) for source, values in per_source.items()},
            "fork_statistics": {k: distribution(g[k] for g in forks) for k in
                                ("makespan_s", "mean_finish_s", "median_finish_s", "ratio_mean", "ratio_median", "gap_mean_s", "gap_median_s")},
            "fork_count": len(forks), "fork_exclusions": excluded,
            "peak_active_families": verification["peak_active_families"],
            "preemptions": metrics["preemption_count"], "cache_hit_rate": metrics["cache_hit_rate"],
            "provenance": {k: metrics[k] for k in ("blis_version", "kernel_version", "schemas_version", "catalog")}}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("runs", nargs="+", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    summaries = [analyze(path) for path in args.runs]
    args.output.write_text(json.dumps(summaries, indent=2) + "\n")
    for s in summaries:
        print(json.dumps({"run": s["output"], "rate": s["arrivals"].get("rate_per_s"),
                          "active": s["request_active_fraction"], "schedule_p95_ms": s["window_scheduling_ms"]["p95"],
                          "waiting_mean": s["window_waiting_per_replica"]["mean"],
                          "family_p50_s": s["family_makespan_s"]["p50"], "forks": s["fork_count"]}))
