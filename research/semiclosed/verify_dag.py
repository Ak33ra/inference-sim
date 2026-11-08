"""Verify semi-closed explicit-DAG replay against its original Weka requests."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
import math
from pathlib import Path
import subprocess
import sys


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def index(rows: list[dict], key, label: str) -> dict:
    result = {key(row): row for row in rows}
    require(len(result) == len(rows), f"duplicate {label}")
    return result


def micros(seconds: float) -> int:
    return math.floor(seconds * 1e6 + 0.5)


def verify(input_path: Path, report_path: Path, metrics_path: Path) -> dict:
    data = (subprocess.run(["zstd", "-dc", str(input_path)], check=True, capture_output=True).stdout
            if input_path.suffix == ".zst" else input_path.read_bytes())
    rows = [json.loads(line) for line in data.splitlines() if line.strip()]
    families = index(rows, lambda row: row["id"], "source family")
    require(bool(families), "empty DAG input")
    raw, sessions, by_request_id = {}, defaultdict(list), {}

    def collect(family: str, session: str, requests: list[dict]) -> None:
        require((family, session) not in sessions, "duplicate source session")
        sessions[family, session] = []
        step = 0
        for request in requests:
            if request["type"] == "subagent":
                collect(family, request["agent_id"], request["requests"])
                continue
            key = family, session, step
            ident = family, request["req_id"]
            require(ident not in by_request_id, "duplicate source req_id")
            by_request_id[ident] = key
            raw[key] = request
            sessions[family, session].append(key)
            step += 1

    for family, row in families.items():
        collect(family, family, row["requests"])
    report = json.loads(report_path.read_bytes())
    engine = json.loads(metrics_path.read_bytes())
    require(report["complete"] and report["accounting_match"] and report["engine_available"],
            "run is incomplete or accounting does not match")
    require(report["join_policy"] == "explicit", "DAG dependencies were not explicit")
    require(report["source"]["source"] == {
        "format": "weka-dag", "digest_encoding": "uncompressed",
        "sha256": hashlib.sha256(data).hexdigest()}, "source digest differs")
    observed = index(report["requests"], lambda row: (row["trace_id"], row["session_id"], row["step"]), "reported request")
    require(observed.keys() == raw.keys(), "reported work differs from source")
    metrics = index(engine["requests"], lambda row: row["requestID"], "engine request")
    require(set(metrics) == {row["request_id"] for row in observed.values()}, "engine request IDs differ")
    totals = {
        "families": len(families), "sessions": len(sessions), "requests": len(raw),
        "prompt_tokens": sum(row["in"] for row in raw.values()),
        "output_tokens": sum(row["out"] for row in raw.values()),
        "zero_output_requests": sum(row["out"] == 0 for row in raw.values()),
    }
    require(report["selected"] == totals, "source totals differ")
    require(report["engine"] == {"completed_requests": len(raw), "prompt_tokens": totals["prompt_tokens"],
                                 "output_tokens": totals["output_tokens"]}, "reported engine totals differ")
    for field, expected in {
        "completed_requests": len(raw), "injected_requests": len(raw),
        "total_input_tokens": totals["prompt_tokens"], "total_output_tokens": totals["output_tokens"],
        "still_queued": 0, "still_running": 0, "dropped_unservable": 0,
        "length_capped_requests": 0, "timed_out_requests": 0,
    }.items():
        require(engine[field] == expected, f"engine {field} differs")
    reported_families = index(report["families"], lambda row: row["trace_id"], "reported family")
    require(reported_families.keys() == families.keys(), "reported family set differs")
    roots = defaultdict(list)
    for key, request in raw.items():
        if not request["parent_ids"]:
            roots[key[0]].append(key)
    origin = {family: min(micros(raw[key]["t"]) for key in keys) for family, keys in roots.items()}
    edges = 0
    for key, request in raw.items():
        actual = observed[key]
        metric = metrics[actual["request_id"]]
        require(actual["state"] == "completed", f"unfinished request {key}")
        require(actual["start_ms"] == micros(request["t"]) // 1000
                and actual["end_ms"] == micros(request["t"] + request["api_time"]) // 1000,
                f"source timestamps differ {key}")
        require(actual["prompt_tokens"] == request["in"] and actual["output_tokens"] == request["out"], f"changed tokens {key}")
        require(metric["num_prefill_tokens"] == request["in"] and metric["num_decode_tokens"] == request["out"]
                and metric.get("output_tokens", 0) == request["out"], f"engine tokens differ {key}")
        require(abs(metric["arrived_at"] * 1e6 - actual["arrival_us"]) < 0.01, f"engine arrival differs {key}")
        require(actual["e2e_us"] == actual["completion_us"] - actual["arrival_us"], f"request duration differs {key}")
        require(abs(metric["e2e_ms"] * 1000 - actual["engine_e2e_us"]) < 0.01
                and abs(actual["arrival_us"] + actual["engine_e2e_us"] - actual["engine_completion_us"]) < 0.01,
                f"engine completion differs {key}")
        if "pd" not in engine:
            require(abs(actual["completion_us"] - actual["engine_completion_us"]) < 0.01,
                    f"lifecycle completion differs from engine {key}")
        parents = [by_request_id[key[0], parent] for parent in request["parent_ids"]]
        prerequisites = [{"request_id": observed[parent]["request_id"], "on": "completion"} for parent in parents]
        require(actual["prerequisites"] == prerequisites, f"changed dependency edges {key}")
        edges += len(parents)
        delay = 0
        if parents:
            latest = max(micros(raw[parent]["t"] + raw[parent]["api_time"]) for parent in parents)
            delay = micros(request["t"]) - latest
            due = max(observed[parent]["completion_us"] for parent in parents) + delay
        else:
            due = reported_families[key[0]]["birth_us"] + micros(request["t"]) - origin[key[0]]
        require(delay >= 0 and actual["delay_us"] == delay and actual["arrival_us"] == due,
                f"dependency timing differs {key}")
    reported_sessions = index(report["sessions"], lambda row: (row["trace_id"], row["session_id"]), "reported session")
    require(reported_sessions.keys() == sessions.keys(), "reported session set differs")
    for key, members in sessions.items():
        session = reported_sessions[key]
        require(session["arrival_us"] == min(observed[m]["arrival_us"] for m in members)
                and session["completion_us"] == max(observed[m]["completion_us"] for m in members), f"session lifetime differs {key}")
    arrivals = report["arrivals"]
    events = []
    previous = 0
    for ordinal, family in enumerate(families):
        result = reported_families[family]
        members = [row for key, row in observed.items() if key[0] == family]
        first, last = min(r["arrival_us"] for r in members), max(r["completion_us"] for r in members)
        require(result["arrival_us"] == first and result["completion_us"] == last
                and result["makespan_us"] == last - first, f"family lifetime differs {family}")
        birth = result["birth_us"]
        if arrivals["mode"] == "fixed":
            require(birth == micros(ordinal * arrivals.get("interval_s", 0)), "fixed arrivals differ")
        elif arrivals["mode"] == "trace":
            require(birth == origin[family], "trace arrivals differ")
        else:
            require(arrivals["mode"] == "poisson" and birth >= previous and (ordinal != 0 or birth == 0), "invalid Poisson schedule")
        previous = birth
        events.extend([(first, 1), (last, -1)])
    active = peak = 0
    for _, change in sorted(events):
        active += change
        peak = max(active, peak)
    return {"verified": True, "source_totals": totals, "dependency_edges": edges,
            "peak_active_families": peak, "arrivals": arrivals}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--metrics", type=Path, required=True)
    args = parser.parse_args()
    try:
        print(json.dumps(verify(args.input, args.report, args.metrics), indent=2))
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f"DAG verification failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
