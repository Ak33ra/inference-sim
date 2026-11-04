"""Check a completed semi-closed run against the original Parquet selection."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def unique(rows: list[dict], key, label: str) -> dict:
    indexed = {key(row): row for row in rows}
    require(len(indexed) == len(rows), f"duplicate {label}")
    return indexed


def read_source(source: Path, audits: list[dict]) -> tuple[dict, dict, dict]:
    import pyarrow.parquet as pq

    calls, sessions, roots = {}, {}, {}
    weeks = defaultdict(list)
    for audit in audits:
        weeks[audit["week"]].append(audit)
    for week, selected in weeks.items():
        tables = source / week / "processed"
        all_sessions = unique(
            pq.read_table(tables / "sessions.parquet", columns=[
                "session_id", "parent_session_id", "spawned_by_step", "spawned_by_call_index",
            ]).to_pylist(), lambda row: row["session_id"], "source session",
        )
        selected_roots = {a["root_session_id"]: a["trace_id"] for a in selected}
        members = {}
        for sid in all_sessions:
            current, seen = sid, set()
            while all_sessions[current]["parent_session_id"]:
                require(current not in seen, f"parent cycle in {week}/{sid}")
                seen.add(current)
                current = all_sessions[current]["parent_session_id"]
            if current in selected_roots:
                trace_id = selected_roots[current]
                members[sid] = trace_id
                sessions[trace_id, sid] = all_sessions[sid]
                roots[trace_id] = current
        for batch in pq.ParquetFile(tables / "requests.parquet").iter_batches(columns=[
            "session_id", "step", "start_ms", "end_ms", "prompt_tokens", "completion_tokens",
        ]):
            for row in batch.to_pylist():
                sid = row["session_id"]
                if sid not in members:
                    continue
                key = members[sid], sid, row["step"]
                require(key not in calls, f"duplicate source request {key}")
                calls[key] = row
    return calls, sessions, roots


def verify(input_dir: Path, source: Path, report_path: Path, metrics_path: Path) -> dict:
    manifest_bytes = (input_dir / "manifest.json").read_bytes()
    manifest = json.loads(manifest_bytes)
    for name in ("corpus.jsonl", "audit.jsonl"):
        artifact = input_dir / name
        with artifact.open("rb") as stream:
            digest = hashlib.file_digest(stream, "sha256").hexdigest()
        expected = manifest["artifacts"][name]
        require(digest == expected["sha256"] and artifact.stat().st_size == expected["bytes"],
                f"input artifact differs from manifest: {name}")
    for week in manifest["source"]["weeks"]:
        for name in ("requests.parquet", "sessions.parquet"):
            path = source / week["week"] / "processed" / name
            with path.open("rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            expected = week["files"][name]
            require(digest == expected["sha256"] and path.stat().st_size == expected["bytes"],
                    f"original source differs from manifest: {path}")
    audits = [a for line in (input_dir / "audit.jsonl").open()
              if (a := json.loads(line))["status"] == "included"]
    require(bool(audits), "no included families")
    raw, sessions, roots = read_source(source, audits)
    audited = {(a["trace_id"], r["session_id"], r["step"])
               for a in audits for r in a["requests"]}
    require(audited == raw.keys(), "audit request set differs from complete source families")
    for audit in audits:
        for child in audit["children"]:
            original = sessions[audit["trace_id"], child["child_session_id"]]
            require(original["parent_session_id"] == audit["root_session_id"],
                    "audit flattened a child under a different source parent")
            for field in ("spawned_by_step", "spawned_by_call_index"):
                require(child[field] == original[field], f"audit child {field} differs from original Parquet")

    report = json.loads(report_path.read_text())
    engine = json.loads(metrics_path.read_text())
    require(report["format_version"] == 1, "unsupported report format")
    require(report["complete"] and report["accounting_match"], "run did not complete with matching accounting")
    require(report["engine_available"], "run has no finalized engine metrics")
    require(report["manifest_sha256"] == hashlib.sha256(manifest_bytes).hexdigest(), "report names another manifest")
    require(report["source"] == manifest, "embedded source manifest differs from input")
    observed = unique(report["requests"], lambda r: (r["trace_id"], r["session_id"], r["step"]), "reported request")
    require(observed.keys() == raw.keys(), "reported request set differs from original Parquet")
    metrics = unique(engine["requests"], lambda r: r["requestID"], "engine request")
    require(set(metrics) == {r["request_id"] for r in observed.values()}, "engine request set differs from report")

    totals = {
        "families": len(roots), "sessions": len(sessions), "requests": len(raw),
        "prompt_tokens": sum(r["prompt_tokens"] for r in raw.values()),
        "output_tokens": sum(r["completion_tokens"] for r in raw.values()),
        "zero_output_requests": sum(r["completion_tokens"] == 0 for r in raw.values()),
    }
    require(report["selected"] == totals, f"selection totals differ from original Parquet: {report['selected']} != {totals}")
    require(report["engine"] == {
        "completed_requests": totals["requests"], "prompt_tokens": totals["prompt_tokens"],
        "output_tokens": totals["output_tokens"],
    }, "reported engine totals differ from original Parquet")
    for field, expected in {
        "completed_requests": totals["requests"], "injected_requests": totals["requests"],
        "total_input_tokens": totals["prompt_tokens"], "total_output_tokens": totals["output_tokens"],
        "still_queued": 0, "still_running": 0, "dropped_unservable": 0,
        "length_capped_requests": 0, "timed_out_requests": 0,
    }.items():
        require(engine[field] == expected, f"engine {field}: {engine[field]} != {expected}")

    by_session = defaultdict(list)
    for key in raw:
        by_session[key[:2]].append(key)
    for keys in by_session.values():
        keys.sort(key=lambda key: key[2])
    reported_sessions = unique(report["sessions"], lambda s: (s["trace_id"], s["session_id"]), "reported session")
    require(reported_sessions.keys() == sessions.keys(), "reported session set differs from source")
    for key, session in reported_sessions.items():
        first, last = by_session[key][0], by_session[key][-1]
        require(session["arrival_us"] == observed[first]["arrival_us"] and
                session["completion_us"] == observed[last]["completion_us"],
                f"session lifetime differs from its requests: {key}")
    origins = {trace: raw[trace, sid, 0]["start_ms"] for trace, sid in roots.items()}
    epoch = min(origins.values())
    require(report["origin_ms"] == epoch, "simulation epoch differs from earliest selected root")
    joins = defaultdict(list)
    policy = report["join_policy"]
    require(policy in ("none", "timing-inferred"), f"unknown join policy {policy}")
    if policy == "timing-inferred":
        for (trace, sid), session in sessions.items():
            parent = session["parent_session_id"]
            if not parent:
                continue
            last = by_session[trace, sid][-1]
            for key in by_session[trace, parent]:
                if key[2] > session["spawned_by_step"] and raw[key]["start_ms"] >= raw[last]["end_ms"]:
                    joins[key].append(last)
                    break

    for key, row in raw.items():
        actual = observed[key]
        metric = metrics[actual["request_id"]]
        require(actual["state"] == "completed", f"request did not complete: {key}")
        for field in ("start_ms", "end_ms", "prompt_tokens"):
            require(actual[field] == row[field], f"source {field} changed for {key}")
        require(actual["output_tokens"] == row["completion_tokens"], f"source output count changed for {key}")
        require(metric["num_prefill_tokens"] == row["prompt_tokens"], f"engine prompt count differs for {key}")
        require(metric["num_decode_tokens"] == row["completion_tokens"] and metric.get("output_tokens", 0) == row["completion_tokens"],
                f"engine output count differs for {key}")
        require(abs(metric["arrived_at"] * 1e6 - actual["arrival_us"]) < 0.01, f"engine arrival differs for {key}")
        require(actual["e2e_us"] == actual["completion_us"] - actual["arrival_us"],
                f"lifecycle duration differs for {key}")
        require(abs(metric["e2e_ms"] * 1000 - actual["engine_e2e_us"]) < 0.01 and
                abs(actual["arrival_us"] + actual["engine_e2e_us"] - actual["engine_completion_us"]) < 0.01,
                f"engine completion differs for {key}")
        if "pd" not in engine:
            require(abs(actual["completion_us"] - actual["engine_completion_us"]) < 0.01,
                    f"colocated lifecycle completion differs from engine for {key}")
        trace, sid, step = key
        session = sessions[trace, sid]
        if step:
            prerequisites = [(trace, sid, step - 1), *joins[key]]
            residual = row["start_ms"] - max(raw[p]["end_ms"] for p in prerequisites)
            require(residual >= 0, f"negative completion residual at {key}")
            due = max(observed[p]["completion_us"] for p in prerequisites) + residual * 1000
        elif session["parent_session_id"]:
            parent = trace, session["parent_session_id"], session["spawned_by_step"]
            overlap = row["start_ms"] < raw[parent]["end_ms"]
            anchor = "start_ms" if overlap else "end_ms"
            simulated = "arrival_us" if overlap else "completion_us"
            residual = row["start_ms"] - raw[parent][anchor]
            require(residual >= 0, f"negative spawn residual at {key}")
            due = observed[parent][simulated] + residual * 1000
        else:
            due = (row["start_ms"] - epoch) * 1000
        require(actual["arrival_us"] == due, f"causal arrival for {key}: {actual['arrival_us']} != {due}")

    families = unique(report["families"], lambda f: f["trace_id"], "reported family")
    require(families.keys() == roots.keys(), "reported family set differs from source")
    for trace, family in families.items():
        birth = (origins[trace] - epoch) * 1000
        finish = max(r["completion_us"] for r in observed.values() if r["trace_id"] == trace)
        require(family["birth_us"] == birth and family["arrival_us"] == birth, f"root arrival differs for {trace}")
        require(family["completion_us"] == finish and family["makespan_us"] == finish - birth,
                f"whole-family makespan differs for {trace}")
    return {"verified": True, "join_policy": policy, **totals,
            "manifest_sha256": report["manifest_sha256"],
            "report": str(report_path), "metrics": str(metrics_path)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="conversion directory")
    parser.add_argument("--report", type=Path, required=True, help="semi-closed report JSON")
    parser.add_argument("--metrics", type=Path, required=True, help="BLIS engine metrics JSON")
    parser.add_argument("--source", type=Path, default=Path("traces/freeinference_agentic_trace/release"))
    args = parser.parse_args()
    try:
        result = verify(args.input, args.source, args.report, args.metrics)
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f"verification failed: {error}\n")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
