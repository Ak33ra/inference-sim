"""Normalize saved runs and compute descriptive policy comparisons.

Request statistics describe completed, non-warmup rows, including zero-output
simulator TTFT. Raw engine E2E includes host completion overhead. No measured
window or unrecorded timing phase is inferred from aggregate values.
"""

from __future__ import annotations

from dataclasses import dataclass
from collections import Counter
import hashlib
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd


STATISTICS = {"mean": None, "p50": .50, "p90": .90, "p95": .95, "p99": .99}
REQUEST_COLUMNS = (
    "run", "request_id", "trace_id", "session_id", "step", "state", "phase",
    "eligible_latency", "arrival_s", "completion_s", "prompt_tokens", "output_tokens",
    "ttft_ms", "scheduling_delay_ms", "remaining_ttft_ms", "engine_e2e_ms",
    "lifecycle_e2e_ms", "engine_itl_ms", "gateway_queue_delay_ms", "handled_by", "trace_model",
)
FAMILY_COLUMNS = (
    "run", "trace_id", "complete", "arrival_s", "completion_s", "makespan_s",
    "requests", "sessions", "prompt_tokens", "output_tokens", "work_signature",
)
SESSION_COLUMNS = (
    "run", "trace_id", "session_id", "complete", "arrival_s", "completion_s", "lifetime_s",
)
PAIR_COLUMNS = (
    "baseline", "run", "trace_id", "baseline_s", "candidate_s", "delta_s", "change_percent",
)
COVERAGE_COLUMNS = (
    "baseline", "run", "baseline_families", "candidate_families", "common", "paired",
    "incomplete", "incompatible_work", "insufficient_provenance", "baseline_only", "candidate_only",
)


def number(value) -> float | None:
    if type(value) not in (int, float) or not math.isfinite(value):
        return None
    return float(value)


def scaled(value, divisor=1) -> float | None:
    value = number(value)
    return value / divisor if value is not None else None


def read_object(path: Path, optional=False) -> dict:
    if optional and not path.exists():
        return {}
    with path.open() as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a JSON object")
    return data


def indexed(rows, key: str, path: str) -> dict[str, dict]:
    if rows is None:
        rows = []
    if not isinstance(rows, list):
        raise ValueError(f"{path}: expected an array")
    result = {}
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get(key), str) or not row[key]:
            raise ValueError(f"{path}: each row needs a nonempty {key}")
        if row[key] in result:
            raise ValueError(f"{path}: duplicate {key} {row[key]}")
        result[row[key]] = row
    return result


@dataclass
class RunData:
    label: str
    path: Path
    metrics: dict
    workload: dict
    config: dict
    verification: dict
    requests: pd.DataFrame
    families: pd.DataFrame
    sessions: pd.DataFrame
    notices: list[str]


def all_completed(row: dict) -> bool:
    states = row.get("states", {})
    return bool(states.get("completed", 0)) and all(
        count == 0 for state, count in states.items() if state != "completed")


def work_signature(rows: list[dict], provenance: dict | None) -> str | None:
    fields = ("request_id", "trace_id", "session_id", "step", "start_ms", "end_ms",
              "prompt_tokens", "output_tokens", "delay_us", "prerequisites")
    if not provenance or not rows or any(any(field not in row for field in fields) for row in rows):
        return None
    canonical = [{field: row[field] for field in fields}
                 for row in sorted(rows, key=lambda r: r["request_id"])]
    for row in canonical:
        row["prerequisites"] = sorted(row["prerequisites"], key=lambda p: (p["request_id"], p["on"]))
    return hashlib.sha256(json.dumps([provenance, canonical], sort_keys=True).encode()).hexdigest()


def family_provenance(trace_id: str, manifest: dict) -> dict | None:
    source = manifest.get("source", {})
    week = next((item for item in source.get("weeks", []) if item.get("week") == trace_id.split("/")[0]), None)
    model = manifest.get("selection", {}).get("model")
    converter = manifest.get("converter", {}).get("source_sha256")
    if not week or not model or not converter or not manifest.get("conversion"):
        return None
    files = week.get("files", {})
    if not all(files.get(name, {}).get("sha256") for name in ("requests.parquet", "sessions.parquet", "tool_calls.parquet")):
        return None
    return {"model": model, "dataset": source.get("dataset"), "revision": source.get("declared_revision"),
            "week": week, "converter": converter, "conversion": manifest["conversion"]}


def validate_lifecycles(workload: dict, work: dict[str, dict], path: Path) -> None:
    if not workload:
        return
    family_members, session_members = {}, {}
    for row in work.values():
        family_members.setdefault(row["trace_id"], []).append(row)
        session_members.setdefault((row["trace_id"], row["session_id"]), []).append(row)
        if row["state"] == "completed":
            arrival, completion = number(row.get("arrival_us")), number(row.get("completion_us"))
            if arrival is None or completion is None or completion < arrival:
                raise ValueError(f"{path}: invalid completed request lifecycle {row['request_id']}")
    if workload.get("complete") is not all(row["state"] == "completed" for row in work.values()):
        raise ValueError(f"{path}: run completeness contradicts request states")
    for kind, groups, key_of in (
        ("families", family_members, lambda row: row["trace_id"]),
        ("sessions", session_members, lambda row: (row["trace_id"], row["session_id"])),
    ):
        seen = set()
        for row in workload.get(kind, []):
            key = key_of(row)
            if key in seen or key not in groups:
                raise ValueError(f"{path}: duplicate or unknown {kind} member {key}")
            seen.add(key)
            members = groups[key]
            counts = dict(Counter(member["state"] for member in members))
            if {state: count for state, count in row["states"].items() if count} != counts:
                raise ValueError(f"{path}: {kind} states contradict member requests for {key}")
            arrivals = [r["arrival_us"] for r in members if r.get("arrival_us") is not None]
            arrival = min(arrivals) if arrivals else None
            completed = all(member["state"] == "completed" for member in members)
            completion = max(member["completion_us"] for member in members) if completed else None
            if row.get("arrival_us") != arrival or row.get("completion_us") != completion:
                raise ValueError(f"{path}: {kind} lifecycle contradicts member requests for {key}")
            if kind == "families" and not completed and row.get("makespan_us") is not None:
                raise ValueError(f"{path}: incomplete family has a makespan for {key}")
        if seen != set(groups):
            raise ValueError(f"{path}: {kind} summaries do not cover every request")


def load_run(label: str, path: str | Path) -> RunData:
    if not label.strip():
        raise ValueError("Run labels must be nonempty")
    path = Path(path).expanduser().resolve()
    metrics = read_object(path / "metrics.json")
    workload = read_object(path / "workload.json", optional=True)
    config = read_object(path / "config.resolved.json", optional=True)
    verification = read_object(path / "verification.json", optional=True)
    engine = indexed(metrics.get("requests", []), "requestID", f"{path}/metrics.json requests")
    work = indexed(workload.get("requests", []), "request_id", f"{path}/workload.json requests")
    notices, requests = [], []
    if workload and workload.get("format_version") != 1:
        raise ValueError(f"{path}: unsupported workload format_version")
    if workload and set(engine) - set(work):
        raise ValueError(f"{path}: metrics request IDs are absent from workload.json")
    validate_lifecycles(workload, work, path)
    source_model = workload.get("source", {}).get("selection", {}).get("model")
    if source_model and any(row.get("model") and row["model"] != source_model for row in engine.values()):
        raise ValueError(f"{path}: request model contradicts source manifest model")
    if workload and (workload.get("complete") is not True or workload.get("accounting_match") is not True):
        notices.append("Workload is incomplete or accounting does not match; distributions cover completed work only.")
    if verification.get("verified") is False:
        notices.append("Saved source verification failed.")
    if metrics.get("measured_window_source") == "explicit":
        notices.append("Request plots cover completed non-warmup rows; the explicit aggregate measurement window cannot be reconstructed.")
    if not workload:
        notices.append("No workload.json: family/session plots and paired makespan are unavailable.")
    for request_id in sorted(set(engine) | set(work)):
        raw, source = engine.get(request_id, {}), work.get(request_id, {})
        e2e, ttft, delay = (number(raw.get(key)) for key in ("e2e_ms", "ttft_ms", "scheduling_delay_ms"))
        if "completion_index" in raw:
            completed = (number(raw["completion_index"]) or 0) > 0
        else:
            completed = e2e is not None and e2e > 0
        state = source.get("state", "completed" if completed else "uncompleted")
        phase = raw.get("phase", "")
        arrival = scaled(source.get("arrival_us"), 1e6) if source else number(raw.get("arrived_at"))
        completion = scaled(source.get("completion_us"), 1e6) if source else (
            arrival + e2e / 1000 if completed and arrival is not None and e2e is not None else None)
        requests.append({
            "run": label, "request_id": request_id, "trace_id": source.get("trace_id"),
            "session_id": source.get("session_id", raw.get("session_id")),
            "step": source.get("step", raw.get("round_index")), "state": state, "phase": phase,
            "eligible_latency": state == "completed" and phase != "warmup" and bool(raw),
            "arrival_s": arrival, "completion_s": completion,
            "prompt_tokens": source.get("prompt_tokens", raw.get("num_prefill_tokens")),
            "output_tokens": source.get("output_tokens", raw.get("output_tokens", raw.get("num_decode_tokens"))),
            "ttft_ms": ttft, "scheduling_delay_ms": delay,
            "remaining_ttft_ms": ttft - delay if ttft is not None and delay is not None else None,
            "engine_e2e_ms": e2e, "lifecycle_e2e_ms": scaled(source.get("e2e_us"), 1000),
            "engine_itl_ms": number(raw.get("itl_ms")),
            "gateway_queue_delay_ms": number(raw.get("gateway_queue_delay_ms")),
            "handled_by": raw.get("handled_by"), "trace_model": raw.get("model"),
        })
    groups = {}
    for row in work.values():
        groups.setdefault(row["trace_id"], []).append(row)
    families = []
    for trace_id, row in indexed(workload.get("families", []), "trace_id", "families").items():
        members = groups.get(trace_id, [])
        arrival, completion = (scaled(row.get(key), 1e6) for key in ("arrival_us", "completion_us"))
        makespan = scaled(row.get("makespan_us"), 1e6)
        complete = all_completed(row) and arrival is not None and completion is not None
        if complete and (makespan is None or makespan < 0 or not math.isclose(
                makespan, completion - arrival, rel_tol=1e-9, abs_tol=1e-6)):
            raise ValueError(f"{path}: inconsistent completed family makespan for {trace_id}")
        families.append({
            "run": label, "trace_id": trace_id, "complete": complete, "arrival_s": arrival,
            "completion_s": completion, "makespan_s": makespan if complete else None,
            "requests": len(members), "sessions": len({r["session_id"] for r in members}),
            "prompt_tokens": sum(r["prompt_tokens"] for r in members),
            "output_tokens": sum(r["output_tokens"] for r in members),
            "work_signature": work_signature(members, family_provenance(trace_id, workload.get("source", {}))),
        })
    sessions, session_ids = [], set()
    for row in workload.get("sessions", []):
        key = row["trace_id"], row["session_id"]
        if key in session_ids:
            raise ValueError(f"{path}: duplicate session {key}")
        session_ids.add(key)
        arrival, completion = (scaled(row.get(key), 1e6) for key in ("arrival_us", "completion_us"))
        complete = all_completed(row) and arrival is not None and completion is not None
        if complete and completion < arrival:
            raise ValueError(f"{path}: negative session lifetime {key}")
        sessions.append({"run": label, "trace_id": row["trace_id"], "session_id": row["session_id"],
                         "complete": complete, "arrival_s": arrival, "completion_s": completion,
                         "lifetime_s": completion - arrival if complete else None})
    return RunData(label, path, metrics, workload, config, verification,
                   pd.DataFrame(requests, columns=REQUEST_COLUMNS), pd.DataFrame(families, columns=FAMILY_COLUMNS),
                   pd.DataFrame(sessions, columns=SESSION_COLUMNS), notices)


def load_runs(paths: dict[str, str | Path]) -> list[RunData]:
    if not paths:
        raise ValueError("Supply at least one named run directory")
    return [load_run(label, path) for label, path in paths.items()]


def values(run: RunData, metric: str) -> np.ndarray:
    rows = run.requests.loc[run.requests["eligible_latency"].eq(True), metric]
    return np.sort(pd.to_numeric(rows, errors="coerce").dropna().loc[lambda s: s >= 0].to_numpy(dtype=float))


def distribution(data) -> dict:
    data = np.asarray(data, dtype=float)
    data = data[np.isfinite(data) & (data >= 0)]
    return {"count": len(data), **{
        name: float(np.mean(data) if quantile is None else np.quantile(data, quantile, method="linear"))
        if len(data) else None for name, quantile in STATISTICS.items()}}


def ttft_statistics(run: RunData) -> pd.DataFrame:
    rows = run.requests.loc[run.requests["eligible_latency"].eq(True)].copy()
    rows = rows.loc[rows["ttft_ms"].notna() & rows["ttft_ms"].ge(0)]
    rows = rows.sort_values(["ttft_ms", "request_id"], kind="stable")
    reason = ""
    if number(run.metrics.get("preemption_count")) != 0:
        reason = "Preemption count is nonzero or unknown; scheduling delay may have been overwritten."
    elif rows["scheduling_delay_ms"].isna().any() or rows["scheduling_delay_ms"].lt(0).any():
        reason = "Scheduling delay is missing or invalid."
    elif rows["remaining_ttft_ms"].lt(0).any():
        reason = "Scheduling delay exceeds TTFT; these fields do not form a valid split."
    data = rows[["ttft_ms", "scheduling_delay_ms", "remaining_ttft_ms"]].to_numpy(dtype=float)
    result = []
    for statistic, quantile in STATISTICS.items():
        row = {"run": run.label, "statistic": statistic, "count": len(rows), "decomposed": not reason and bool(len(rows)),
               "reason": reason, "total_ms": None, "scheduling_ms": None, "remaining_ms": None}
        if len(rows):
            if quantile is None:
                parts = data.mean(axis=0)
            else:
                rank = quantile * (len(rows) - 1)
                lower, upper = math.floor(rank), math.ceil(rank)
                weight = rank - lower
                parts = data[lower] * (1 - weight) + data[upper] * weight
            row["total_ms"] = float(parts[0])
            if row["decomposed"]:
                row["scheduling_ms"], row["remaining_ms"] = map(float, parts[1:])
        result.append(row)
    return pd.DataFrame(result)


def paired_families(runs: list[RunData], baseline: str | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    baseline = baseline or runs[0].label
    by_label = {run.label: run for run in runs}
    if baseline not in by_label:
        raise ValueError(f"Unknown baseline {baseline!r}")
    base = by_label[baseline].families.set_index("trace_id")
    pairs, coverage = [], []
    for run in runs:
        if run.label == baseline:
            continue
        candidate = run.families.set_index("trace_id")
        common = sorted(set(base.index) & set(candidate.index))
        record = {"baseline": baseline, "run": run.label, "baseline_families": len(base),
                  "candidate_families": len(candidate), "common": len(common), "paired": 0,
                  "incomplete": 0, "incompatible_work": 0, "insufficient_provenance": 0,
                  "baseline_only": len(base) - len(common), "candidate_only": len(candidate) - len(common)}
        for trace_id in common:
            left, right = base.loc[trace_id], candidate.loc[trace_id]
            if not left["work_signature"] or not right["work_signature"]:
                record["insufficient_provenance"] += 1
                continue
            if left["work_signature"] != right["work_signature"]:
                record["incompatible_work"] += 1
                continue
            if not left["complete"] or not right["complete"]:
                record["incomplete"] += 1
                continue
            a, b = float(left["makespan_s"]), float(right["makespan_s"])
            pairs.append({"baseline": baseline, "run": run.label, "trace_id": trace_id,
                          "baseline_s": a, "candidate_s": b, "delta_s": b - a,
                          "change_percent": 100 * (b / a - 1) if a > 0 else None})
            record["paired"] += 1
        coverage.append(record)
    return pd.DataFrame(pairs, columns=PAIR_COLUMNS), pd.DataFrame(coverage, columns=COVERAGE_COLUMNS)


def run_summary(run: RunData) -> dict:
    eligible = run.requests["eligible_latency"].eq(True)
    split = ttft_statistics(run)
    return {
        "run": run.label, "path": str(run.path), "notices": run.notices,
        "requests": len(run.requests), "completed_nonwarmup_rows": int(eligible.sum()),
        "request_states": run.requests["state"].value_counts().to_dict(),
        "warmup_rows": int(run.requests["phase"].eq("warmup").sum()),
        "zero_output_rows": int(run.requests["output_tokens"].eq(0).sum()),
        "families": len(run.families), "complete_families": int(run.families["complete"].eq(True).sum()),
        "sessions": len(run.sessions), "complete_sessions": int(run.sessions["complete"].eq(True).sum()),
        "ttft_ms": distribution(values(run, "ttft_ms")),
        "engine_e2e_ms": distribution(values(run, "engine_e2e_ms")),
        "family_makespan_s": distribution(run.families["makespan_s"].to_numpy(dtype=float)),
        "session_lifetime_s": distribution(run.sessions["lifetime_s"].to_numpy(dtype=float)),
        "decomposition_available": bool(split.iloc[0]["decomposed"]),
        "decomposition_reason": split.iloc[0]["reason"],
        "source_complete": run.workload.get("complete"), "accounting_match": run.workload.get("accounting_match"),
        "saved_verification": run.verification.get("verified"),
        "trace_model": run.config.get("trace", {}).get("model"),
        "serving_model": run.config.get("serving", {}).get("model"),
        "config": run.config,
        "emitted_aggregates": {key: value for key, value in run.metrics.items() if key not in ("requests", "server_series")},
        "observed_fields": {"metrics": sorted(run.metrics),
                            "request": sorted({key for row in run.metrics.get("requests", []) for key in row}),
                            "workload": sorted(run.workload)},
    }
