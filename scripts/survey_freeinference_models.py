#!/usr/bin/env python3
"""Compare model populations and reconstructed subagent fan-outs in FreeInference.

Reads only small Parquet columns, one week at a time (never block_ids or prompts).
Requires pyarrow. Run from any directory; defaults resolve against the repo root.

The release is already agent-session data. 'tool_call_requests' counts LLM
requests with at least one exported tool call; 'subagent_requests' counts calls
inside a session with a parent. These are separate operational definitions.

A fan-out candidate is >=2 traced children attached to the same parent step.
Overlap means positive-duration LLM requests from distinct child subtrees overlap;
overlapping session envelopes alone are insufficient. Matching spawn edges come
from the dataset's reconstruction, not instrumented orchestration IDs. A parent's
next request starting after every branch ends is join-compatible, not proof of a
causal join. Calls that spawn children on different parent steps are not grouped.
"""

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import statistics

import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
REVISION = "16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade"
REQUEST_COLUMNS = [
    "session_id", "step", "model", "provider", "start_ms", "end_ms",
    "prompt_tokens", "completion_tokens", "step_trigger", "waited_for",
]


def peak_branches(intervals):
    """Peak distinct branches with in-flight requests; intervals are half-open.

    Multiple concurrent requests inside ONE branch still count as one branch.
    """
    events = []
    for branch, spans in intervals.items():
        for start, end in spans:
            if end < start:
                raise ValueError("Negative request duration")
            if end > start:
                events.extend(((start, 1, branch), (end, -1, branch)))
    active = Counter()
    peak = 0
    for _, delta, branch in sorted(events):  # end before start at equal timestamps
        active[branch] += delta
        if active[branch] == 0:
            del active[branch]
        peak = max(peak, len(active))
    assert not active
    return peak


def session_tree(sessions):
    """Return root IDs and complete subtrees, rejecting missing parents/cycles."""
    children = defaultdict(list)
    for sid, s in sessions.items():
        parent = s["parent_session_id"]
        if parent:
            if parent not in sessions:
                raise ValueError(f"Missing parent for {sid}")
            children[parent].append(sid)
    roots = {}
    for sid in sessions:
        current, seen = sid, set()
        while sessions[current]["parent_session_id"]:
            if current in seen:
                raise ValueError(f"Parent cycle at {sid}")
            seen.add(current)
            current = sessions[current]["parent_session_id"]
        roots[sid] = current
    subtrees = {}

    def descend(sid):
        if sid not in subtrees:
            subtrees[sid] = [sid]
            for child in children[sid]:
                subtrees[sid].extend(descend(child))
        return subtrees[sid]

    for sid in sessions:
        descend(sid)
    return roots, children, subtrees


def csv_write(path, rows):
    if not rows:
        return
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def percentile(values, p):
    values = sorted(values)
    if not values:
        return 0
    at = (len(values) - 1) * p
    lo = int(at)
    hi = min(lo + 1, len(values) - 1)
    return values[lo] + (values[hi] - values[lo]) * (at - lo)


def survey(source, output):
    counters = defaultdict(Counter)
    weekly_counters = defaultdict(Counter)
    prompts, outputs = defaultdict(list), defaultdict(list)
    providers, harnesses = defaultdict(Counter), defaultdict(Counter)
    families_out, groups_out, sources = [], [], []
    checks = Counter()

    def bump(week, model, key, n=1):
        counters[model][key] += n
        weekly_counters[week, model][key] += n

    for week in sorted(source.iterdir()):
        if not week.is_dir():
            continue
        folder = week / "processed"
        session_rows = pq.read_table(folder / "sessions.parquet").to_pylist()
        sessions = {s["session_id"]: s for s in session_rows}
        assert len(sessions) == len(session_rows)
        roots, children, subtrees = session_tree(sessions)
        requests = pq.read_table(folder / "requests.parquet", columns=REQUEST_COLUMNS).to_pylist()
        by_key = {(r["session_id"], r["step"]): r for r in requests}
        assert len(by_key) == len(requests)
        by_session = defaultdict(list)
        by_family = defaultdict(list)
        for r in requests:
            by_session[r["session_id"]].append(r)
            by_family[roots[r["session_id"]]].append(r)
        assert set(by_session) == set(sessions)
        for rs in by_session.values():
            rs.sort(key=lambda r: r["step"])
        family_models = {root: {r["model"] for r in rs} for root, rs in by_family.items()}
        subagent_families = {roots[sid] for sid, s in sessions.items() if s["parent_session_id"]}
        tools = pq.read_table(folder / "tool_calls.parquet", columns=[
            "session_id", "step", "call_index", "category"
        ]).to_pylist()
        tool_keys = {(r["session_id"], r["step"]) for r in tools}
        tool_ids = {(r["session_id"], r["step"], r["call_index"]) for r in tools}
        assert tool_keys <= by_key.keys()
        spawn_groups = defaultdict(list)
        for sid, s in sessions.items():
            if s["parent_session_id"]:
                key = (s["parent_session_id"], s["spawned_by_step"])
                assert key in by_key
                assert (*key, s["spawned_by_call_index"]) in tool_ids
                spawn_groups[key].append(sid)
                checks["subagent_sessions"] += 1
        for r in requests:
            model, sid = r["model"], r["session_id"]
            assert model and r["start_ms"] is not None and r["end_ms"] is not None
            assert r["end_ms"] >= r["start_ms"]
            bump(week.name, model, "requests")
            bump(week.name, model, "tool_call_requests", int((sid, r["step"]) in tool_keys))
            bump(week.name, model, "tool_triggered_requests", int(r["step_trigger"] == "tool"))
            bump(week.name, model, "subagent_requests", int(bool(sessions[sid]["parent_session_id"])))
            bump(week.name, model, "requests_in_subagent_families", int(roots[sid] in subagent_families))
            bump(week.name, model, "input_tokens", r["prompt_tokens"] or 0)
            bump(week.name, model, "output_tokens", r["completion_tokens"] or 0)
            prompts[model].append(r["prompt_tokens"] or 0)
            outputs[model].append(r["completion_tokens"] or 0)
            providers[model][r["provider"]] += 1
            harnesses[model][sessions[sid]["harness"]] += 1
        for sid, rs in by_session.items():
            for model in {r["model"] for r in rs}:
                bump(week.name, model, "sessions_including_model")
                if sessions[sid]["parent_session_id"]:
                    bump(week.name, model, "subagent_sessions_including_model")

        family_groups = defaultdict(list)
        for (parent, step), child_ids in spawn_groups.items():
            parent_request = by_key[parent, step]
            bump(week.name, parent_request["model"], "parent_steps_with_matched_children")
            if len(child_ids) < 2:
                continue
            branch_requests = {
                sid: [r for descendant in subtrees[sid] for r in by_session[descendant]]
                for sid in child_ids
            }
            intervals = {sid: [(r["start_ms"], r["end_ms"]) for r in rs]
                         for sid, rs in branch_requests.items()}
            envelopes = {sid: [(min(a for a, b in spans), max(b for a, b in spans))]
                         for sid, spans in intervals.items()}
            branch_peak = peak_branches(intervals)
            first = min(a for spans in intervals.values() for a, b in spans)
            last = max(b for spans in intervals.values() for a, b in spans)
            later = [r for r in by_session[parent] if r["step"] > step]
            next_request = later[0] if later else None
            next_after = bool(next_request and next_request["start_ms"] >= last)
            root = roots[parent]
            pure = len(family_models[root]) == 1
            row = {
                "week": week.name, "root_session_id": root, "parent_session_id": parent,
                "parent_step": step, "parent_model": parent_request["model"],
                "family_model_count": len(family_models[root]),
                "single_model_family": pure, "child_branches": len(child_ids),
                "child_subtree_requests": sum(len(rs) for rs in branch_requests.values()),
                "child_models": "|".join(sorted({r["model"] for rs in branch_requests.values() for r in rs})),
                "peak_overlapping_branches": branch_peak,
                "peak_overlapping_branch_envelopes": peak_branches(envelopes),
                "parent_end_ms": parent_request["end_ms"], "first_child_start_ms": first,
                "last_child_end_ms": last, "branch_window_ms": last - first,
                "earliest_child_minus_parent_end_ms": first - parent_request["end_ms"],
                "parent_continuation_observed": bool(next_request),
                "next_parent_start_ms": next_request["start_ms"] if next_request else None,
                "next_parent_waited_for": next_request["waited_for"] if next_request else None,
                "next_parent_after_all_children": next_after,
                "nonnegative_spawn_gap": first >= parent_request["end_ms"],
                "child_session_ids": "|".join(sorted(child_ids)),
            }
            groups_out.append(row)
            family_groups[root].append(row)
            model = parent_request["model"]
            bump(week.name, model, "fanout_groups_parent_model")
            if branch_peak >= 2:
                bump(week.name, model, "overlapping_fanout_groups_parent_model")
            if pure:
                bump(week.name, model, "single_model_fanout_groups")
                if branch_peak >= 2:
                    bump(week.name, model, "single_model_overlapping_fanout_groups")
                    if next_after:
                        bump(week.name, model, "single_model_overlap_join_compatible_groups")
                        if row["nonnegative_spawn_gap"]:
                            bump(week.name, model, "single_model_overlap_join_nonnegative_groups")

        for root, rs in by_family.items():
            models = family_models[root]
            pure_model = next(iter(models)) if len(models) == 1 else ""
            family_sessions = subtrees[root]
            family_fanouts = family_groups[root]
            overlapping = [g for g in family_fanouts if g["peak_overlapping_branches"] >= 2]
            input_tokens = sum(r["prompt_tokens"] or 0 for r in rs)
            output_tokens = sum(r["completion_tokens"] or 0 for r in rs)
            row = {
                "week": week.name, "root_session_id": root,
                "models": "|".join(sorted(models)), "single_model": pure_model,
                "harness": sessions[root]["harness"], "user_id": sessions[root]["user_id"],
                "sessions": len(family_sessions), "subagent_sessions": len(family_sessions) - 1,
                "requests": len(rs), "subagent_requests": sum(bool(sessions[r["session_id"]]["parent_session_id"]) for r in rs),
                "input_tokens": input_tokens, "output_tokens": output_tokens,
                "max_prompt_tokens": max(r["prompt_tokens"] or 0 for r in rs),
                "max_prompt_plus_output_tokens": max((r["prompt_tokens"] or 0) + (r["completion_tokens"] or 0) for r in rs),
                "fanout_groups": len(family_fanouts), "overlapping_fanout_groups": len(overlapping),
                "join_compatible_groups": sum(g["next_parent_after_all_children"] for g in overlapping),
                "start_ms": min(r["start_ms"] for r in rs), "end_ms": max(r["end_ms"] for r in rs),
            }
            families_out.append(row)
            for model in models:
                bump(week.name, model, "families_including_model")
            if pure_model:
                bump(week.name, pure_model, "single_model_families")
                bump(week.name, pure_model, "single_model_family_requests", len(rs))
                if root in subagent_families:
                    bump(week.name, pure_model, "single_model_families_with_subagents")
                    bump(week.name, pure_model, "single_model_subagent_family_requests", len(rs))
                if overlapping:
                    bump(week.name, pure_model, "single_model_families_with_overlapping_fanouts")
                    bump(week.name, pure_model, "single_model_overlap_family_requests", len(rs))
                    bump(week.name, pure_model, "single_model_overlap_family_input_tokens", input_tokens)
                    bump(week.name, pure_model, "single_model_overlap_family_output_tokens", output_tokens)
        for name in ["requests.parquet", "sessions.parquet", "tool_calls.parquet", "tables.json"]:
            path = folder / name
            sources.append({"path": str(path.relative_to(source)), "bytes": path.stat().st_size})
        checks["weeks"] += 1
        checks["requests"] += len(requests)
        checks["sessions"] += len(sessions)
        checks["families"] += len(by_family)
        checks["families_with_subagents"] += len(subagent_families)
        print(f"{week.name}: {len(requests):,} requests, {len(spawn_groups):,} parent steps with children", flush=True)

    keys = sorted({k for c in counters.values() for k in c})
    model_rows = []
    for model, counts in sorted(counters.items(), key=lambda item: -item[1]["requests"]):
        row = {"model": model, **{k: counts[k] for k in keys}}
        row.update({
            "tool_call_request_pct": 100 * counts["tool_call_requests"] / counts["requests"],
            "subagent_request_pct": 100 * counts["subagent_requests"] / counts["requests"],
            "input_tokens_mean": statistics.mean(prompts[model]),
            "input_tokens_p50": percentile(prompts[model], 0.5),
            "input_tokens_p95": percentile(prompts[model], 0.95),
            "input_tokens_max": max(prompts[model]),
            "output_tokens_mean": statistics.mean(outputs[model]),
            "active_weeks": sum(m == model for w, m in weekly_counters),
            "provider_count": len(providers[model]),
        })
        model_rows.append(row)
    weekly_rows = [{"week": week, "model": model, **{k: c[k] for k in keys}}
                   for (week, model), c in sorted(weekly_counters.items())]
    assert sum(c["requests"] for c in counters.values()) == checks["requests"]
    assert sum(c["subagent_requests"] for c in counters.values()) == 35617
    assert checks["requests"] == 1222199 and checks["sessions"] == 14036
    assert checks["families"] == 12002 and checks["subagent_sessions"] == 2034
    checks["fanout_groups"] = len(groups_out)
    checks["overlapping_fanout_groups"] = sum(g["peak_overlapping_branches"] >= 2 for g in groups_out)
    checks["single_model_overlapping_fanout_groups"] = sum(g["single_model_family"] and g["peak_overlapping_branches"] >= 2 for g in groups_out)
    output.mkdir(parents=True, exist_ok=True)
    csv_write(output / "models.csv", model_rows)
    csv_write(output / "weekly_models.csv", weekly_rows)
    csv_write(output / "families.csv", families_out)
    csv_write(output / "fanout_groups.csv", groups_out)
    report = {
        "source": str(source), "dataset_revision": REVISION,
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "definitions": {
            "tool_call_requests": "LLM request with >=1 exported tool call; not the count of tools",
            "subagent_requests": "LLM request in a session with parent_session_id",
            "fanout": ">=2 traced child sessions matched to the same parent session and step",
            "overlap": ">=2 distinct child subtrees have positive-duration LLM intervals overlapping; touching endpoints do not count",
            "single_model": "every request in the complete root family, including all descendants, has exactly the same model label",
            "attribution": "all fan-out groups attributed to the parent request's model; single-model family counts avoid cross-model attribution ambiguity",
            "join_compatible": "the next observed parent request starts at/after the final child-subtree completion; this does not establish a causal join",
        },
        "limitations": [
            "Spawn relations are reconstructed by the release's prompt/time matching, not instrumented ground truth.",
            "Same-step groups miss asynchronous fan-outs spawned over multiple parent steps and unmatched children.",
            "A family is complete relative to the published release; unobserved provider activity may be absent.",
            "Provider letters are retained as supplied; no cross-week provider identity is assumed.",
            "Input lengths are provider prompt_tokens, not block-hash tokenizer lengths.",
        ],
        "checks": dict(checks), "models": model_rows, "source_files": sources,
        "provider_labels_by_model": dict(providers), "harness_requests_by_model": dict(harnesses),
    }
    (output / "summary.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(dict(checks), indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "traces/freeinference_agentic_trace/release")
    parser.add_argument("--output", type=Path, default=ROOT / "results/freeinference_model_survey_20261027")
    args = parser.parse_args()
    survey(args.source, args.output)
