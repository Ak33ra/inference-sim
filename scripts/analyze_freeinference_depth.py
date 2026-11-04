#!/usr/bin/env python3
"""Measure delegation depth and repeated spawn steps in local FreeInference data.

Requires pyarrow. Reads only metadata columns, never prompts or block_ids.
Depth is the number of parent_session_id edges from a root (root depth = 0).
A spawn step groups observed children by (parent_session_id, spawned_by_step);
it is not necessarily a simultaneous fan-out or a completed fork/join round.
Edges are the publisher's reconstructed matches, not orchestration ground truth.
"""

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path

import pyarrow.parquet as pq


ROOT = Path(__file__).resolve().parents[1]
REVISION = "16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade"


def write_csv(path, rows):
    if rows:
        with path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)


def histogram(values):
    return dict(sorted(Counter(values).items()))


def analyze(source, output):
    session_rows, family_rows, edge_rows, step_rows, sources = [], [], [], [], []
    request_depths, tool_depths, unmatched_tool_depths = Counter(), Counter(), Counter()
    weekly = []
    for week in sorted(source.iterdir()):
        if not week.is_dir():
            continue
        folder = week / "processed"
        rows = pq.read_table(folder / "sessions.parquet").to_pylist()
        sessions = {s["session_id"]: s for s in rows}
        assert len(sessions) == len(rows), "Duplicate session IDs within a week"
        depths, roots = {}, {}
        children, groups = defaultdict(list), defaultdict(list)

        def locate(sid, seen=None):
            if sid in depths:
                return depths[sid], roots[sid]
            seen = set() if seen is None else seen
            assert sid not in seen, f"Parent cycle: {week.name}/{sid}"
            seen.add(sid)
            parent = sessions[sid]["parent_session_id"]
            if parent:
                assert parent in sessions, f"Missing parent: {week.name}/{sid}"
                depth, root = locate(parent, seen)
                depth += 1
            else:
                depth, root = 0, sid
            depths[sid], roots[sid] = depth, root
            return depth, root

        for sid, s in sessions.items():
            locate(sid)
            if s["parent_session_id"]:
                children[s["parent_session_id"]].append(sid)
                groups[s["parent_session_id"], s["spawned_by_step"]].append(sid)

        requests = pq.read_table(folder / "requests.parquet", columns=[
            "session_id", "step", "model", "start_ms", "end_ms",
        ]).to_pylist()
        by_key = {(r["session_id"], r["step"]): r for r in requests}
        assert len(by_key) == len(requests), "Duplicate session/step"
        by_session, by_family = defaultdict(list), defaultdict(list)
        for r in requests:
            sid = r["session_id"]
            by_session[sid].append(r)
            by_family[roots[sid]].append(r)
            request_depths[depths[sid]] += 1
        assert set(by_session) == set(sessions)
        tools = pq.read_table(folder / "tool_calls.parquet", columns=[
            "session_id", "step", "call_index", "category", "name", "outcome",
        ]).to_pylist()
        tool_by_key = {(t["session_id"], t["step"], t["call_index"]): t for t in tools}
        assert len(tool_by_key) == len(tools), "Duplicate tool call key"
        matched_keys = set()
        for sid, s in sessions.items():
            parent = s["parent_session_id"]
            if not parent:
                continue
            key = (parent, s["spawned_by_step"], s["spawned_by_call_index"])
            assert key in tool_by_key and key[:2] in by_key
            assert key not in matched_keys, "Multiple children matched to one tool call"
            matched_keys.add(key)
            tool, request = tool_by_key[key], by_key[key[:2]]
            assert tool["category"] == "Subagent"
            edge_rows.append({
                "week": week.name, "root_session_id": roots[sid],
                "parent_session_id": parent, "child_session_id": sid,
                "parent_depth": depths[parent], "child_depth": depths[sid],
                "spawned_by_step": key[1], "spawned_by_call_index": key[2],
                "parent_model": request["model"],
                "child_models": "|".join(sorted({r["model"] for r in by_session[sid]})),
                "tool_name": tool["name"], "tool_outcome": tool["outcome"],
                "child_minus_parent_end_ms": min(r["start_ms"] for r in by_session[sid]) - request["end_ms"],
            })
        for key, t in tool_by_key.items():
            if t["category"] == "Subagent":
                tool_depths[depths[t["session_id"]]] += 1
                if key not in matched_keys:
                    unmatched_tool_depths[depths[t["session_id"]]] += 1

        steps_per_parent = Counter(parent for parent, step in groups)
        fanouts_per_parent = Counter(parent for (parent, step), ids in groups.items() if len(ids) >= 2)
        family_sessions = defaultdict(list)
        for sid, s in sessions.items():
            family_sessions[roots[sid]].append(sid)
            session_rows.append({
                "week": week.name, "session_id": sid, "root_session_id": roots[sid],
                "depth": depths[sid], "children": len(children[sid]),
                "spawn_steps": steps_per_parent[sid], "fanout_steps": fanouts_per_parent[sid],
                "requests": len(by_session[sid]), "harness": s["harness"],
                "models": "|".join(sorted({r["model"] for r in by_session[sid]})),
            })
        for root, ids in family_sessions.items():
            models = sorted({r["model"] for r in by_family[root]})
            family_rows.append({
                "week": week.name, "root_session_id": root,
                "harness": sessions[root]["harness"], "user_id": sessions[root]["user_id"],
                "models": "|".join(models), "single_model": models[0] if len(models) == 1 else "",
                "max_depth": max(depths[sid] for sid in ids),
                "sessions": len(ids), "subagent_sessions": len(ids) - 1,
                "requests": len(by_family[root]),
                "root_children": len(children[root]), "root_spawn_steps": steps_per_parent[root],
                "root_fanout_steps": fanouts_per_parent[root],
                "subagents_with_children": sum(bool(children[sid]) for sid in ids if sid != root),
            })
        for (parent, step), ids in sorted(groups.items()):
            step_rows.append({
                "week": week.name, "root_session_id": roots[parent], "parent_session_id": parent,
                "parent_depth": depths[parent], "parent_step": step,
                "parent_model": by_key[parent, step]["model"], "children": len(ids),
            })
        weekly.append({"week": week.name, "sessions": len(rows), "requests": len(requests),
                       "session_depths": histogram(depths.values())})
        sources.append({"week": week.name,
                        "sessions_sha256": hashlib.sha256((folder / "sessions.parquet").read_bytes()).hexdigest(),
                        "tables_metadata": json.loads((folder / "tables.json").read_text())})
        print(f"{week.name}: {len(rows)} sessions, max depth {max(depths.values())}", flush=True)

    active = [f for f in family_rows if f["max_depth"] > 0]
    nested = [f for f in active if f["max_depth"] > 1]
    agents = [s for s in session_rows if s["depth"] > 0]
    model_rows = []
    for model in sorted({f["single_model"] for f in active} - {""}):
        selected = [f for f in active if f["single_model"] == model]
        model_rows.append({"model": model, "families_with_children": len(selected),
                           "nested_families": sum(f["max_depth"] > 1 for f in selected),
                           "max_depth": max(f["max_depth"] for f in selected)})
    summary = {
        "dataset_revision": REVISION, "source": str(source.resolve()),
        "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "definitions": {
            "depth": "Number of parent_session_id edges from a root; roots are depth 0.",
            "family": "Root session and all descendants within a published week.",
            "spawn_step": "Parent request with >=1 matched child; repeated steps do not prove sequential fork/join rounds.",
            "fanout_step": "Parent request with >=2 matched children; simultaneity is not required.",
            "single_model_family": "Every request in the root and all descendants has the same model label.",
        },
        "counts": {"weeks": len(weekly), "requests": sum(request_depths.values()),
                   "sessions": len(session_rows), "families": len(family_rows),
                   "families_with_children": len(active), "nested_families": len(nested),
                   "subagent_sessions": len(agents),
                   "subagents_with_children": sum(s["children"] > 0 for s in agents),
                   "root_families_with_repeated_spawn_steps": sum(f["root_spawn_steps"] > 1 for f in active),
                   "matched_spawn_edges": len(edge_rows),
                   "nested_family_requests": sum(f["requests"] for f in nested),
                   "nested_family_users": len({f["user_id"] for f in nested})},
        "session_depths": histogram(s["depth"] for s in session_rows),
        "request_depths": dict(sorted(request_depths.items())),
        "family_max_depths": histogram(f["max_depth"] for f in family_rows),
        "spawners_by_depth": histogram(s["depth"] for s in session_rows if s["children"] > 0),
        "root_spawn_step_counts_among_spawning_families": histogram(f["root_spawn_steps"] for f in active),
        "root_fanout_step_counts_among_spawning_families": histogram(f["root_fanout_steps"] for f in active),
        "children_per_spawn_step": histogram(s["children"] for s in step_rows),
        "fanout_steps_by_parent_depth": histogram(s["parent_depth"] for s in step_rows if s["children"] >= 2),
        "subagent_category_tools_by_depth": dict(sorted(tool_depths.items())),
        "unmatched_subagent_category_tools_by_depth": dict(sorted(unmatched_tool_depths.items())),
        "single_model_family_summary": model_rows, "nested_families": nested, "weekly": weekly,
        "limitations": [
            "Parent edges are publisher prompt/time matches, not instrumented orchestration IDs.",
            "Missing/unmatched spawn edges and weekly boundaries can understate nesting; false matches can overstate it.",
            "Subagent-category tool counts need not represent successful spawns with a captured child.",
            "Source hashes in tables.json are recorded as provenance; large request/tool files are not rehashed.",
        ],
        "source_files": sources,
    }
    assert len(edge_rows) == len(agents) == len(session_rows) - len(family_rows)
    assert sum(f["subagents_with_children"] for f in family_rows) == summary["counts"]["subagents_with_children"]
    output.mkdir(parents=True, exist_ok=True)
    for name, rows in [("sessions", session_rows), ("families", family_rows), ("spawn_edges", edge_rows), ("spawn_steps", step_rows)]:
        write_csv(output / f"{name}.csv", rows)
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({k: v for k, v in summary.items() if k not in {"source_files", "nested_families", "weekly"}}, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=ROOT / "traces/freeinference_agentic_trace/release")
    parser.add_argument("--output", type=Path, default=ROOT / "results/freeinference_depth_20261029")
    args = parser.parse_args()
    analyze(args.source, args.output)
