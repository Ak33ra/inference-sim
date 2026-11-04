from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from itertools import combinations

from .model import Family, Request
from .prefix import common_prefix, map_prefix

BlockReader = Callable[[Request], Sequence[int]]


def convert_family(family: Family, read_blocks: BlockReader) -> tuple[dict, dict]:
    """Convert an already validated family; preserve source times without retiming."""
    origin = family.requests[family.root_id][0].start_ms
    calls: dict[str, list[dict]] = {}
    stats: Counter = Counter()
    request_audit = []
    sharing = []

    def compare(left: Request, right: Request, kind: str) -> None:
        left_hash = calls[left.session_id][left.step]["hash_ids"]
        right_hash = calls[right.session_id][right.step]["hash_ids"]
        source_tokens = 16 * common_prefix(read_blocks(left), read_blocks(right))
        mapped_tokens = 64 * common_prefix(left_hash, right_hash)
        sharing.append({"kind": kind, "left": list(left.key), "right": list(right.key),
                        "source_shared_prefix_tokens": source_tokens,
                        "mapped_shared_prefix_tokens": mapped_tokens})

    for session in family.sessions:
        sid = session.session_id
        calls[sid] = []
        previous = None
        previous_blocks: Sequence[int] = ()
        previous_hashes: Sequence[int] = ()
        for request in family.requests[sid]:
            blocks = read_blocks(request)
            hashes = map_prefix(blocks, request.prompt_tokens)
            calls[sid].append({
                "type": "s" if request.stream else "n",
                "t": (request.start_ms - origin) / 1000,
                "api_time": (request.end_ms - request.start_ms) / 1000,
                "in": request.prompt_tokens, "out": request.completion_tokens,
                "model": request.model, "hash_ids": hashes,
            })
            gap = None if previous is None else request.start_ms - previous.end_ms
            source_lcp = 0 if previous is None else common_prefix(previous_blocks, blocks) * 16
            mapped_lcp = 0 if previous is None else common_prefix(previous_hashes, hashes) * 64
            request_audit.append({
                "session_id": sid, "step": request.step,
                "start_ms": request.start_ms, "end_ms": request.end_ms,
                "prompt_tokens": request.prompt_tokens, "completion_tokens": request.completion_tokens,
                "step_trigger": request.step_trigger, "waited_for": request.waited_for,
                "gap_from_previous_end_ms": gap,
                "source_blocks": len(blocks), "mapped_blocks": len(hashes),
                "source_shared_with_previous_tokens": source_lcp,
                "mapped_shared_with_previous_tokens": mapped_lcp,
            })
            stats.update({"requests": 1, "prompt_tokens": request.prompt_tokens,
                          "completion_tokens": request.completion_tokens,
                          "source_blocks": len(blocks), "mapped_blocks": len(hashes),
                          "unmapped_prompt_tokens": request.prompt_tokens - 64 * len(hashes),
                          "source_groups_clipped_to_prompt": len(blocks) // 4 - len(hashes),
                          "source_trailing_blocks": len(blocks) % 4,
                          "zero_output_requests": int(request.completion_tokens == 0),
                          "requests_without_mapped_blocks": int(not hashes)})
            if previous is not None:
                stats.update({"sequential_gaps": 1, "sequential_gap_ms": gap,
                              "sequential_gaps_over_10s": int(gap > 10000),
                              "prompt_contractions": int(request.prompt_tokens < previous.prompt_tokens),
                              "source_shared_with_previous_tokens": source_lcp,
                              "mapped_shared_with_previous_tokens": mapped_lcp})
            previous, previous_blocks, previous_hashes = request, blocks, hashes

    children = defaultdict(list)
    for session in family.sessions:
        if session.parent_session_id:
            children[session.spawned_by_step].append(session)
    entries, branches = [], []
    parent_rows = family.requests[family.root_id]
    for parent in parent_rows:
        entries.append(calls[family.root_id][parent.step])
        siblings = sorted(children[parent.step], key=lambda s: (s.spawned_by_call_index, s.session_id))
        for child in siblings:
            rows = family.requests[child.session_id]
            first, end = rows[0], max(r.end_ms for r in rows)
            join = next((r for r in parent_rows[parent.step + 1:] if r.start_ms >= end), None)
            entries.append({
                "type": "subagent", "agent_id": child.session_id,
                "t": (first.start_ms - origin) / 1000,
                "duration_ms": end - first.start_ms,
                "tool_tokens": 0, "system_tokens": 0,
                "requests": calls[child.session_id],
            })
            branches.append({
                "child_session_id": child.session_id, "spawned_by_step": parent.step,
                "spawned_by_call_index": child.spawned_by_call_index,
                "first_start_ms": first.start_ms, "last_end_ms": end,
                "spawn_gap_from_parent_end_ms": first.start_ms - parent.end_ms,
                "inferred_join_step": None if join is None else join.step,
                "inferred_join_residual_ms": None if join is None else join.start_ms - end,
                "join_waited_for": None if join is None else join.waited_for,
            })
            compare(parent, first, "parent_child")
        for a, b in combinations(siblings, 2):
            compare(family.requests[a.session_id][0], family.requests[b.session_id][0], "siblings")

    # AgentX merges children with identical spawn and inferred join anchors.
    groups = defaultdict(list)
    for child in branches:
        groups[child["spawned_by_step"], child["inferred_join_step"]].append(child)
    group_audit = []
    for (spawn, join), group in groups.items():
        last_end = max(c["last_end_ms"] for c in group)
        group_audit.append({
            "spawn_step": spawn, "inferred_join_step": join,
            "children": [c["child_session_id"] for c in group],
            "last_child_end_ms": last_end,
            "residual_ms": None if join is None else parent_rows[join].start_ms - last_end,
        })
    stats.update({"families": 1, "sessions": len(family.sessions),
                  "child_sessions": len(branches), "spawn_groups": len(groups),
                  "fanout_groups": sum(len(g) >= 2 for g in groups.values()),
                  "background_child_sessions": sum(b["inferred_join_step"] is None for b in branches),
                  "children_starting_before_parent_end": sum(b["spawn_gap_from_parent_end_ms"] < 0 for b in branches),
                  "positive_spawn_gap_ms": sum(max(0, b["spawn_gap_from_parent_end_ms"]) for b in branches),
                  "inferred_group_join_residual_ms": sum(g["residual_ms"] or 0 for g in group_audit)})
    trace = {"version": 7, "id": family.trace_id, "models": [parent_rows[0].model],
             "block_size": 64, "hash_id_scope": "local", "tool_tokens": 0,
             "system_tokens": 0, "requests": entries}
    audit = {"trace_id": family.trace_id, "status": "included", "week": family.week,
             "root_session_id": family.root_id, "origin_ms": origin,
             "stats": dict(stats), "requests": request_audit, "children": branches,
             "inferred_groups": group_audit, "sharing": sharing}
    return trace, audit
