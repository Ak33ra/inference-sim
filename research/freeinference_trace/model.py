from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable


class TraceError(ValueError):
    """An input or conversion contract was violated."""


@dataclass(frozen=True)
class Request:
    session_id: str
    step: int
    start_ms: int
    end_ms: int
    model: str
    prompt_tokens: int | None
    completion_tokens: int | None
    stream: bool
    step_trigger: str | None
    waited_for: str | None

    @property
    def key(self) -> tuple[str, int]:
        return self.session_id, self.step


@dataclass(frozen=True)
class Session:
    session_id: str
    parent_session_id: str | None
    spawned_by_step: int | None
    spawned_by_call_index: int | None


@dataclass(frozen=True)
class Family:
    week: str
    root_id: str
    sessions: tuple[Session, ...]
    requests: dict[str, tuple[Request, ...]]

    @property
    def trace_id(self) -> str:
        return f"{self.week}/{self.root_id}"

    def all_requests(self) -> Iterable[Request]:
        for session in self.sessions:
            yield from self.requests[session.session_id]


def group_families(
    week: str, sessions: Iterable[Session], requests: Iterable[Request]
) -> list[Family]:
    by_id: dict[str, Session] = {}
    for session in sessions:
        if not session.session_id or session.session_id in by_id:
            raise TraceError(f"{week}: empty or duplicate session ID {session.session_id!r}")
        by_id[session.session_id] = session
    roots: dict[str, str] = {}
    for sid in sorted(by_id):
        path: set[str] = set()
        current = sid
        while current not in roots:
            if current not in by_id:
                raise TraceError(f"{week}: missing parent {current!r} for {sid!r}")
            if current in path:
                raise TraceError(f"{week}: parent cycle at {current!r}")
            path.add(current)
            parent = by_id[current].parent_session_id
            if not parent:
                roots[current] = current
                break
            current = parent
        for member in path:
            roots[member] = roots[current]

    rows: dict[str, list[Request]] = defaultdict(list)
    keys: set[tuple[str, int]] = set()
    for request in requests:
        if request.session_id not in by_id:
            raise TraceError(f"{week}: request has unknown session {request.session_id!r}")
        if request.key in keys:
            raise TraceError(f"{week}: duplicate request {request.key}")
        if not all(type(v) is int for v in (request.step, request.start_ms, request.end_ms)):
            raise TraceError(f"{week}: invalid step or timestamps at {request.key}")
        keys.add(request.key)
        rows[request.session_id].append(request)
    members: dict[str, list[Session]] = defaultdict(list)
    for sid, session in by_id.items():
        if not rows[sid]:
            raise TraceError(f"{week}: session {sid!r} has no requests")
        rows[sid].sort(key=lambda request: request.step)
        members[roots[sid]].append(session)
    families = [
        Family(week, root, tuple(sorted(group, key=lambda s: (s.session_id != root, s.session_id))),
               {s.session_id: tuple(rows[s.session_id]) for s in group})
        for root, group in members.items()
    ]
    return sorted(families, key=lambda f: (f.requests[f.root_id][0].start_ms, f.root_id))


def unsupported_reason(family: Family, tool_keys: set[tuple[str, int, int]]) -> str | None:
    """Return a whole-family exclusion reason for the current BLIS Weka loader."""
    for session in family.sessions:
        rows = family.requests[session.session_id]
        if [r.step for r in rows] != list(range(len(rows))):
            return "incomplete_session_steps"
        for request in rows:
            if request.end_ms < request.start_ms:
                return "negative_service_duration"
            if (type(request.prompt_tokens) is not int or
                    not 0 < request.prompt_tokens <= 2**31 - 1 or
                    type(request.completion_tokens) is not int or
                    not 0 <= request.completion_tokens < 2**31 - 1):
                return "invalid_token_counts"
            if not isinstance(request.stream, bool):
                return "invalid_stream_flag"
        if any(b.start_ms < a.end_ms for a, b in zip(rows, rows[1:])):
            return "overlapping_session_turns"
        if not session.parent_session_id:
            continue
        if session.parent_session_id != family.root_id:
            return "nested_delegation"
        parents = family.requests[family.root_id]
        step = session.spawned_by_step
        if type(step) is not int or not 0 <= step < len(parents):
            return "missing_spawn_step"
        if (family.root_id, step, session.spawned_by_call_index) not in tool_keys:
            return "missing_subagent_tool_call"
        if rows[0].start_ms < parents[step].start_ms:
            return "child_before_spawn_request"
    return None
