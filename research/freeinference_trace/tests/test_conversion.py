from __future__ import annotations

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import tempfile
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from research.freeinference_trace.convert import convert_family
from research.freeinference_trace.model import Request, Session, TraceError, group_families, unsupported_reason
from research.freeinference_trace.pipeline import Selection, convert_release
from research.freeinference_trace.prefix import common_prefix, map_prefix

WEEK = "2026-07-12_2026-07-18"


def request(sid, step, start, end, *, prompt=144, output=32, model="glm-5.2"):
    return Request(sid, step, start, end, model, prompt, output, True, "tool", "tool")


def fixture():
    sessions = [Session("root", None, None, None), Session("a", "root", 0, 0),
                Session("b", "root", 0, 1), Session("c", "root", 1, 0)]
    requests = [request("root", 0, 1000, 2000), request("root", 1, 9000, 10000),
                request("root", 2, 15000, 16000, prompt=64),
                request("a", 0, 2500, 3500), request("a", 1, 6000, 7000, output=0),
                request("b", 0, 3000, 8000), request("c", 0, 11000, 20000)]
    blocks = {r.key: list(range(1, 10)) for r in requests}
    blocks["a", 1] = list(range(1, 14))
    tools = {("root", 0, 0), ("root", 0, 1), ("root", 1, 0)}
    return sessions, requests, blocks, tools


def make_release(root, sessions, requests, blocks, tools):
    from dataclasses import asdict
    folder = root / WEEK
    processed = folder / "processed"
    processed.mkdir(parents=True)
    metadata = {"format_version": 1, "week": WEEK, "tokenizer": "o200k_base",
                "block_tokens": 16, "salt_series": "synthetic"}
    (folder / "manifest.json").write_text(json.dumps(metadata))
    session_schema = pa.schema([
        ("session_id", pa.string()), ("parent_session_id", pa.string()),
        ("spawned_by_step", pa.int32()), ("spawned_by_call_index", pa.int32())])
    pq.write_table(pa.Table.from_pylist([asdict(s) for s in sessions], schema=session_schema),
                   processed / "sessions.parquet")
    pq.write_table(pa.Table.from_pylist([{**asdict(r), "block_ids": blocks[r.key]} for r in requests]),
                   processed / "requests.parquet", row_group_size=2)
    tool_schema = pa.schema([("session_id", pa.string()), ("step", pa.int32()),
                            ("call_index", pa.int32()), ("category", pa.string())])
    pq.write_table(pa.Table.from_pylist([
        {"session_id": sid, "step": step, "call_index": index, "category": "Subagent"}
        for sid, step, index in sorted(tools)], schema=tool_schema), processed / "tool_calls.parquet")
    digest = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
    tables = {"manifest_sha256": digest(folder / "manifest.json"),
              "files": {p.name: {"sha256": digest(p)} for p in sorted(processed.glob("*.parquet"))}}
    (processed / "tables.json").write_text(json.dumps(tables))


class ConversionTests(unittest.TestCase):
    def setUp(self):
        self.sessions, self.requests, self.blocks, self.tools = fixture()
        self.family = group_families(WEEK, self.sessions, self.requests)[0]

    def test_fork_join_background_and_source_timing(self):
        trace, audit = convert_family(self.family, lambda r: self.blocks[r.key])
        self.assertEqual([r["type"] for r in trace["requests"]],
                         ["s", "subagent", "subagent", "s", "subagent", "s"])
        first, a, b, join, c, last = trace["requests"]
        self.assertEqual((first["t"], first["api_time"]), (0, 1))
        self.assertEqual((a["t"], a["duration_ms"]), (1.5, 4500))
        self.assertEqual(a["requests"][1]["t"], 5)
        self.assertEqual(join["t"], 8)
        self.assertEqual(last["in"], 64)  # Compaction is not accumulated away.
        self.assertEqual(last["hash_ids"], [4])
        self.assertEqual(a["requests"][1]["out"], 0)
        self.assertEqual(a["requests"][1]["in"], 144)
        self.assertEqual(audit["inferred_groups"][0]["residual_ms"], 1000)
        self.assertIsNone(audit["inferred_groups"][1]["inferred_join_step"])
        self.assertEqual(audit["children"][0]["spawn_gap_from_parent_end_ms"], 500)
        a1 = next(r for r in audit["requests"] if (r["session_id"], r["step"]) == ("a", 1))
        self.assertEqual(a1["gap_from_previous_end_ms"], 2500)
        self.assertEqual(audit["stats"]["requests"], 7)
        self.assertEqual(audit["stats"]["zero_output_requests"], 1)
        self.assertEqual(audit["stats"]["background_child_sessions"], 1)
        self.assertEqual(len(audit["sharing"]), 4)

    def test_prefix_rounding_sharing_and_serving_length_cap(self):
        # IDs after the fork differ because source hashes are chained.
        a = list(range(1, 14))
        b = list(range(1, 7)) + list(range(100, 107))
        mapped_a, mapped_b = map_prefix(a, 200), map_prefix(b, 200)
        self.assertEqual(mapped_a, [4, 8, 12])
        self.assertEqual(common_prefix(mapped_a, mapped_b), common_prefix(a, b) // 4)
        self.assertEqual(map_prefix(a, 65), [4])
        self.assertEqual(map_prefix(a, 63), [])
        self.assertEqual(map_prefix([], 128), [])
        with self.assertRaises(TraceError):
            map_prefix([1, 2, 3, -1], 64)

    def test_simultaneous_children_are_ordered_by_call_index(self):
        sessions = [replace(s, spawned_by_call_index=1 - s.spawned_by_call_index)
                    if s.session_id in ("a", "b") else s for s in self.sessions]
        requests = [replace(r, start_ms=2500) if r.key == ("b", 0) else r for r in self.requests]
        first = group_families(WEEK, sessions, requests)[0]
        shuffled = group_families(WEEK, reversed(sessions), reversed(requests))[0]
        a = convert_family(first, lambda r: self.blocks[r.key])
        b = convert_family(shuffled, lambda r: self.blocks[r.key])
        self.assertEqual(a, b)
        self.assertEqual([r["agent_id"] for r in a[0]["requests"] if r["type"] == "subagent"],
                         ["b", "a", "c"])

    def test_child_can_start_during_spawning_request(self):
        requests = [replace(r, start_ms=1500) if r.key == ("a", 0) else r for r in self.requests]
        family = group_families(WEEK, self.sessions, requests)[0]
        self.assertIsNone(unsupported_reason(family, self.tools))
        _, audit = convert_family(family, lambda r: self.blocks[r.key])
        self.assertEqual(audit["children"][0]["spawn_gap_from_parent_end_ms"], -500)

    def test_unsupported_shapes_are_explicit(self):
        cases = [
            (replace(self.requests[4], start_ms=3000), "overlapping_session_turns"),
            (replace(self.requests[3], start_ms=500), "child_before_spawn_request"),
            (replace(self.requests[3], completion_tokens=None), "invalid_token_counts"),
            (replace(self.requests[3], end_ms=2000), "negative_service_duration"),
        ]
        for changed, expected in cases:
            with self.subTest(reason=expected):
                requests = [changed if r.key == changed.key else r for r in self.requests]
                family = group_families(WEEK, self.sessions, requests)[0]
                self.assertEqual(unsupported_reason(family, self.tools), expected)
        self.assertEqual(unsupported_reason(self.family, set()), "missing_subagent_tool_call")
        nested = [replace(s, parent_session_id="a") if s.session_id == "b" else s for s in self.sessions]
        family = group_families(WEEK, nested, self.requests)[0]
        self.assertEqual(unsupported_reason(family, self.tools), "nested_delegation")

    def test_broken_graph_and_duplicate_requests_fail(self):
        with self.assertRaisesRegex(TraceError, "duplicate request"):
            group_families(WEEK, self.sessions, self.requests + [self.requests[0]])
        with self.assertRaisesRegex(TraceError, "missing parent"):
            group_families(WEEK, [replace(self.sessions[0], parent_session_id="absent")], [])
        with self.assertRaisesRegex(TraceError, "cycle"):
            group_families(WEEK, [Session("a", "b", 0, 0), Session("b", "a", 0, 0)], [])


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.source = self.root / "release"
        self.sessions, self.requests, self.blocks, self.tools = fixture()

    def write_source(self):
        make_release(self.source, self.sessions, self.requests, self.blocks, self.tools)

    def run_conversion(self, name="output", **kwargs):
        selection = kwargs.pop("selection", Selection((WEEK,), "glm-5.2"))
        return convert_release(self.source, self.root / name, selection, **kwargs)

    def test_deterministic_artifacts_and_record_conservation(self):
        # Exercise scattered family members, row-group boundaries and batch sizes.
        self.requests.reverse()
        self.write_source()
        a = self.run_conversion("a", batch_size=1)
        b = self.run_conversion("b", batch_size=3)
        self.assertEqual(a, b)
        for name in ("corpus.jsonl", "audit.jsonl", "manifest.json"):
            self.assertEqual((self.root / "a" / name).read_bytes(), (self.root / "b" / name).read_bytes())
        self.assertEqual(a["included"]["requests"], len(self.requests))
        self.assertEqual(a["counts"]["source_requests"], len(self.requests))
        self.assertEqual(a["artifacts"]["corpus.jsonl"]["sha256"],
                         hashlib.sha256((self.root / "a/corpus.jsonl").read_bytes()).hexdigest())
        with self.assertRaisesRegex(TraceError, "already exists"):
            self.run_conversion("a")

    def test_mixed_family_is_excluded_whole(self):
        self.sessions.append(Session("other", None, None, None))
        self.requests.append(request("other", 0, 100, 200))
        self.blocks["other", 0] = [21, 22, 23, 24]
        self.requests = [replace(r, model="another-model") if r.key == ("a", 0) else r for r in self.requests]
        self.write_source()
        manifest = self.run_conversion()
        self.assertEqual(manifest["excluded_families_by_reason"], {"mixed_model_family": 1})
        self.assertEqual(manifest["included"]["requests"], 1)
        self.assertEqual(manifest["counts"]["excluded_requests"], 7)

    def test_root_window_preserves_descendants_outside_window(self):
        self.write_source()
        manifest = self.run_conversion(selection=Selection((WEEK,), "glm-5.2", 1000, 1001))
        self.assertEqual(manifest["included"]["requests"], 7)
        with self.assertRaisesRegex(TraceError, "no eligible"):
            self.run_conversion("empty", selection=Selection((WEEK,), "glm-5.2", 1001, 2000))
        self.assertFalse((self.root / "empty").exists())
        self.assertFalse(list(self.root.glob(".empty-*")))

    def test_failure_leaves_no_published_output(self):
        self.write_source()
        path = self.source / WEEK / "processed/requests.parquet"
        with path.open("ab") as stream:
            stream.write(b"changed")
        with self.assertRaisesRegex(TraceError, "digest differs"):
            self.run_conversion()
        self.assertFalse((self.root / "output").exists())
        self.assertFalse(list(self.root.glob(".output-*")))

    def test_bad_prefix_cleans_up_disk_index_and_partial_artifacts(self):
        self.blocks["a", 0] = None
        self.write_source()
        scratch = self.root / "scratch"
        scratch.mkdir()
        with self.assertRaisesRegex(TraceError, "invalid block_ids"):
            self.run_conversion(temp_dir=scratch, batch_size=1)
        self.assertFalse((self.root / "output").exists())
        self.assertFalse(list(self.root.glob(".output-*")))
        self.assertEqual(list(scratch.iterdir()), [])

    def test_nested_family_is_excluded_without_flattening(self):
        self.sessions = [replace(s, parent_session_id="a") if s.session_id == "b" else s
                         for s in self.sessions]
        self.sessions.append(Session("ordinary", None, None, None))
        self.requests.append(request("ordinary", 0, 100, 200))
        self.blocks["ordinary", 0] = [21, 22, 23, 24]
        self.write_source()
        manifest = self.run_conversion()
        self.assertEqual(manifest["excluded_families_by_reason"], {"nested_delegation": 1})
        self.assertEqual(manifest["included"]["requests"], 1)
        corpus = (self.root / "output/corpus.jsonl").read_text().splitlines()
        self.assertEqual([json.loads(line)["id"] for line in corpus], [f"{WEEK}/ordinary"])

    def test_unsupported_error_and_invalid_selection(self):
        self.requests[4] = replace(self.requests[4], start_ms=3000)
        self.write_source()
        with self.assertRaisesRegex(TraceError, "overlapping_session"):
            self.run_conversion(selection=Selection((WEEK,), "glm-5.2", unsupported="error"))
        for selection in (Selection(("../escape",), "x"), Selection((WEEK,), "x", 10, 5),
                          Selection((WEEK, WEEK), "x")):
            with self.subTest(selection=selection), self.assertRaises(TraceError):
                selection.validate()


if __name__ == "__main__":
    unittest.main()
