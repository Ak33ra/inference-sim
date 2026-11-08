from __future__ import annotations

from copy import deepcopy
import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from research.semiclosed.mix import Source, compose, parse_sources, verify_composition


def family(ident="same", model="gpt-oss-120b"):
    def call(ident, parents, time):
        return {"type": "n", "req_id": ident, "parent_ids": parents, "model": model,
                "t": time, "api_time": .125, "think_time": 0, "in": 128,
                "out": 10, "hash_ids": [1, 2]}
    return {"id": ident, "models": [model], "block_size": 64, "hash_id_scope": "local",
            "requests": [call(0, [], 1.25),
                         {"type": "subagent", "agent_id": "child:/1", "requests": [call(1, [0], 1.5)]},
                         call(2, [0, 1], 2)]}


class MixTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.a = self.write("a.jsonl", [family(), family("tail")])
        self.b = self.write("b.jsonl", [family()])
        self.output = self.root / "input.jsonl"
        self.manifest = self.root / "composition.json"

    def write(self, name, rows):
        path = self.root / name
        path.write_text("".join(json.dumps(row) + "\n" for row in rows))
        return path

    def test_unequal_sources_interleave_copies_and_drain_every_family(self):
        manifest = compose((Source(self.a, 2), Source(self.b)), self.output, self.manifest)
        rows = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual([r["id"] for r in rows], [
            "source0/copy0/same", "source0/copy1/same", "source1/copy0/same",
            "source0/copy0/tail", "source0/copy1/tail"])
        self.assertEqual(rows[0]["requests"][1]["agent_id"], "source0/copy0/child:/1")
        self.assertEqual(rows[1]["requests"][1]["agent_id"], "source0/copy1/child:/1")
        for row in rows:
            self.assertEqual(row["requests"][2], family()["requests"][2])
        self.assertEqual(manifest["included"], {"families": 5, "sessions": 10, "requests": 15,
                                             "prompt_tokens": 1920, "output_tokens": 150,
                                             "zero_output_requests": 0})
        result = verify_composition(self.output, self.manifest)
        self.assertTrue(result["verified"])
        self.assertEqual(result["source_totals"], manifest["included"])
        self.assertEqual(result["source_copies"], 3)
        self.assertEqual(json.loads(self.b.read_text()), family())

    def test_repeated_path_is_an_independent_source(self):
        compose((Source(self.b), Source(self.b)), self.output, self.manifest)
        result = verify_composition(self.output, self.manifest)
        self.assertEqual(result["source_totals"]["families"], 2)
        self.assertEqual(result["source_totals"]["requests"], 6)

    def test_nested_sessions_preserve_dag_and_local_prefixes(self):
        row = family("family:/id")
        nested = {"type": "subagent", "agent_id": "nested:/id", "requests": [
            {**deepcopy(row["requests"][0]), "req_id": 3, "parent_ids": [1], "t": 1.75}]}
        row["requests"][1]["requests"].append(nested)
        row["requests"][-1]["parent_ids"].append(3)
        path = self.write("nested.jsonl", [row])
        compose((Source(path, 2),), self.output, self.manifest)
        self.assertTrue(verify_composition(self.output, self.manifest)["verified"])
        copies = [json.loads(line) for line in self.output.read_text().splitlines()]
        self.assertEqual(copies[1]["requests"][1]["requests"][1]["agent_id"], "source0/copy1/nested:/id")
        self.assertEqual(copies[1]["requests"][-1]["parent_ids"], [0, 1, 3])
        self.assertEqual(copies[1]["requests"][0]["hash_ids"], [1, 2])

    def test_verifier_rejects_changed_work_even_with_updated_input_digest(self):
        compose((Source(self.b, 2),), self.output, self.manifest)
        original_rows = [json.loads(line) for line in self.output.read_text().splitlines()]
        manifest = json.loads(self.manifest.read_text())
        for field, value in (("in", 129), ("out", 11), ("parent_ids", [1]),
                             ("hash_ids", [2, 1]), ("t", 1.3), ("api_time", .25)):
            with self.subTest(field=field):
                rows = deepcopy(original_rows)
                rows[0]["requests"][0][field] = value
                self.write(self.output.name, rows)
                manifest["input_sha256"] = hashlib.sha256(self.output.read_bytes()).hexdigest()
                self.manifest.write_text(json.dumps(manifest))
                with self.assertRaisesRegex(ValueError, "changed source requests"):
                    verify_composition(self.output, self.manifest)

    def test_verifier_rejects_changed_source_and_missing_copies(self):
        compose((Source(self.b, 2),), self.output, self.manifest)
        saved = self.b.read_bytes()
        self.b.write_bytes(saved + b"\n")
        with self.assertRaisesRegex(ValueError, "source changed"):
            verify_composition(self.output, self.manifest)
        self.b.write_bytes(saved)
        manifest = json.loads(self.manifest.read_text())
        manifest["sources"][0]["copies"] = 3
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "family count"):
            verify_composition(self.output, self.manifest)

    def test_verifier_rejects_reordered_families(self):
        compose((Source(self.a),), self.output, self.manifest)
        rows = list(reversed([json.loads(line) for line in self.output.read_text().splitlines()]))
        self.write(self.output.name, rows)
        manifest = json.loads(self.manifest.read_text())
        manifest["families"].reverse()
        manifest["input_sha256"] = hashlib.sha256(self.output.read_bytes()).hexdigest()
        self.manifest.write_text(json.dumps(manifest))
        with self.assertRaisesRegex(ValueError, "ordering"):
            verify_composition(self.output, self.manifest)

    def test_source_config_validation(self):
        parsed = parse_sources([{"path": "a.jsonl", "copies": 2}, {"path": "b.jsonl"}], self.root)
        self.assertEqual(parsed, (Source(self.a, 2), Source(self.b)))
        for invalid in ([], {}, ["a.jsonl"], [{}], [{"path": "missing"}],
                        [{"path": "a.jsonl", "copies": True}], [{"path": "a.jsonl", "copies": 0}],
                        [{"path": "a.jsonl", "copies": 1.5}], [{"path": "a.jsonl", "copise": 2}]):
            with self.subTest(invalid=invalid), self.assertRaisesRegex(ValueError, "trace.sources"):
                parse_sources(invalid, self.root)

    def test_output_aliases_and_existing_files_are_not_overwritten(self):
        for target in (self.b, self.root / "alias"):
            if target != self.b:
                target.symlink_to(self.b)
            with self.subTest(target=target), self.assertRaisesRegex(ValueError, "already exists"):
                compose((Source(self.b),), target, self.manifest)
            self.assertEqual(json.loads(self.b.read_text()), family())
        with self.assertRaisesRegex(ValueError, "different files"):
            compose((Source(self.b),), self.output, self.output)
        self.assertFalse(self.output.exists())

    def test_invalid_sources_fail_before_writing(self):
        for name, rows, error in (("empty", [], "empty"),
                                  ("duplicate", [family(), family()], "unique"),
                                  ("models", [family("a"), family("b", "other")], "same source model")):
            path = self.write(name, rows)
            with self.subTest(name=name), self.assertRaisesRegex(ValueError, error):
                compose((Source(path),), self.output, self.manifest)
            self.assertFalse(self.output.exists())

    @unittest.skipUnless(shutil.which("zstd"), "zstd is required")
    def test_compressed_source_has_uncompressed_provenance(self):
        compressed = self.root / "b.jsonl.zst"
        compressed.write_bytes(subprocess.run(["zstd", "-c", str(self.b)], capture_output=True, check=True).stdout)
        manifest = compose((Source(compressed),), self.output, self.manifest)
        self.assertEqual(manifest["sources"][0]["sha256"], hashlib.sha256(self.b.read_bytes()).hexdigest())
        self.assertTrue(verify_composition(self.output, self.manifest)["verified"])


if __name__ == "__main__":
    unittest.main()
