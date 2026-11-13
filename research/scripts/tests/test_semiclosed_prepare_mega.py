import hashlib
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from research.semiclosed.prepare_mega import build, verify
from research.scripts.tests.test_semiclosed_mix import family


@unittest.skipUnless(shutil.which("zstd"), "requires zstd")
class MegaTraceTests(unittest.TestCase):
    def test_seeded_order_preserves_complete_families_and_is_reproducible(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            sources = []
            for source in range(2):
                path = root / f"source{source}.jsonl"
                path.write_text("".join(json.dumps(family(f"family{i}")) + "\n" for i in range(9)))
                sources.append(path)
            a, b, c = (root / name for name in ("a", "b", "c"))
            first = build(sources, a, 42)
            build(sources, b, 42)
            build(sources, c, 43)
            self.assertEqual((a / "input.jsonl.zst").read_bytes(), (b / "input.jsonl.zst").read_bytes())
            self.assertNotEqual((a / "input.jsonl.zst").read_bytes(), (c / "input.jsonl.zst").read_bytes())
            self.assertTrue(first["verified"])
            self.assertEqual(first["source_totals"]["families"], 18)
            self.assertEqual(first["source_copies"], 2)
            for directory in (a, c):
                rows = [json.loads(line) for line in subprocess.check_output(
                    ["zstd", "-dc", str(directory / "input.jsonl.zst")]).splitlines()]
                originals = {r["id"]: r for r in map(json.loads, (directory / "round-robin.jsonl").read_text().splitlines())}
                self.assertEqual({r["id"]: r for r in rows}, originals)

    def test_changed_request_fails_even_with_updated_output_digest(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.jsonl"
            source.write_text(json.dumps(family()) + "\n")
            output = root / "mega"
            build([source], output, 42)
            row = json.loads(subprocess.check_output(["zstd", "-dc", str(output / "input.jsonl.zst")]))
            row["requests"][0]["in"] += 1
            data = (json.dumps(row) + "\n").encode()
            with (output / "input.jsonl.zst").open("wb") as handle:
                subprocess.run(["zstd", "-q", "-c"], input=data, stdout=handle, check=True)
            manifest = json.loads((output / "manifest.json").read_text())
            manifest["sha256"] = hashlib.sha256(data).hexdigest()
            (output / "manifest.json").write_text(json.dumps(manifest))
            with self.assertRaisesRegex(ValueError, "differs from the seeded permutation"):
                verify(output)

    def test_duplicate_sources_rejected_without_creating_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "source.jsonl"
            source.write_text(json.dumps(family()) + "\n")
            with self.assertRaisesRegex(ValueError, "distinct"):
                build([source, source], root / "mega", 42)
            self.assertFalse((root / "mega").exists())
