from pathlib import Path
import tempfile
import unittest

from research.policy_study.run import digest, run_one, validate_provenance


class FrozenStudyTests(unittest.TestCase):
    def test_resume_checks_config_before_reusing_completed_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "config.json"
            config.write_text("{}")
            expected = digest(config)
            config.write_text('{"different": true}')
            run = {"id": "verified-run", "output": str(root / "output"),
                   "config": str(config), "config_sha256": expected}
            with self.assertRaisesRegex(ValueError, "config.json"):
                run_one(run, {"helper_sources": {}}, root, False)

    def test_changed_catalog_is_rejected_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            provenance = root / "provenance"
            provenance.mkdir()
            files = {}
            for name in ("blis", "input.jsonl.zst", "source.tar.gz"):
                path = provenance / name
                path.write_text(name)
                files[name] = path
            catalog = provenance / "catalog"
            catalog.mkdir()
            hardware = catalog / "b200.json"
            hardware.write_text('{"capacity": 1}')
            manifest = {"binary": str(files["blis"]), "binary_sha256": digest(files["blis"]),
                        "input_compressed_sha256": digest(files["input.jsonl.zst"]),
                        "source_archive_sha256": digest(files["source.tar.gz"]),
                        "helper_sources": {}, "provenance_files": {},
                        "dependencies": {"catalog": {"snapshot": str(catalog),
                                                      "files": {"b200.json": digest(hardware)}}}}
            validate_provenance(root, manifest)
            hardware.write_text('{"capacity": 2}')
            with self.assertRaisesRegex(ValueError, "b200.json"):
                validate_provenance(root, manifest)


if __name__ == "__main__":
    unittest.main()
