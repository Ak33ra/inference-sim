import copy
from dataclasses import replace
import json
from pathlib import Path
import sys
import tempfile
import unittest

from research.policy_study.analysis import distribution
from research.policy_study.run import completed, digest, write_json
from research.semiclosed.run import execute
from .analysis import aggregate, object_rows, paired_sessions, paired_statistics, per_run_statistics
from .common import (RESERVED, assert_baseline_parity, reference_manifest,
                     validate_provenance)
from .run import adapted_plan, run_one


class SessionReferenceTests(unittest.TestCase):
    def work(self, duration_a=10, duration_b=10):
        return {"sessions": [
            {"trace_id": "family-a", "session_id": "same", "arrival_us": 90, "completion_us": 90 + duration_a},
            {"trace_id": "family-b", "session_id": "same", "arrival_us": 100, "completion_us": 100 + duration_b}],
            "families": [{"trace_id": "family-a", "makespan_us": duration_a},
                         {"trace_id": "family-b", "makespan_us": duration_b}]}

    def test_runtime_namespace_minimum_ties_and_deterministic_source(self):
        manifest = reference_manifest([("z", self.work()), ("a", self.work())], "f" * 64, 1)
        reverse = reference_manifest([("a", self.work()), ("z", self.work())], "f" * 64, 1)
        self.assertEqual(manifest, reverse)
        self.assertEqual(manifest["sessions"]["family-a/same"]["class"], "short")
        self.assertEqual(manifest["sessions"]["family-b/same"]["class"], "long")
        self.assertEqual(manifest["sessions"]["family-a/same"]["reference_run"], "a")
        lower = reference_manifest([("z", self.work()), ("a", self.work(11, 9))], "f" * 64, 1)
        self.assertEqual(lower["sessions"]["family-b/same"]["class"], "short")
        self.assertEqual(lower["sessions"]["family-a/same"]["reference_elapsed_us"], 10)

    def test_individual_pair_keeps_shared_reference_and_raw_identity(self):
        rows = []
        for router, elapsed in [("jsq", 100), ("session-reservation", 75)]:
            rows.append({"router": router, "arrival_seed": 42, "runtime_session_id": "family/same",
                         "trace_id": "family", "session_id": "same", "class": "short",
                         "reference_elapsed_us": 50, "elapsed_us": elapsed, "slowdown": elapsed / 50})
        pair = paired_sessions(rows)[0]
        self.assertEqual(pair["paired_ratio"], .75)
        self.assertEqual(pair["percent_change"], -25)
        self.assertEqual(pair["reference_elapsed_us"], 50)
        self.assertEqual(pair["trace_id"], "family")
        self.assertEqual(pair["session_id"], "same")

    def test_reference_coverage_and_duplicate_identity_rejected(self):
        work = self.work()
        work["sessions"] = work["sessions"][:1]
        with self.assertRaisesRegex(ValueError, "coverage"):
            reference_manifest([("a", self.work()), ("b", work)], "f" * 64, 1)
        work["sessions"].append(work["sessions"][0])
        with self.assertRaisesRegex(ValueError, "duplicate"):
            reference_manifest([("a", work)], "f" * 64, 1)

    def test_elapsed_includes_waits_and_ratios_below_one_remain(self):
        ref = reference_manifest([("a", self.work(100, 200))], "f" * 64, 1)
        work = self.work(50, 400)
        for family in work["families"]:
            family.update(birth_us=0, arrival_us=0, completion_us=family["makespan_us"])
        run = {"id": "test", "router": "jsq", "arrival_seed": 42}
        sessions, families = object_rows(run, work, ref)
        self.assertEqual([row["slowdown"] for row in sessions], [.5, 2])
        rows = per_run_statistics(run, sessions, families)
        mean = next(row for row in rows if row["unit"] == "session" and row["cohort"] == "all" and row["statistic"] == "mean")
        self.assertEqual(mean["value"], 1.25)
        self.assertEqual(mean["below_one_count"], 1)
        self.assertEqual(sessions[0]["elapsed_us"], 50)
        self.assertEqual(sessions[0]["trace_id"], "family-a")

    def test_linear_quantiles_and_paired_changes_are_distinct(self):
        self.assertEqual(distribution([1, 3])["p50"], 2)
        self.assertAlmostEqual(distribution([1, 3])["p99"], 2.98)
        rows = []
        for seed, base, candidate in [(42, 1, 2), (43, 2, 10), (44, 100, 100)]:
            for router, value in [("jsq", base), ("session-reservation", candidate)]:
                rows.append({"arrival_seed": seed, "router": router, "unit": "session",
                             "cohort": "short", "statistic": "mean", "value": value})
        deltas = paired_statistics(rows)
        self.assertEqual([row["percent_change"] for row in deltas], [100, 400, 0])
        summary = aggregate(deltas, ("unit", "cohort", "statistic"), "percent_change")
        self.assertEqual(summary[0]["median"], 100)
        grouped = aggregate(rows, ("router",), "value")
        self.assertEqual(grouped[1]["median"] / grouped[0]["median"], 5)


class FrozenExperimentTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_changed_provenance_rejected_before_execution(self):
        path = self.root / "classes.json"
        path.write_text("{}")
        study = {"frozen_files": {str(path): digest(path)}}
        path.write_text('{"changed": true}')
        with self.assertRaisesRegex(ValueError, "classes.json"):
            validate_provenance(study)
        run = {"config": str(path), "config_sha256": "f" * 64,
               "preparation_config": str(path), "preparation_config_sha256": "f" * 64}
        study.update(binary=str(path), binary_sha256="f" * 64,
                     session_class_manifest=str(path), session_class_manifest_sha256="f" * 64)
        with self.assertRaisesRegex(ValueError, "classes.json"):
            run_one(study, run, False)

    def test_changed_saved_artifact_is_not_reused(self):
        artifact = self.root / "workload.json"
        artifact.write_text("{}")
        write_json(self.root / "study-status.json", {"verified": True, "binary_sha256": "binary",
                   "config_sha256": "config", "artifacts": {"workload.json": digest(artifact)}})
        run = {"id": "one", "output": str(self.root), "config_sha256": "config"}
        self.assertTrue(completed(run, "binary"))
        artifact.write_text('{"changed": true}')
        with self.assertRaisesRegex(ValueError, "saved artifact changed"):
            completed(run, "binary")

    def test_baseline_parity_ignores_only_catalog_path(self):
        actual, old = self.root / "new", self.root / "old"
        actual.mkdir(); old.mkdir()
        for directory in (actual, old):
            for name in ("workload.json", "router-decisions.json", "verification.json"):
                write_json(directory / name, {"accounting": 7})
            write_json(directory / "metrics.json", {"requests": [7], "catalog": {"path": str(directory), "hash": "same"}})
        assert_baseline_parity(actual, old)
        write_json(actual / "metrics.json", {"requests": [8], "catalog": {"path": str(actual), "hash": "same"}})
        with self.assertRaisesRegex(ValueError, "metrics.json"):
            assert_baseline_parity(actual, old)

    def test_adapter_saves_effective_policy_and_preserves_other_inputs(self):
        for name in ("catalog", "registry"):
            (self.root / name).mkdir()
        (self.root / "defaults.yaml").write_text("{}")
        (self.root / "input.jsonl.zst").write_bytes(b"trace")
        base = {"version": 1, "trace": {"format": "weka-dag", "path": str(self.root / "input.jsonl.zst")},
                "serving": {"model": "gpt-oss-120b", "cluster": {"hardware": "b200", "fabric": "ib-400g", "replicas": 8, "gpus_per_node": 1, "tp": 1},
                            "engine": {"enable_prefix_caching": False}, "scheduler": {"name": "fcfs"},
                            "router": {"research_policy": "jsq", "parameters": {}}},
                "run": {"seed": 42, "arrivals": {"mode": "poisson", "rate_per_s": .25, "seed": 43}},
                "output": str(self.root / "output"), "catalog": str(self.root / "catalog"),
                "registry": str(self.root / "registry"), "defaults": str(self.root / "defaults.yaml"), "python": sys.executable}
        candidate = copy.deepcopy(base)
        params = {"session_class_manifest": str(self.root / "classes.json"), "reserved_instances": RESERVED}
        candidate["serving"]["router"] = {"research_policy": "session-reservation", "parameters": params}
        base_path, actual_path = self.root / "base.json", self.root / "candidate.json"
        write_json(base_path, base); write_json(actual_path, candidate)
        run = {"preparation_config": str(base_path), "config": str(actual_path), "router": "session-reservation"}
        plan = adapted_plan(run)
        command = plan.commands[2]
        self.assertEqual(command[command.index("--research-router") + 1], "session-reservation")
        self.assertEqual(command[command.index("--semi-closed-arrival-seed") + 1], "43")
        self.assertEqual(plan.resolved["serving"]["router"]["parameters"], params)
        self.assertEqual(execute(replace(plan, commands=())), 0)
        self.assertEqual(json.loads((plan.output / "config.original.json").read_text()), candidate)
        self.assertEqual(json.loads((plan.output / "router.json").read_text()), params)
        base["run"]["seed"] = 99
        write_json(base_path, base)
        run["preparation_config"] = str(base_path)
        base["output"] = str(self.root / "elsewhere")
        write_json(base_path, base)
        with self.assertRaisesRegex(ValueError, "beyond the explicit router adapter"):
            adapted_plan(run)


if __name__ == "__main__":
    unittest.main()
