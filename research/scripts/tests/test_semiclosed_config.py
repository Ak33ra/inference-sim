from __future__ import annotations

from dataclasses import replace
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from research.semiclosed.run import REPOSITORY, WEEK_FILES, WEEKS, execute, prepare


class ConfigRunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.directory = self.root / "configs"
        self.directory.mkdir()
        self.source = self.root / "source"
        self.source.mkdir()
        self.catalog = self.root / "catalog"
        self.catalog.mkdir()
        self.registry = self.root / "registry"
        self.registry.mkdir()
        self.defaults = self.root / "defaults.yaml"
        self.defaults.write_text("{}\n")
        self.path = self.directory / "study.json"
        self.config = {
            "version": 1,
            "trace": {"source": "../source", "model": "source-model", "weeks": 9},
            "serving": {
                "model": "serving-model",
                "cluster": {"hardware": "b200", "fabric": "ib-400g", "replicas": 2,
                            "gpus_per_node": 8, "tp": 8},
                "engine": {"block_size": 64, "max_num_seqs": 32, "enable_prefix_caching": True},
            },
            "catalog": "../catalog", "registry": "../registry", "defaults": "../defaults.yaml",
            "output": "../results/run", "python": sys.executable,
        }
        self.week(9)

    def week(self, number, complete=True):
        for name in WEEK_FILES if complete else WEEK_FILES[:1]:
            target = self.source / WEEKS[number - 1] / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("{}")

    def plan(self, **options):
        self.path.write_text(json.dumps(self.config))
        return prepare(self.path, caller=self.root, **options)

    def test_models_window_paths_and_commands_are_independent(self):
        self.config["trace"].update(start_ms=100, end_ms=200)
        plan = self.plan()
        self.assertEqual(plan.output, self.root / "results/run")
        self.assertEqual(plan.resolved["trace"]["source"], str(self.source))
        self.assertEqual(plan.resolved["catalog"], str(self.catalog))
        self.assertEqual(plan.resolved["defaults"], str(self.defaults))
        scenario, deployment = map(json.loads, plan.scenario.split("---\n"))
        self.assertEqual(scenario["model"], "serving-model")
        self.assertEqual(scenario["cluster"]["nodes"], 2)
        self.assertEqual(deployment["pools"][0]["nodes"], 2)
        conversion, build, replay, verify = plan.commands
        self.assertEqual(conversion[conversion.index("--model") + 1], "source-model")
        self.assertEqual(conversion[conversion.index("--week") + 1], "2026-07-12_2026-07-18")
        self.assertEqual(conversion[conversion.index("--start-ms") + 1], "100")
        self.assertEqual(conversion[conversion.index("--end-ms") + 1], "200")
        self.assertEqual(replay[replay.index("--num-instances") + 1], "2")
        self.assertEqual(replay[replay.index("--seed") + 1], "42")
        self.assertNotIn("--horizon", replay)
        self.assertEqual(verify[verify.index("--source") + 1], str(self.source))
        self.assertEqual(plan.resolved["python_argv"], (sys.executable, "-B"))
        self.assertFalse(plan.output.exists())

    def test_dag_input_arrivals_and_independent_verification(self):
        source = self.root / "synthetic.jsonl.zst"
        source.write_bytes(b"test")
        self.config["trace"] = {"format": "weka-dag", "path": "../synthetic.jsonl.zst"}
        self.config["run"] = {"seed": 7, "arrivals": {"mode": "poisson", "rate_per_s": 2}}
        plan = self.plan()
        copy, build, replay, verify = plan.commands
        self.assertEqual(copy[-2:], (str(source), str(plan.output / "input.jsonl.zst")))
        self.assertEqual(replay[replay.index("--semi-closed-join-policy") + 1], "explicit")
        self.assertEqual(replay[replay.index("--semi-closed-arrival-rate") + 1], "2")
        self.assertEqual(replay[replay.index("--semi-closed-arrival-seed") + 1], "7")
        self.assertIn(str(REPOSITORY / "research/semiclosed/verify_dag.py"), verify)
        self.assertNotIn("--source", verify)
        self.assertNotIn("--week", copy)
        self.assertEqual(plan.resolved["run"]["arrivals"], {"mode": "poisson", "rate_per_s": 2, "seed": 7})
        self.config["run"]["join_policy"] = "none"
        with self.assertRaisesRegex(ValueError, "explicit"):
            self.plan()

    def test_invalid_arrival_config_fails_before_output(self):
        source = self.root / "synthetic.jsonl"
        source.write_text("{}")
        self.config["trace"] = {"format": "weka-dag", "path": "../synthetic.jsonl"}
        for arrivals in ({"mode": "poisson"}, {"mode": "poisson", "rate_per_s": 0},
                         {"mode": "fixed", "interval_s": -1}, {"mode": "fixed", "rate_per_s": 1},
                         {"mode": "trace", "interval_s": 1}, {"mode": "unknown"},
                         {"mode": "fixed", "seed": 2**63}):
            with self.subTest(arrivals=arrivals):
                self.config["run"] = {"arrivals": arrivals}
                with self.assertRaisesRegex(ValueError, "arrivals"):
                    self.plan()
        self.assertFalse((self.root / "results/run").exists())

    def test_mixed_dag_sources_resolve_without_changing_arrival_rate(self):
        source = self.root / "synthetic.jsonl"
        source.write_text("{}")
        self.config["trace"] = {"format": "weka-dag", "sources": [
            {"path": "../synthetic.jsonl", "copies": 3}, {"path": "../synthetic.jsonl"}]}
        self.config["run"] = {"arrivals": {"mode": "fixed", "interval_s": .5}}
        plan = self.plan()
        self.assertEqual(plan.resolved["trace"]["sources"], [
            {"path": str(source), "copies": 3}, {"path": str(source), "copies": 1}])
        build, _, replay, verify = plan.commands
        self.assertIn("research.semiclosed.mix", build)
        self.assertIn("build", build)
        self.assertEqual(build[-1], str(plan.output / "config.resolved.json"))
        self.assertIn("research.semiclosed.mix", verify)
        self.assertEqual(verify[verify.index("--manifest") + 1], str(plan.output / "composition.json"))
        self.assertEqual(replay[replay.index("--semi-closed-arrival-interval-s") + 1], "0.5")
        self.assertFalse(plan.output.exists())
        self.config["trace"]["path"] = "../synthetic.jsonl"
        with self.assertRaisesRegex(ValueError, "exactly one"):
            self.plan()

    def test_router_is_snapshotted_and_used_by_the_generated_deployment(self):
        source = self.root / "picker.yaml"
        text = "apiVersion: llm-d.ai/v1\nkind: EndpointPickerConfig\n"
        source.write_text(text)
        self.config["serving"]["router"] = {"endpoint_picker_config": "../picker.yaml"}
        plan = self.plan()
        _, deployment = map(json.loads, plan.scenario.split("---\n"))
        self.assertEqual(deployment["router"], {"endpoint_picker_config": "router.yaml"})
        self.assertEqual(plan.resolved["serving"]["router"]["endpoint_picker_config"], str(source))
        self.assertFalse(plan.output.exists())
        source.write_text("changed after planning")
        self.assertEqual(execute(replace(plan, commands=())), 0)
        self.assertEqual((plan.output / "router.yaml").read_text(), text)

    def test_router_absence_preserves_defaults_and_invalid_paths_fail_early(self):
        plan = self.plan()
        _, deployment = map(json.loads, plan.scenario.split("---\n"))
        self.assertNotIn("router", deployment)
        self.assertNotIn("router", plan.resolved["serving"])
        self.assertIsNone(plan.router_config)
        for router in ({}, "jsq", {"endpoint_picker_config": "missing"},
                       {"endpoint_picker_config": "../source"}, {"policy": "jsq"}):
            with self.subTest(router=router):
                self.config["serving"]["router"] = router
                with self.assertRaisesRegex(ValueError, "serving.router"):
                    self.plan()
        self.assertFalse(plan.output.exists())

    def test_caller_overrides_and_environment_paths(self):
        del self.config["catalog"], self.config["registry"], self.config["python"]
        alias = self.root / "python-alias"
        alias.symlink_to(sys.executable)
        with patch.dict(os.environ, {"BLIS_CATALOG": "catalog", "BLIS_REGISTRY": "registry",
                                     "PYTHON": "./python-alias"}):
            plan = self.plan(output=Path("caller-run"))
        self.assertEqual(plan.output, self.root / "caller-run")
        self.assertEqual(plan.resolved["catalog"], str(self.catalog))
        self.assertEqual(plan.resolved["registry"], str(self.registry))
        self.assertEqual(plan.commands[0][0], str(alias))
        self.config["catalog"], self.config["registry"] = "../catalog", "../registry"
        self.config["python"] = "../python-alias"
        self.assertEqual(self.plan().commands[0][0], str(alias))
        plan = self.plan(python=sys.executable)
        self.assertEqual(plan.commands[0][0], sys.executable)

    def test_sparse_all_keeps_stable_ordinals_and_reports_incomplete(self):
        self.week(1)
        self.week(10)
        self.week(2, complete=False)
        self.config["trace"]["weeks"] = "all"
        plan = self.plan()
        self.assertEqual(plan.resolved["trace"]["weeks"], [1, 9, 10])
        self.assertIn(2, plan.resolved["unavailable_weeks"])
        self.assertEqual(plan.resolved["week_mapping"]["10"], "2026-07-19_2026-07-25")
        self.config["trace"]["weeks"] = [10, 1]
        self.assertEqual(self.plan().resolved["trace"]["weeks"], [1, 10])

    def test_invalid_week_selections(self):
        for weeks in (True, 0, 17, [], [9, 9], [True], [9.0], "9", [1]):
            with self.subTest(weeks=weeks):
                self.config["trace"]["weeks"] = weeks
                with self.assertRaisesRegex(ValueError, "trace.weeks"):
                    self.plan()

    def test_layout_packs_replicas_and_counts_spare_capacity(self):
        layout = self.config["serving"]["cluster"]
        for tp, replicas, nodes, allocated, active in (
                (2, 1, 1, 8, 2), (2, 4, 1, 8, 8), (2, 5, 2, 16, 10),
                (8, 2, 2, 16, 16), (16, 2, 4, 32, 32)):
            with self.subTest(tp=tp, replicas=replicas):
                layout.update(tp=tp, replicas=replicas)
                plan = self.plan()
                self.assertEqual(plan.resolved["serving"]["cluster"]["nodes"], nodes)
                self.assertEqual(plan.resolved["capacity"], {
                    "allocated_gpus": allocated, "active_gpus": active, "spare_gpus": allocated - active})
                self.assertEqual(plan.commands[2][plan.commands[2].index("--num-instances") + 1], str(replicas))

    def test_layout_rejects_impossible_or_unsupported_shapes(self):
        for key, value, error in (("tp", 3, "pack evenly"), ("pp", 2, "PP1/DP1"),
                                  ("dp", 2, "PP1/DP1"), ("replicas", True, "int"),
                                  ("nodes", 4, "unsupported field")):
            with self.subTest(key=key):
                original = dict(self.config["serving"]["cluster"])
                self.config["serving"]["cluster"][key] = value
                with self.assertRaisesRegex(ValueError, error):
                    self.plan()
                self.config["serving"]["cluster"] = original
        del self.config["serving"]["cluster"]["fabric"]
        with self.assertRaisesRegex(ValueError, "fabric.*multi-node"):
            self.plan()

    def test_draft_configuration_requires_and_preserves_selectors(self):
        self.config["serving"]["engine"].update(
            engram_config={"cpu_offload": True}, speculative={"method": "dspark", "num_spec_tokens": 5})
        with self.assertRaisesRegex(ValueError, "serving.acceptance"):
            self.plan()
        self.config["serving"]["acceptance"] = {"category": "coding", "thinking": "off"}
        plan = self.plan()
        replay = plan.commands[2]
        self.assertEqual(replay[replay.index("--speculative-thinking") + 1], "off")
        self.assertEqual(replay[replay.index("--speculative-category") + 1], "coding")
        self.config["serving"]["acceptance"]["thinking"] = False
        with self.assertRaisesRegex(ValueError, "thinking.*string"):
            self.plan()

    def test_typographical_and_numeric_errors_fail_before_output(self):
        cases = (
            ("version", True, "version"),
            ("serving.engine.max_num_seq", 32, "unsupported field"),
            ("serving.engine.block_size", True, "block_size"),
            ("serving.engine.gpu_memory_utilization", 1.1, "gpu_memory_utilization"),
            ("serving.engine.engram_config", {"cpu_offlaod": True}, "unsupported field"),
            ("serving.engine.cudagraph_capture_sizes", [1, True], "cudagraph_capture_sizes"),
            ("serving.engine.cudagraph_capture_sizes", [], "must not be empty"),
            ("run", {"series_interval_s": -1}, "nonnegative"),
            ("run", {"series_interval_s": float("inf")}, "nonfinite"),
            ("run", {"series_interval_s": 10**1000}, "float"),
            ("run", {"seed": 2**64}, "64-bit"),
            ("trace.start_ms", "1", "start_ms"),
            ("mystery", 1, "unsupported field"),
        )
        baseline = json.dumps(self.config)
        for path, value, error in cases:
            with self.subTest(path=path):
                self.config = json.loads(baseline)
                target = self.config
                names = path.split(".")
                for name in names[:-1]:
                    target = target[name]
                target[names[-1]] = value
                with self.assertRaisesRegex(ValueError, error):
                    self.plan()
                self.assertFalse((self.root / "results").exists())
        self.path.write_text('{"version":1,"version":1,"trace":{},"serving":{}}')
        with self.assertRaisesRegex(ValueError, "duplicate JSON key"):
            prepare(self.path)

    def test_existing_output_including_dangling_symlink_is_rejected(self):
        output = self.root / "already-there"
        output.symlink_to(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "already exists"):
            self.plan(output=output)

    def test_dry_run_alias_does_not_import_pyarrow_or_write(self):
        del self.config["python"]
        self.plan()
        fake = self.root / "pyarrow"
        fake.mkdir()
        marker = self.root / "imported"
        (fake / "__init__.py").write_text(f"from pathlib import Path\nPath({str(marker)!r}).touch()\n")
        before = set(self.root.rglob("*"))
        env = {**os.environ, "PYTHONPATH": str(self.root), "PYTHONDONTWRITEBYTECODE": "1"}
        result = subprocess.run(
            [str(REPOSITORY / "research/scripts/semi_closed.sh"), "run", str(self.path),
             "--output", "preview-output", "--dry-run"], cwd=self.root, env=env,
            capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("Trace model: source-model", result.stdout)
        self.assertIn("Serving model: serving-model", result.stdout)
        self.assertIn("Week 9: 2026-07-12_2026-07-18", result.stdout)
        self.assertIn(str(self.root / "preview-output"), result.stdout)
        self.assertEqual(set(self.root.rglob("*")), before)

    def test_failed_command_retains_provenance_and_stops_later_commands(self):
        plan = self.plan()
        marker = self.root / "should-not-run"
        plan = replace(plan, commands=(
            (sys.executable, "-B", "-c", "print('partial work'); raise SystemExit(7)"),
            (sys.executable, "-B", "-c", f"from pathlib import Path; Path({str(marker)!r}).touch()")))
        self.assertEqual(execute(plan), 7)
        self.assertFalse(marker.exists())
        self.assertEqual((plan.output / "config.original.json").read_bytes(), self.path.read_bytes())
        self.assertEqual(json.loads((plan.output / "commands.json").read_text()),
                         [list(argv) for argv in plan.commands])
        self.assertIn("partial work", (plan.output / "run.log").read_text())
        self.assertIn("Exit status: 7", (plan.output / "run.log").read_text())
        self.assertFalse((plan.output / "verification.json").exists())

    def test_verification_json_excludes_diagnostics(self):
        plan = replace(self.plan(), commands=((sys.executable, "-B", "-c",
            'import sys; print(\'{"complete": true}\'); print("diagnostic", file=sys.stderr)'),))
        self.assertEqual(execute(plan), 0)
        self.assertEqual(json.loads((plan.output / "verification.json").read_text()), {"complete": True})
        self.assertIn("diagnostic", (plan.output / "run.log").read_text())


if __name__ == "__main__":
    unittest.main()
