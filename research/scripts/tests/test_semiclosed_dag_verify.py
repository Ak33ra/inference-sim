from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from research.semiclosed.verify_dag import verify


class DAGVerificationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.directory = Path(self.temp.name)
        self.source = self.directory / "input.jsonl"
        self.report_path = self.directory / "workload.json"
        self.metrics_path = self.directory / "metrics.json"
        requests = [
            {"req_id": 0, "parent_ids": [], "type": "n", "t": 0, "api_time": 1, "in": 1, "out": 1},
            {"req_id": 1, "parent_ids": [0], "type": "n", "t": 1, "api_time": 0.1, "in": 1, "out": 1},
            {"req_id": 2, "parent_ids": [0], "type": "n", "t": 1.5, "api_time": 0.1, "in": 1, "out": 1},
        ]
        data = json.dumps({"id": "f", "requests": requests}).encode()
        self.source.write_bytes(data)
        totals = {"families": 1, "sessions": 1, "requests": 3, "prompt_tokens": 3,
                  "output_tokens": 3, "zero_output_requests": 0}
        self.report = {
            "complete": True, "accounting_match": True, "engine_available": True,
            "join_policy": "explicit", "source": {"source": {
                "format": "weka-dag", "digest_encoding": "uncompressed",
                "sha256": hashlib.sha256(data).hexdigest()}}, "selected": totals,
            "engine": {"completed_requests": 3, "prompt_tokens": 3, "output_tokens": 3},
            "requests": [], "arrivals": {"mode": "fixed", "interval_s": 1, "seed": 42},
            "families": [{"trace_id": "f", "birth_us": 0, "arrival_us": 0,
                          "completion_us": 500030, "makespan_us": 500030}],
            "sessions": [{"trace_id": "f", "session_id": "f", "arrival_us": 0, "completion_us": 500030}],
        }
        self.metrics = {"requests": [], "completed_requests": 3, "injected_requests": 3,
                        "total_input_tokens": 3, "total_output_tokens": 3,
                        "still_queued": 0, "still_running": 0, "dropped_unservable": 0,
                        "length_capped_requests": 0, "timed_out_requests": 0}
        for i, (arrival, completion, delay) in enumerate(((0, 10, 0), (10, 20, 0), (500010, 500030, 500000))):
            source = requests[i]
            self.report["requests"].append({
                "trace_id": "f", "session_id": "f", "step": i, "request_id": f"f/{i}",
                "state": "completed", "prompt_tokens": 1, "output_tokens": 1,
                "start_ms": round(source["t"] * 1000), "end_ms": round((source["t"] + source["api_time"]) * 1000),
                "arrival_us": arrival, "completion_us": completion, "e2e_us": completion - arrival,
                "engine_e2e_us": completion - arrival, "engine_completion_us": completion,
                "prerequisites": [] if i == 0 else [{"request_id": "f/0", "on": "completion"}],
                "delay_us": delay,
            })
            self.metrics["requests"].append({
                "requestID": f"f/{i}", "num_prefill_tokens": 1, "num_decode_tokens": 1,
                "output_tokens": 1, "arrived_at": arrival / 1e6, "e2e_ms": (completion - arrival) / 1000,
            })

    def check(self):
        self.report_path.write_text(json.dumps(self.report))
        self.metrics_path.write_text(json.dumps(self.metrics))
        return verify(self.source, self.report_path, self.metrics_path)

    def test_complete_work_and_graph_verify(self):
        result = self.check()
        self.assertTrue(result["verified"])
        self.assertEqual(result["dependency_edges"], 2)
        self.assertEqual(result["peak_active_families"], 1)

    def test_changed_source_timestamps_reject(self):
        self.report["requests"][0]["start_ms"] += 12345
        self.report["requests"][0]["end_ms"] += 12345
        with self.assertRaisesRegex(ValueError, "source timestamps"):
            self.check()

    def test_changed_edges_reject(self):
        self.report["requests"][1]["prerequisites"] = []
        with self.assertRaisesRegex(ValueError, "dependency edges"):
            self.check()

    def test_changed_engine_tokens_reject(self):
        self.metrics["requests"][1]["num_decode_tokens"] = 2
        with self.assertRaisesRegex(ValueError, "engine tokens"):
            self.check()

    def test_coherently_changed_request_timing_rejects(self):
        request = self.report["requests"][1]
        request["arrival_us"] += 5
        request["completion_us"] += 5
        request["engine_completion_us"] += 5
        self.metrics["requests"][1]["arrived_at"] = request["arrival_us"] / 1e6
        with self.assertRaisesRegex(ValueError, "dependency timing"):
            self.check()

    def test_saved_composed_run_requires_its_manifest(self):
        self.check()
        script = Path(__file__).resolve().parents[3] / "research/scripts/semi_closed.sh"
        command = [str(script), "verify", str(self.directory), "--python", sys.executable]
        raw = subprocess.run(command, cwd=self.directory, capture_output=True, text=True)
        self.assertEqual(raw.returncode, 0, raw.stderr)
        (self.directory / "config.resolved.json").write_text(json.dumps({
            "trace": {"format": "weka-dag", "sources": [{"path": "original.jsonl", "copies": 2}]}}))
        composed = subprocess.run(command, cwd=self.directory, capture_output=True, text=True)
        self.assertNotEqual(composed.returncode, 0)
        self.assertIn("composition.json", composed.stderr)
        self.assertNotIn('"verified": true', composed.stdout)

    def test_saved_run_dry_run_does_not_provision_python(self):
        self.check()
        (self.directory / "config.resolved.json").write_text(json.dumps({
            "trace": {"format": "weka-dag", "sources": [{"path": "original.jsonl"}]}}))
        commands = self.directory / "bin"
        commands.mkdir()
        marker = self.directory / "provisioned"
        uv = commands / "uv"
        uv.write_text(f'#!/bin/sh\ntouch "{marker}"\nexit 97\n')
        uv.chmod(0o755)
        script = Path(__file__).resolve().parents[3] / "research/scripts/semi_closed.sh"
        env = {**os.environ, "PATH": str(commands) + os.pathsep + os.environ["PATH"]}
        env.pop("PYTHON", None)
        result = subprocess.run([str(script), "verify", str(self.directory), "--dry-run"],
                                cwd=self.directory, env=env, capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertFalse(marker.exists())
        self.assertIn("research.semiclosed.mix", result.stdout)
        self.assertIn("composition.json", result.stdout)


if __name__ == "__main__":
    unittest.main()
