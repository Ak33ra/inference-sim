from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from research.session_split import common
from research.session_split.analysis import paired_objects, paired_statistics, per_run_statistics


class SessionSplitTests(unittest.TestCase):
    def test_oracle_uses_selected_timeline_and_explicit_predecessor(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = [{"request_id": "a", "trace_id": "f", "session_id": "s", "step": 0,
                     "arrival_us": 100, "completion_us": 120, "prerequisites": []},
                    {"request_id": "b", "trace_id": "f", "session_id": "s", "step": 1,
                     "arrival_us": 130, "completion_us": 150,
                     "prerequisites": [{"request_id": "a", "on": "completion"}]}]
            work = {"complete": True, "accounting_match": True, "requests": rows,
                    "sessions": [{"trace_id": "f", "session_id": "s", "arrival_us": 100, "completion_us": 150}]}
            path = root / "workload.json"
            path.write_text(json.dumps(work))
            old = {"runs": [{"id": "chosen", "output": directory}]}
            classes = {"trace_sha256": "a" * 64, "sessions": {"f/s": {"reference_run": "chosen", "reference_elapsed_us": 50}}}
            with patch.dict(common.TOTALS, {"requests": 2, "sessions": 1}):
                oracle = common.remaining_manifest(old, classes)
                self.assertEqual(oracle["requests"]["b"]["remaining_us"], 20)
                self.assertEqual(oracle["requests"]["b"]["reference_arrival_us"], 130)
                rows[1]["prerequisites"] = []
                path.write_text(json.dumps(work))
                with self.assertRaisesRegex(ValueError, "sequential"):
                    common.remaining_manifest(old, classes)

    def test_audit_reconstructs_extreme_and_native_admission(self):
        refs = {ident: {"runtime_session_id": "f/" + ident, "remaining_us": remaining,
                       "reference_run": "ref", "previous_request_id": ""}
                for ident, remaining in (("a", 100), ("b", 50))}
        work = {"requests": [{"request_id": ident, "trace_id": "f", "session_id": ident,
                              "arrival_us": 0, "completion_us": 10} for ident in refs]}
        metrics = {"preemption_count": 0, "requests": [{"requestID": ident, "handled_by": "instance_0", "scheduling_delay_ms": .002,
                                  "gateway_queue_delay_ms": .002} for ident in refs]}
        def event(ident, kind, at, count, batch=1):
            return {"kind": kind, "at_us": at, "request_id": ident, "session_id": "f/"+ident,
                    "remaining_us": refs[ident]["remaining_us"], "reference_run": "ref", "arrival_us": 0,
                    "preparation_instance": "instance_0" if ident == "a" else "instance_1",
                    "preparation_service_us": 2, "preparation_wait_us": 0,
                    "prepared_us": 2, "ready_count": count, "instance": "instance_0", "largest_first": True,
                    "capacity": {"batch_size": batch, "scheduled_tokens": 4, "used_kv_blocks": 1,
                                 "promised_kv_blocks": 2, "total_kv_blocks": 10}}
        events = [event("a", "prepare", 0, 0), event("b", "prepare", 0, 0),
                  event("a", "ready", 2, 1), event("b", "ready", 2, 2),
                  event("a", "claim", 2, 2), event("b", "claim", 2, 1, 2),
                  event("a", "complete", 10, 0), event("b", "complete", 10, 0)]
        audit = {"format_version": 1, "policy": "session-split", "events": events,
                 "router_latency_us": 0,
                 "renderers": [{"instance": f"instance_{i}", "api_servers": 1, "workers": 1, "rendered": int(i < 2), "wait_us": 0} for i in range(8)],
                 "reservations": {f"instance_{i}": {"admission_refusals": 0, "peak_promised_blocks": 2, "final_promised_blocks": 0, "final_used_kv_blocks": 0, "total_kv_blocks": 10, "preemptions": 0, "allocation_failures": 0} for i in range(8)}}
        self.assertEqual(common.verify_audit(audit, {"requests": refs}, work, metrics)["claims"], 2)
        bad = deepcopy(audit)
        bad["events"][4] = event("b", "claim", 2, 2)
        with self.assertRaisesRegex(ValueError, "priority"):
            common.verify_audit(bad, {"requests": refs}, work, metrics)

    def test_paired_tables_include_elapsed_and_slowdown(self):
        rows = []
        for policy, elapsed in (("jsq", 20), ("session-split", 10)):
            run = {"id": policy, "router": policy, "arrival_seed": 42}
            objects = [{"elapsed_us": elapsed, "slowdown": elapsed/5}]
            rows.extend(per_run_statistics(run, objects, objects))
        paired = paired_statistics(rows)
        self.assertEqual({r["metric"] for r in paired}, {"elapsed_us", "slowdown"})
        self.assertTrue(all(r["percent_change"] == -50 for r in paired))


if __name__ == "__main__":
    unittest.main()
