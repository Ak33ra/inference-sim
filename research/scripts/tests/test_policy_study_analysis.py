import tempfile
import unittest
from pathlib import Path

from research.policy_study import analysis


def spec(run_id, kind="loaded", cache=False, arrival=42, simulation=42, gap=4):
    return analysis.RunSpec(
        run_id, kind, "jsq", "fcfs", cache, gap if kind == "loaded" else None,
        arrival, simulation, Path("/tmp") / run_id,
    )


class PolicyStudyAnalysisTest(unittest.TestCase):
    def test_request_samples_must_be_complete_finite_and_token_matched(self):
        workload = {"requests": [{"request_id": "r", "prompt_tokens": 5, "output_tokens": 2}],
                    "sessions": []}
        row = {"requestID": "r", "num_prefill_tokens": 5, "output_tokens": 2,
               "ttft_ms": 1, "e2e_ms": 2, "scheduling_delay_ms": 0}
        analysis.request_indexes(workload, {"requests": [row]})
        invalid = [[], [row, row], [{**row, "requestID": "other"}],
                   [{**row, "output_tokens": 3}]]
        invalid += [[{**row, field: value}] for field in
                    ("ttft_ms", "e2e_ms", "scheduling_delay_ms")
                    for value in (None, float("nan"), float("inf"), -1, True)]
        for rows in invalid:
            with self.subTest(rows=rows), self.assertRaises(analysis.StudyError):
                analysis.request_indexes(workload, {"requests": rows})

    def test_parse_specs_uses_kind_specific_replica_seed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            study = {"runs": [
                {"id": "loaded", "kind": "loaded", "router": "jsq", "scheduler": "fcfs",
                 "prefix_caching": False, "mean_interarrival_s": 4, "arrival_seed": 43,
                 "simulation_seed": 42, "output": str(root / "loaded")},
                {"id": "isolated", "kind": "isolated", "router": "jsq", "scheduler": "fcfs",
                 "prefix_caching": False, "mean_interarrival_s": None, "arrival_seed": 42,
                 "simulation_seed": 43, "output": str(root / "isolated")},
            ]}
            loaded, isolated = analysis.parse_specs(study)
            self.assertEqual(43, loaded.analysis_seed)
            self.assertEqual(43, isolated.analysis_seed)

    def test_matrix_gaps_tracks_loaded_arrival_and_isolated_simulation_seeds(self):
        specs = [spec("loaded", arrival=42), spec("isolated", "isolated", simulation=42, gap=None)]
        missing = analysis.matrix_gaps(specs)
        cells = {(row["kind"], row["mean_interarrival_s"], row["seed"]) for row in missing}
        self.assertNotIn(("loaded", 4, 42), cells)
        self.assertNotIn(("isolated", None, 42), cells)
        self.assertIn(("loaded", 8, 42), cells)
        self.assertIn(("isolated", None, 43), cells)

    def test_fork_duration_starts_at_spawn_completion(self):
        run = spec("run")
        workload = {
            "requests": [
                {"request_id": "f/f/3", "trace_id": "f", "session_id": "f",
                 "completion_us": 10_000_000, "prerequisites": []},
                {"request_id": "f/c1/0", "trace_id": "f", "session_id": "c1",
                 "completion_us": 13_000_000,
                 "prerequisites": [{"request_id": "f/f/3", "on": "completion"}]},
                {"request_id": "f/c2/0", "trace_id": "f", "session_id": "c2",
                 "completion_us": 15_000_000,
                 "prerequisites": [{"request_id": "f/f/3", "on": "completion"}]},
            ],
            "sessions": [
                {"trace_id": "f", "session_id": "c1", "completion_us": 13_000_000},
                {"trace_id": "f", "session_id": "c2", "completion_us": 15_000_000},
            ],
        }
        rows = analysis.fork_rows(
            run, workload,
            {"f": {"source_family_id": "source", "source_index": 0, "copy_index": 0}},
        )
        self.assertEqual(1, len(rows))
        self.assertEqual(5, rows[0]["makespan_s"])
        self.assertEqual(4, rows[0]["mean_child_finish_s"])
        self.assertEqual(2, rows[0]["child_count"])

    def test_isolated_reference_is_best_observed_within_cache_mode(self):
        def result(run, duration, cache, kind):
            run_spec = spec(run, kind, cache=cache, simulation=42, gap=None if kind == "isolated" else 4)
            family = {"family_id": "f", "makespan_s": duration}
            return analysis.RunResult(run_spec, [family], [], {"statistics": {}}, "hash", 1)

        results = [
            result("loaded-off", 10, False, "loaded"),
            result("loaded-on", 10, True, "loaded"),
            result("isolated-off-a", 4, False, "isolated"),
            result("isolated-off-b", 3, False, "isolated"),
            result("isolated-on", 2, True, "isolated"),
        ]
        references, coverage = analysis.isolated_references(results, {"f"}, False)
        self.assertEqual(3, references[False, "family", "f"]["duration_s"])
        self.assertEqual(2, references[True, "family", "f"]["duration_s"])
        self.assertEqual(1, coverage["false"]["families"])


if __name__ == "__main__":
    unittest.main()
