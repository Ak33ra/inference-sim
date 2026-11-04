from __future__ import annotations

import copy
from dataclasses import replace
import json
import unittest

from research.scripts.compare_semiclosed import (
    RequestPair, cohort_summary, length_bin, lifetimes, paired_summary, strata,
)


def pair(step=0, *, start=1000, end=1100, ttft=20, output=5, stream=True,
         sim_e2e=100, sim_ttft=20, session="root", cache=50):
    source = {"session_id": session, "step": step, "start_ms": start, "end_ms": end,
              "prompt_tokens": 100, "completion_tokens": output, "model": "source-model",
              "provider": "A", "stream": stream, "ttft_ms": ttft,
              "cache_read_tokens": cache, "finish_reason": "stop"}
    reported = {"trace_id": "week/root", "request_id": f"week/root/{session}/{step}",
                "state": "completed", "engine_e2e_us": sim_e2e * 1000,
                "ttft_us": sim_ttft * 1000, "arrival_us": 0,
                "completion_us": sim_e2e * 1000, "engine_completion_us": sim_e2e * 1000}
    return RequestPair("week", source, reported, {"model": "simulator-model", "itl_ms": 999})


class ComparisonTests(unittest.TestCase):
    def test_exact_match_and_tpot_use_same_definition(self):
        p = pair()
        original = copy.deepcopy(p.source)
        summary = cohort_summary([p])
        for name in ("e2e_ms", "ttft_ms", "tpot_ms"):
            self.assertEqual(summary[name]["geometric_mean_ratio"], 1)
            self.assertEqual(summary[name]["median_absolute_relative_error"], 0)
        self.assertEqual(summary["tpot_ms"]["simulator"]["p50"], 20)
        self.assertEqual(p.csv_row()["source_tpot_ms"], 20)
        self.assertEqual(p.source, original)

    def test_equal_distributions_do_not_hide_wrong_pairs(self):
        result = paired_summary([(10, 100), (100, 10)])
        self.assertEqual(result["percentile_ratios"]["p50"], 1)
        self.assertAlmostEqual(result["median_absolute_relative_error"], 4.95)
        self.assertAlmostEqual(result["geometric_mean_ratio"], 1)

    def test_ttft_coverage_preserves_nonstreaming_and_invalid_rows(self):
        pairs = [pair(ttft=None), pair(ttft=0), pair(ttft=95), pair(ttft=96),
                 pair(ttft=94), pair(stream=False), pair(output=1), pair(output=0)]
        result = cohort_summary(pairs)
        self.assertEqual(result["e2e_ms"]["paired_count"], 8)
        self.assertEqual(result["ttft_ms"]["paired_count"], 3)
        self.assertEqual(result["tpot_ms"]["paired_count"], 1)
        self.assertEqual(result["ttft_coverage"]["status_counts"], {
            "missing_or_nonfinite": 1, "nonpositive": 1,
            "at_least_95_percent_of_e2e": 2, "trusted": 3, "nonstreaming": 1,
        })
        groups = strata(pairs)["stream"]
        self.assertEqual(groups["nonstreaming"]["e2e_ms"]["paired_count"], 1)
        self.assertEqual(groups["nonstreaming"]["ttft_ms"]["paired_count"], 0)

    def test_zero_denominators_have_explicit_counts_and_no_nan(self):
        result = paired_summary([(0, 0), (0, 10), (10, 0), (10, 20)])
        self.assertEqual(result["relative_error_eligible_count"], 2)
        self.assertEqual(result["geometric_ratio_eligible_count"], 1)
        self.assertEqual(result["source_zero_count"], 2)
        self.assertAlmostEqual(result["geometric_mean_ratio"], 2)
        zero = paired_summary([(0, 0)])
        self.assertIsNone(zero["percentile_ratios"]["p50"])
        self.assertIsNone(zero["median_absolute_relative_error"])
        json.dumps([result, zero, paired_summary([])], allow_nan=False)

    def test_family_makespan_includes_last_child(self):
        pairs = [pair(start=1000, end=1100), pair(1, start=1200, end=1400),
                 pair(start=1300, end=2000, session="child")]
        report = {"families": [{"trace_id": "week/root", "makespan_us": 700000}],
                  "sessions": [
                      {"trace_id": "week/root", "session_id": "root",
                       "arrival_us": 0, "completion_us": 300000},
                      {"trace_id": "week/root", "session_id": "child",
                       "arrival_us": 100000, "completion_us": 700000},
                  ]}
        result = lifetimes(pairs, [{"trace_id": "week/root", "root_session_id": "root"}], report)
        self.assertEqual(result["families"]["pairs"][0]["source_makespan_ms"], 1000)
        self.assertEqual(result["families"]["pairs"][0]["simulator_makespan_ms"], 700)
        self.assertEqual([r["source_makespan_ms"] for r in result["sessions"]["pairs"]], [400, 700])

    def test_cache_coverage_and_turn_groups_do_not_hide_outliers(self):
        pairs = [pair(cache=50), pair(1, cache=None), pair(2, cache=150)]
        cache = cohort_summary(pairs)["source_cache_read"]
        self.assertEqual(cache["known_requests"], 2)
        self.assertEqual(cache["missing_or_nonfinite_requests"], 1)
        self.assertEqual(cache["out_of_range_requests"], 1)
        self.assertEqual(cache["token_weighted_fraction"], 1)
        self.assertEqual(cache["request_fractions"]["max"], 1.5)
        self.assertEqual(strata(pairs)["turn"]["continuation"]["requests"], 2)

    def test_length_bins_have_exact_boundaries(self):
        self.assertEqual(length_bin(0, (1, 2, 128)), "[0,1)")
        self.assertEqual(length_bin(1, (1, 2, 128)), "[1,2)")
        self.assertEqual(length_bin(127, (1, 2, 128)), "[2,128)")
        self.assertEqual(length_bin(128, (1, 2, 128)), "[128,inf)")

    def test_provider_labels_are_scoped_by_week_and_model_tags_are_optional(self):
        first = pair()
        second = replace(first, week="other-week", engine={})
        groups = strata([first, second])["week_provider"]
        self.assertEqual(set(groups), {"week/A", "other-week/A"})
        self.assertTrue(all(group["requests"] == 1 for group in groups.values()))
        self.assertIsNone(second.csv_row()["engine_request_model_tag"])


if __name__ == "__main__":
    unittest.main()
