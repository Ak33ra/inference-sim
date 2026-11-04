#!/usr/bin/env python3
"""Compare a verified semi-closed replay with its unchanged source requests.

Latency units are milliseconds. Ratios are simulator/source. Percentiles use
linear interpolation. An optional CSV retains every paired request, including
rows ineligible for TTFT or TPOT comparison.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from dataclasses import dataclass
import json
import math
from pathlib import Path
import statistics
import sys


PERCENTILES = {"p50": 0.50, "p90": 0.90, "p95": 0.95, "p99": 0.99}
PROMPT_BOUNDS = (4096, 16384, 32768, 65536, 131072)
OUTPUT_BOUNDS = (1, 2, 128, 512, 2048)
SOURCE_COLUMNS = [
    "session_id", "step", "start_ms", "end_ms", "prompt_tokens", "completion_tokens",
    "model", "provider", "stream", "ttft_ms", "cache_read_tokens", "finish_reason",
]
FAILURE_COUNTERS = (
    "still_queued", "still_running", "dropped_unservable", "length_capped_requests",
    "timed_out_requests", "cancelled_requests", "context_overflow_requests", "failed_requests",
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def finite(value) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(value)


def distribution(values: list[float]) -> dict:
    ordered = sorted(values)
    result = {"count": len(ordered), "min": min(ordered, default=None),
              "max": max(ordered, default=None),
              "mean": statistics.fmean(ordered) if ordered else None}
    for name, fraction in PERCENTILES.items():
        position = (len(ordered) - 1) * fraction
        low, high = math.floor(position), math.ceil(position)
        result[name] = (ordered[low] + (ordered[high] - ordered[low]) * (position - low)
                        if ordered else None)
    return result


def paired_summary(pairs: list[tuple[float, float]]) -> dict:
    source = distribution([a for a, _ in pairs])
    simulator = distribution([b for _, b in pairs])
    relative = [(b - a) / a for a, b in pairs if a > 0]
    log_ratios = [math.log(b) - math.log(a) for a, b in pairs if a > 0 and b > 0]
    median = lambda values: statistics.median(values) if values else None
    return {
        "paired_count": len(pairs), "source": source, "simulator": simulator,
        "percentile_ratios": {p: simulator[p] / source[p] if source[p] else None
                              for p in PERCENTILES},
        "source_zero_count": sum(a == 0 for a, _ in pairs),
        "simulator_zero_count": sum(b == 0 for _, b in pairs),
        "relative_error_eligible_count": len(relative),
        "median_absolute_relative_error": median([abs(r) for r in relative]),
        "median_relative_bias": median(relative),
        "median_bias_ms": median([b - a for a, b in pairs]),
        "geometric_ratio_eligible_count": len(log_ratios),
        "geometric_mean_ratio": math.exp(statistics.fmean(log_ratios)) if log_ratios else None,
    }


@dataclass(frozen=True)
class RequestPair:
    week: str
    source: dict
    reported: dict
    engine: dict

    @property
    def source_e2e_ms(self) -> float:
        return self.source["end_ms"] - self.source["start_ms"]

    @property
    def simulator_e2e_ms(self) -> float:
        return self.reported["engine_e2e_us"] / 1000

    @property
    def simulator_ttft_ms(self) -> float:
        return self.reported["ttft_us"] / 1000

    @property
    def ttft_status(self) -> str:
        if not self.source["stream"]:
            return "nonstreaming"
        ttft = self.source["ttft_ms"]
        if not finite(ttft):
            return "missing_or_nonfinite"
        if ttft <= 0:
            return "nonpositive"
        if ttft >= 0.95 * self.source_e2e_ms:
            return "at_least_95_percent_of_e2e"
        return "trusted"

    @property
    def tpot_eligible(self) -> bool:
        return self.ttft_status == "trusted" and self.source["completion_tokens"] > 1

    def csv_row(self) -> dict:
        output = self.source["completion_tokens"]
        return {
            "week": self.week, "trace_id": self.reported["trace_id"],
            "request_id": self.reported["request_id"], **self.source,
            "engine_request_model_tag": self.engine.get("model"), "state": self.reported["state"],
            "source_e2e_ms": self.source_e2e_ms, "simulator_e2e_ms": self.simulator_e2e_ms,
            "simulator_ttft_ms": self.simulator_ttft_ms, "ttft_status": self.ttft_status,
            "source_tpot_ms": ((self.source_e2e_ms - self.source["ttft_ms"]) / (output - 1)
                               if self.tpot_eligible else None),
            "simulator_tpot_ms": ((self.simulator_e2e_ms - self.simulator_ttft_ms) / (output - 1)
                                  if self.tpot_eligible else None),
            "arrival_us": self.reported["arrival_us"],
            "completion_us": self.reported["completion_us"],
            "engine_completion_us": self.reported["engine_completion_us"],
        }


def cohort_summary(pairs: list[RequestPair]) -> dict:
    trusted = [p for p in pairs if p.ttft_status == "trusted"]
    tpot = [p for p in pairs if p.tpot_eligible]
    cache = [p for p in pairs if finite(p.source["cache_read_tokens"])]
    cache_prompt = sum(p.source["prompt_tokens"] for p in cache)
    cache_tokens = sum(p.source["cache_read_tokens"] for p in cache)
    return {
        "requests": len(pairs),
        "e2e_ms": paired_summary([(p.source_e2e_ms, p.simulator_e2e_ms) for p in pairs]),
        "ttft_ms": paired_summary([(p.source["ttft_ms"], p.simulator_ttft_ms) for p in trusted]),
        "tpot_ms": paired_summary([
            ((p.source_e2e_ms - p.source["ttft_ms"]) / (p.source["completion_tokens"] - 1),
             (p.simulator_e2e_ms - p.simulator_ttft_ms) / (p.source["completion_tokens"] - 1))
            for p in tpot]),
        "ttft_coverage": {
            "status_counts": dict(sorted(Counter(p.ttft_status for p in pairs).items())),
            "streaming_requests": sum(p.source["stream"] for p in pairs),
            "trusted_requests": len(trusted),
            "trusted_fraction_of_all": len(trusted) / len(pairs) if pairs else None,
            "trusted_fraction_of_streaming": (len(trusted) / sum(p.source["stream"] for p in pairs)
                                               if any(p.source["stream"] for p in pairs) else None),
            "tpot_eligible_requests": len(tpot),
        },
        "source_prompt_tokens": distribution([p.source["prompt_tokens"] for p in pairs]),
        "source_output_tokens": distribution([p.source["completion_tokens"] for p in pairs]),
        "source_cache_read": {
            "known_requests": len(cache), "missing_or_nonfinite_requests": len(pairs) - len(cache),
            "out_of_range_requests": sum(not 0 <= p.source["cache_read_tokens"] <= p.source["prompt_tokens"]
                                         for p in cache),
            "tokens": cache_tokens, "eligible_prompt_tokens": cache_prompt,
            "token_weighted_fraction": cache_tokens / cache_prompt if cache_prompt else None,
            "request_fractions": distribution([p.source["cache_read_tokens"] / p.source["prompt_tokens"]
                                               for p in cache]),
        },
        "source_finish_reasons": dict(sorted(Counter(p.source["finish_reason"] or "unknown" for p in pairs).items())),
    }


def length_bin(value: int, bounds: tuple[int, ...]) -> str:
    low = 0
    for high in bounds:
        if value < high:
            return f"[{low},{high})"
        low = high
    return f"[{low},inf)"


def strata(pairs: list[RequestPair]) -> dict:
    dimensions = {
        "week": lambda p: p.week,
        "week_provider": lambda p: f"{p.week}/{p.source['provider'] or 'unknown'}",
        "prompt_tokens": lambda p: length_bin(p.source["prompt_tokens"], PROMPT_BOUNDS),
        "output_tokens": lambda p: length_bin(p.source["completion_tokens"], OUTPUT_BOUNDS),
        "turn": lambda p: "first" if p.source["step"] == 0 else "continuation",
        "stream": lambda p: "streaming" if p.source["stream"] else "nonstreaming",
    }
    result = {}
    for name, key in dimensions.items():
        groups = defaultdict(list)
        for pair in pairs:
            groups[key(pair)].append(pair)
        result[name] = {group: cohort_summary(members) for group, members in sorted(groups.items())}
    return result


def read_pairs(source: Path, audits: list[dict], report: dict, engine: dict) -> list[RequestPair]:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    week_for = {a["trace_id"]: a["week"] for a in audits}
    members = defaultdict(dict)
    observed = {}
    for row in report["requests"]:
        week = week_for[row["trace_id"]]
        members[week][row["session_id"]] = row["trace_id"]
        observed[row["trace_id"], row["session_id"], row["step"]] = row
    metrics = {row["requestID"]: row for row in engine["requests"]}
    pairs, matched = [], set()
    for week, sessions in sorted(members.items()):
        wanted = pa.array(sorted(sessions), type=pa.string())
        parquet = pq.ParquetFile(source / week / "processed" / "requests.parquet")
        for batch in parquet.iter_batches(columns=SOURCE_COLUMNS):
            selected = batch.filter(pc.is_in(batch.column("session_id"), value_set=wanted))
            for row in selected.to_pylist():
                key = sessions[row["session_id"]], row["session_id"], row["step"]
                require(key in observed and key not in matched, f"unexpected source pair {key}")
                actual = observed[key]
                pair = RequestPair(week, row, actual, metrics[actual["request_id"]])
                require(pair.source_e2e_ms >= 0, f"negative source E2E for {key}")
                require(finite(pair.simulator_e2e_ms) and pair.simulator_e2e_ms >= 0,
                        f"invalid simulator E2E for {key}")
                require(finite(pair.simulator_ttft_ms) and pair.simulator_ttft_ms >= 0,
                        f"invalid simulator TTFT for {key}")
                require(abs(pair.simulator_ttft_ms - pair.engine["ttft_ms"]) < 0.00001,
                        f"report and engine TTFT differ for {key}")
                require(row["completion_tokens"] <= 1 or pair.simulator_ttft_ms <= pair.simulator_e2e_ms,
                        f"simulator TTFT exceeds E2E for {key}")
                pairs.append(pair)
                matched.add(key)
    require(matched == observed.keys(), "source comparison request set differs from verified report")
    return sorted(pairs, key=lambda p: (p.reported["trace_id"], p.source["session_id"], p.source["step"]))


def lifetimes(pairs: list[RequestPair], audits: list[dict], report: dict) -> dict:
    by_family, by_session = defaultdict(list), defaultdict(list)
    for pair in pairs:
        trace = pair.reported["trace_id"]
        by_family[trace].append(pair)
        by_session[trace, pair.source["session_id"]].append(pair)
    roots = {a["trace_id"]: a["root_session_id"] for a in audits}
    families, sessions = [], []
    for family in report["families"]:
        trace = family["trace_id"]
        start = min(p.source["start_ms"] for p in by_session[trace, roots[trace]])
        end = max(p.source["end_ms"] for p in by_family[trace])
        families.append({"trace_id": trace, "source_start_ms": start, "source_end_ms": end,
                         "source_makespan_ms": end - start,
                         "simulator_makespan_ms": family["makespan_us"] / 1000})
    for session in report["sessions"]:
        trace, sid = session["trace_id"], session["session_id"]
        members = by_session[trace, sid]
        start = min(p.source["start_ms"] for p in members)
        end = max(p.source["end_ms"] for p in members)
        sessions.append({"trace_id": trace, "session_id": sid,
                         "source_start_ms": start, "source_end_ms": end,
                         "source_makespan_ms": end - start,
                         "simulator_makespan_ms": (session["completion_us"] - session["arrival_us"]) / 1000})
    return {kind: {"makespan_ms": paired_summary([
                (row["source_makespan_ms"], row["simulator_makespan_ms"]) for row in rows]),
                  "pairs": rows}
            for kind, rows in (("families", families), ("sessions", sessions))}


def compare(input_dir: Path, source: Path, report_path: Path, metrics_path: Path,
            serving_model: str | None = None) -> tuple[dict, list[RequestPair]]:
    from research.semiclosed.verify import verify

    verification = verify(input_dir, source, report_path, metrics_path)
    report = json.loads(report_path.read_text())
    engine = json.loads(metrics_path.read_text())
    manifest = report["source"]
    audits = [a for line in (input_dir / "audit.jsonl").read_text().splitlines()
              if (a := json.loads(line))["status"] == "included"]
    failure_counts = {name: engine.get(name) for name in FAILURE_COUNTERS}
    for name, count in failure_counts.items():
        require(count in (None, 0), f"engine reports {name}={count}")
    for name, count in report["states"].items():
        require(count == (verification["requests"] if name == "completed" else 0),
                f"workload reports state {name}={count}")
    require(report["states"].get("completed") == verification["requests"], "missing completed state total")
    pairs = read_pairs(source, audits, report, engine)
    source_models = Counter(p.source["model"] for p in pairs)
    request_tags = Counter(p.engine.get("model") or "" for p in pairs)
    result = {
        "format_version": 1,
        "verification": verification,
        "definitions": {
            "units": "milliseconds", "ratio": "simulator/source",
            "percentiles": "linear interpolation at (n-1)*q over identical paired rows",
            "e2e": "source end_ms-start_ms; simulator engine_e2e_us/1000",
            "ttft": "streaming source rows with finite 0 < ttft_ms < 0.95*(end_ms-start_ms)",
            "tpot": "(E2E-TTFT)/(completion_tokens-1) on both sides; trusted source TTFT and output > 1",
            "family_makespan": "root first start to maximum request end across the complete family",
            "session_makespan": "first request start to last request end in that session",
            "relative_error": "(simulator-source)/source; source > 0; absolute error uses abs",
            "zero_handling": "zero source excludes relative errors; either zero excludes geometric ratio; zero percentile denominator gives null",
            "cache_read": "source cache_read_tokens/prompt_tokens; known values retained even outside [0,1]",
            "cohorts": "no latency filtering for E2E; TTFT and TPOT use only the stated eligibility rules",
            "turn": "first is step 0 in each session; continuation is step > 0",
            "provider": "anonymous source provider labels are scoped to each week",
            "prompt_bin_boundaries": [0, *PROMPT_BOUNDS],
            "output_bin_boundaries": [0, *OUTPUT_BOUNDS],
        },
        "selection": manifest["selection"],
        "source_counts": manifest["counts"],
        "excluded_families_by_reason": manifest["excluded_families_by_reason"],
        "selected_source_totals": report["selected"],
        "engine_totals": report["engine"],
        "matched_counts": {"source_requests": len(pairs), "reported_requests": len(report["requests"]),
                           "engine_requests": len(engine["requests"]), "families": len(audits),
                           "sessions": len(report["sessions"])},
        "reported_states": report["states"],
        "reported_source_tokens_by_state": report["source_tokens_by_state"],
        "engine_failure_counts": failure_counts,
        "model_mapping": [{"source_model": model, "serving_model": serving_model, "requests": count}
                          for model, count in sorted(source_models.items())],
        "serving_model_provenance": ("caller declaration via --serving-model; verify against the run scenario"
                                     if serving_model else "unknown; engine metrics do not record serving model identity"),
        "engine_request_model_tags": [{"model_tag": tag or None, "requests": count}
                                      for tag, count in sorted(request_tags.items())],
        "engine_cache_hit_rate": engine.get("cache_hit_rate"),
        "overall": cohort_summary(pairs),
        "strata": strata(pairs),
        **lifetimes(pairs, audits, report),
    }
    return result, pairs


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="conversion directory")
    parser.add_argument("--report", type=Path, required=True, help="semi-closed workload report JSON")
    parser.add_argument("--metrics", type=Path, required=True, help="engine metrics JSON")
    parser.add_argument("--source", type=Path, required=True, help="original FreeInference release directory")
    parser.add_argument("--output", type=Path, required=True, help="new comparison JSON; never overwritten")
    parser.add_argument("--pairs-csv", type=Path, help="optional new CSV with all paired requests")
    parser.add_argument("--serving-model", help="declared scenario model, not inferred from request tags; retain scenario evidence")
    args = parser.parse_args(argv)
    try:
        require(not args.output.exists(), f"output already exists: {args.output}")
        if args.pairs_csv:
            require(not args.pairs_csv.exists(), f"CSV already exists: {args.pairs_csv}")
            require(args.pairs_csv.resolve() != args.output.resolve(), "JSON and CSV paths must differ")
        result, pairs = compare(args.input, args.source, args.report, args.metrics, args.serving_model)
        result["request_pairs_csv"] = str(args.pairs_csv) if args.pairs_csv else None
        serialized = json.dumps(result, indent=2, allow_nan=False) + "\n"
        if args.pairs_csv:
            with args.pairs_csv.open("x", newline="", encoding="utf-8") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(pairs[0].csv_row()))
                writer.writeheader()
                writer.writerows(p.csv_row() for p in pairs)
        with args.output.open("x", encoding="utf-8") as stream:
            stream.write(serialized)
    except (ValueError, KeyError, OSError, ImportError) as error:
        parser.exit(1, f"comparison failed: {error}\n")
    print(f"Compared {len(pairs)} verified requests; wrote {args.output}")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    raise SystemExit(main())
