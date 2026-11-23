"""Validate and summarize the paired mega-trace scheduling and routing study.

The semi-closed workload report is authoritative for causal request, family,
session, and fork lifetimes. Engine metrics are authoritative for request TTFT,
engine E2E, serving instance, preemptions, and server time series.

Usage:
    python -m research.policy_study.analysis STUDY_DIR [--allow-partial] [--requests]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from dataclasses import dataclass
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import sys
import tempfile
from typing import Iterable, Sequence


SEEDS = tuple(range(42, 47))
GAPS = (4, 8)
STATS = ("count", "mean", "p50", "p90", "p99", "min", "max")
TERMINAL_ZERO_FIELDS = (
    "still_queued", "still_running", "dropped_unservable", "length_capped_requests",
    "timed_out_requests", "cancelled_requests", "context_overflow_requests", "failed_requests",
    "kv_allocation_failures",
)


class StudyError(ValueError):
    """The study or one of its supposedly complete runs is inconsistent."""


class IncompleteRun(StudyError):
    """A listed output has not reached a publishable terminal state."""


def require(condition: bool, message: str) -> None:
    if not condition:
        raise StudyError(message)


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise StudyError(f"{path}: {error}") from error
    require(isinstance(value, dict), f"{path}: expected a JSON object")
    return value


def percentile(values: Sequence[float], q: float) -> float:
    """Numpy-compatible linear quantile for an already sorted finite sequence."""
    position = (len(values) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    return values[lower] + (values[upper] - values[lower]) * (position - lower)


def distribution(values: Iterable[float]) -> dict:
    clean = sorted(float(value) for value in values if isinstance(value, (int, float))
                   and not isinstance(value, bool) and math.isfinite(value))
    if not clean:
        return {name: 0 if name == "count" else None for name in STATS}
    return {
        "count": len(clean), "mean": statistics.fmean(clean),
        "p50": percentile(clean, .50), "p90": percentile(clean, .90),
        "p99": percentile(clean, .99), "min": clean[0], "max": clean[-1],
    }


def sha256(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def canonical_json_hash(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class RunSpec:
    id: str
    kind: str
    router: str
    scheduler: str
    prefix_caching: bool
    mean_interarrival_s: int | None
    arrival_seed: int
    simulation_seed: int
    output: Path
    config: Path | None = None
    config_sha256: str | None = None

    @property
    def condition(self) -> tuple:
        return self.kind, self.router, self.scheduler, self.prefix_caching, self.mean_interarrival_s

    @property
    def loaded_key(self) -> tuple:
        return (self.router, self.scheduler, self.prefix_caching,
                self.mean_interarrival_s, self.arrival_seed)

    @property
    def analysis_seed(self) -> int:
        """The seed varied across replicas for this run kind."""
        return self.arrival_seed if self.kind == "loaded" else self.simulation_seed


@dataclass
class RunResult:
    spec: RunSpec
    family_rows: list[dict]
    fork_rows: list[dict]
    summary: dict
    logical_hash: str
    dependency_edges: int


def parse_specs(study: dict) -> list[RunSpec]:
    rows = study.get("runs")
    require(isinstance(rows, list) and rows, "study.json: runs must be a nonempty array")
    specs, ids, keys = [], set(), set()
    for index, row in enumerate(rows):
        require(isinstance(row, dict), f"study.json runs[{index}]: expected an object")
        missing = {name for name in ("id", "kind", "router", "scheduler", "prefix_caching",
                                      "mean_interarrival_s", "arrival_seed", "simulation_seed", "output")
                   if name not in row}
        require(not missing, f"study.json runs[{index}]: missing {sorted(missing)}")
        require(isinstance(row["id"], str) and row["id"], f"runs[{index}]: invalid id")
        require(row["id"] not in ids, f"study.json: duplicate run id {row['id']}")
        require(row["kind"] in ("loaded", "isolated"), f"{row['id']}: invalid kind")
        require(isinstance(row["router"], str) and row["router"], f"{row['id']}: invalid router")
        require(isinstance(row["scheduler"], str) and row["scheduler"], f"{row['id']}: invalid scheduler")
        require(type(row["prefix_caching"]) is bool, f"{row['id']}: prefix_caching must be boolean")
        mean = row["mean_interarrival_s"]
        if row["kind"] == "loaded":
            require(mean in GAPS, f"{row['id']}: loaded mean_interarrival_s must be 4 or 8")
        else:
            require(mean is None, f"{row['id']}: isolated mean_interarrival_s must be null")
        for field in ("arrival_seed", "simulation_seed"):
            require(type(row[field]) is int and row[field] in SEEDS,
                    f"{row['id']}: {field} must be one of {SEEDS}")
        output = Path(row["output"])
        require(output.is_absolute(), f"{row['id']}: output must be an absolute path")
        config = Path(row["config"]) if row.get("config") is not None else None
        config_hash = row.get("config_sha256")
        require((config is None) == (config_hash is None),
                f"{row['id']}: config and config_sha256 must appear together")
        if config is not None:
            require(config.is_absolute(), f"{row['id']}: config must be an absolute path")
            require(isinstance(config_hash, str) and len(config_hash) == 64,
                    f"{row['id']}: invalid config_sha256")
        spec = RunSpec(row["id"], row["kind"], row["router"], row["scheduler"],
                       row["prefix_caching"], mean, row["arrival_seed"],
                       row["simulation_seed"], output, config, config_hash)
        key = (*spec.condition, spec.analysis_seed)
        require(key not in keys, f"study.json: duplicate run cell {key}")
        ids.add(spec.id); keys.add(key); specs.append(spec)
    return specs


def matrix_gaps(specs: Sequence[RunSpec]) -> list[dict]:
    """Return absent seed/gap cells within every condition named by the manifest."""
    conditions = {(s.router, s.scheduler, s.prefix_caching) for s in specs if s.kind == "loaded"}
    present = {s.loaded_key for s in specs if s.kind == "loaded"}
    missing = []
    for router, scheduler, cache in sorted(conditions):
        for gap in GAPS:
            for seed in SEEDS:
                if (router, scheduler, cache, gap, seed) not in present:
                    missing.append({"kind": "loaded", "router": router, "scheduler": scheduler,
                                    "prefix_caching": cache, "mean_interarrival_s": gap, "seed": seed})
    isolated_conditions = {(s.router, s.scheduler, s.prefix_caching)
                           for s in specs if s.kind == "isolated"}
    isolated_present = {(s.router, s.scheduler, s.prefix_caching, s.simulation_seed)
                        for s in specs if s.kind == "isolated"}
    for router, scheduler, cache in sorted(isolated_conditions):
        for seed in SEEDS:
            if (router, scheduler, cache, seed) not in isolated_present:
                missing.append({"kind": "isolated", "router": router, "scheduler": scheduler,
                                "prefix_caching": cache, "mean_interarrival_s": None, "seed": seed})
    return missing


def locate_trace_metadata(study_dir: Path, study: dict) -> tuple[Path, Path]:
    repo = Path(__file__).resolve().parents[2]
    default = repo / "traces/mega-commit0-oolong"
    manifest = Path(study["trace_manifest"]) if study.get("trace_manifest") else default / "manifest.json"
    composition = Path(study["composition"]) if study.get("composition") else default / "composition.json"
    require(manifest.is_absolute() and composition.is_absolute(),
            "trace_manifest and composition must resolve to absolute paths")
    require(manifest.exists() and composition.exists(),
            "trace metadata not found; set absolute trace_manifest and composition in study.json")
    return manifest, composition


def validate_study_provenance(study_dir: Path, study: dict, manifest_path: Path,
                              composition_path: Path) -> tuple[str | None, dict]:
    binary_path = Path(study["binary"]) if study.get("binary") else None
    binary_hash = study.get("binary_sha256")
    require((binary_path is None) == (binary_hash is None),
            "study.json: binary and binary_sha256 must appear together")
    if binary_path is not None:
        require(binary_path.is_absolute() and binary_path.is_file(), "frozen binary is missing")
        require(isinstance(binary_hash, str) and sha256(binary_path) == binary_hash,
                "frozen binary sha256 differs")
    checks = (("input_compressed_sha256", manifest_path.parent / "input.jsonl.zst"),
              ("source_archive_sha256", study_dir / "provenance/source.tar.gz"))
    for field, path in checks:
        if field in study:
            require(path.is_file() and sha256(path) == study[field], f"{field} differs")
    return binary_hash, {
        "trace_manifest": str(manifest_path), "trace_manifest_sha256": sha256(manifest_path),
        "composition": str(composition_path), "composition_sha256": sha256(composition_path),
        "binary_sha256": binary_hash,
        "source_archive_sha256": study.get("source_archive_sha256"),
        "input_compressed_sha256": study.get("input_compressed_sha256"),
        "analyzer_sha256": sha256(Path(__file__)),
    }


def source_mapping(composition: dict, source_totals: dict) -> dict[str, dict]:
    # The composition digest names the pre-permutation round-robin artifact. The trace
    # manifest digest names the final deterministically shuffled artifact, so those two
    # digests intentionally differ while their logical work and provenance agree.
    require(composition.get("included") == source_totals, "composition totals differ from study")
    require(isinstance(composition.get("input_sha256"), str)
            and len(composition["input_sha256"]) == 64, "composition has no source digest")
    result = {}
    for row in composition.get("families", []):
        family = row.get("family_id")
        require(isinstance(family, str) and family and family not in result,
                "composition has an invalid or duplicate family_id")
        result[family] = {
            "source_family_id": row["source_family_id"],
            "source_index": row["source_index"],
            "copy_index": row["copy_index"],
        }
    return result


def run_files(spec: RunSpec) -> tuple[Path, Path, Path]:
    files = tuple(spec.output / name for name in ("workload.json", "metrics.json", "verification.json"))
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise IncompleteRun(f"{spec.id}: missing {', '.join(missing)}")
    return files


def validate_run_provenance(spec: RunSpec, binary_hash: str | None) -> None:
    if spec.config is None:
        return
    if not spec.config.is_file() or sha256(spec.config) != spec.config_sha256:
        raise StudyError(f"{spec.id}: frozen configuration is absent or changed")
    config = read_json(spec.config)
    require(config.get("output") == str(spec.output), f"{spec.id}: config output differs")
    require(config.get("run", {}).get("seed") == spec.simulation_seed,
            f"{spec.id}: config simulation seed differs")
    serving = config.get("serving", {})
    require(serving.get("engine", {}).get("enable_prefix_caching") == spec.prefix_caching,
            f"{spec.id}: config prefix caching differs")
    require(serving.get("scheduler", {}).get("name") == spec.scheduler,
            f"{spec.id}: config scheduler differs")
    router = serving.get("router", {})
    if spec.router == "llmd-jsq":
        require("endpoint_picker_config" in router and "research_policy" not in router,
                f"{spec.id}: config is not original llmd-jsq")
    else:
        require(router.get("research_policy") == spec.router,
                f"{spec.id}: config research router differs")
    status_path = spec.output / "study-status.json"
    if not status_path.is_file():
        raise IncompleteRun(f"{spec.id}: missing {status_path}")
    status = read_json(status_path)
    if not status.get("verified"):
        raise IncompleteRun(f"{spec.id}: study status is not verified")
    require(status.get("config_sha256") == spec.config_sha256,
            f"{spec.id}: status config provenance differs")
    if binary_hash is not None:
        require(status.get("binary_sha256") == binary_hash,
                f"{spec.id}: status binary provenance differs")
    artifacts = status.get("artifacts")
    require(isinstance(artifacts, dict) and artifacts, f"{spec.id}: status has no artifact hashes")
    for name, expected in artifacts.items():
        path = spec.output / name
        require(isinstance(expected, str) and len(expected) == 64 and path.is_file()
                and sha256(path) == expected, f"{spec.id}: artifact provenance differs for {name}")


def logical_work_signature(requests: list[dict]) -> tuple[str, int]:
    by_id, by_key = {}, {}
    for row in requests:
        key = row.get("trace_id"), row.get("session_id"), row.get("step")
        require(all(value is not None for value in key), "request lacks logical identity")
        require(row.get("request_id") not in by_id and key not in by_key, "duplicate logical request")
        by_id[row["request_id"]] = key
        by_key[key] = row
    canonical, edges = [], 0
    for key in sorted(by_key):
        row = by_key[key]
        parents = []
        for parent in row.get("prerequisites", []):
            require(parent.get("request_id") in by_id, f"unknown prerequisite {parent}")
            parents.append((*by_id[parent["request_id"]], parent.get("on")))
        edges += len(parents)
        canonical.append((*key, row.get("start_ms"), row.get("end_ms"), row.get("prompt_tokens"),
                          row.get("output_tokens"), row.get("delay_us"), sorted(parents)))
    return canonical_json_hash(canonical), edges


def validate_run(spec: RunSpec, workload: dict, metrics: dict, verification: dict,
                 source_totals: dict, input_sha: str, family_map: dict[str, dict]) -> tuple[str, int]:
    if not workload.get("complete") or not workload.get("accounting_match") or not verification.get("verified"):
        raise IncompleteRun(f"{spec.id}: workload or verification is incomplete")
    require(workload.get("join_policy") == "explicit", f"{spec.id}: expected explicit DAG dependencies")
    require(verification.get("source_totals") == source_totals, f"{spec.id}: verification source totals differ")
    require(workload.get("selected") == source_totals, f"{spec.id}: selected work differs")
    require(workload.get("source", {}).get("source", {}).get("sha256") == input_sha,
            f"{spec.id}: input provenance differs")
    require(workload.get("engine") == {"completed_requests": source_totals["requests"],
                                       "prompt_tokens": source_totals["prompt_tokens"],
                                       "output_tokens": source_totals["output_tokens"]},
            f"{spec.id}: workload engine accounting differs")
    for field, expected in (("completed_requests", source_totals["requests"]),
                            ("injected_requests", source_totals["requests"]),
                            ("total_input_tokens", source_totals["prompt_tokens"]),
                            ("total_output_tokens", source_totals["output_tokens"])):
        require(metrics.get(field) == expected, f"{spec.id}: metrics {field} differs")
    for field in TERMINAL_ZERO_FIELDS:
        require(metrics.get(field, 0) == 0, f"{spec.id}: metrics {field} is nonzero")
    families = workload.get("families", [])
    requests = workload.get("requests", [])
    require(len(families) == source_totals["families"] == len(family_map),
            f"{spec.id}: family count differs")
    require(len(requests) == source_totals["requests"], f"{spec.id}: request count differs")
    require({row.get("trace_id") for row in families} == set(family_map),
            f"{spec.id}: family identities differ from composition")
    require(all(row.get("state") == "completed" for row in requests), f"{spec.id}: unfinished request")
    request_indexes(workload, metrics)
    logical_hash, edges = logical_work_signature(requests)
    require(verification.get("dependency_edges") == edges, f"{spec.id}: dependency edge count differs")
    arrivals = workload.get("arrivals", {})
    require(arrivals.get("seed") == spec.arrival_seed, f"{spec.id}: arrival seed differs")
    if spec.kind == "loaded":
        require(arrivals.get("mode") == "poisson", f"{spec.id}: loaded arrivals are not Poisson")
        require(math.isclose(arrivals.get("rate_per_s", -1), 1 / spec.mean_interarrival_s,
                             rel_tol=0, abs_tol=1e-12), f"{spec.id}: arrival rate differs")
    else:
        require(arrivals.get("mode") == "fixed" and arrivals.get("interval_s") == 12000,
                f"{spec.id}: isolated arrivals must use fixed 12000 s gaps")
        ordered = sorted(families, key=lambda row: row["birth_us"])
        require(all(left["completion_us"] <= right["birth_us"] for left, right in zip(ordered, ordered[1:])),
                f"{spec.id}: isolated families overlap")
    return logical_hash, edges


def request_indexes(workload: dict, metrics: dict) -> tuple[dict, dict, dict]:
    work = {row["request_id"]: row for row in workload["requests"]}
    engine = {row["requestID"]: row for row in metrics["requests"]}
    require(len(work) == len(workload["requests"]) and len(engine) == len(metrics["requests"]),
            "duplicate workload or engine request ID")
    require(work.keys() == engine.keys(), "engine and workload request identities differ")
    for request_id, metric in engine.items():
        for field in ("ttft_ms", "e2e_ms", "scheduling_delay_ms"):
            value = metric.get(field)
            require(isinstance(value, (int, float)) and not isinstance(value, bool)
                    and math.isfinite(value) and value >= 0,
                    f"{request_id}: invalid required {field}")
        source = work[request_id]
        require(metric["num_prefill_tokens"] == source["prompt_tokens"]
                and metric["output_tokens"] == source["output_tokens"],
                f"{request_id}: engine and workload token counts differ")
    sessions = {(row["trace_id"], row["session_id"]): row for row in workload["sessions"]}
    require(len(sessions) == len(workload["sessions"]), "duplicate session summary")
    return work, engine, sessions


def family_rows(spec: RunSpec, workload: dict, family_map: dict[str, dict]) -> list[dict]:
    rows = []
    for family in workload["families"]:
        meta = family_map[family["trace_id"]]
        rows.append({
            "run_id": spec.id, "kind": spec.kind, "router": spec.router, "scheduler": spec.scheduler,
            "prefix_caching": spec.prefix_caching, "mean_interarrival_s": spec.mean_interarrival_s,
            "seed": spec.analysis_seed, "family_id": family["trace_id"], **meta,
            "birth_s": family["birth_us"] / 1e6, "completion_s": family["completion_us"] / 1e6,
            "makespan_s": family["makespan_us"] / 1e6,
        })
    return rows


def parse_spawn(request_id: str) -> int:
    try:
        return int(request_id.rsplit("/", 1)[1])
    except (IndexError, ValueError) as error:
        raise StudyError(f"cannot recover root spawn index from {request_id!r}") from error


def fork_rows(spec: RunSpec, workload: dict, family_map: dict[str, dict]) -> list[dict]:
    requests = {row["request_id"]: row for row in workload["requests"]}
    sessions = {(row["trace_id"], row["session_id"]): row for row in workload["sessions"]}
    children, spawns = defaultdict(set), {}
    child_spawn = {}
    for row in workload["requests"]:
        if row["session_id"] == row["trace_id"]:
            continue
        for dependency in row["prerequisites"]:
            parent = requests[dependency["request_id"]]
            if parent["session_id"] != row["trace_id"]:
                continue
            key = row["trace_id"], parent["request_id"]
            previous = child_spawn.setdefault((row["trace_id"], row["session_id"]), key)
            require(previous == key, f"{spec.id}: child session has multiple root spawns")
            children[key].add(row["session_id"])
            spawns[key] = parent
    result = []
    by_family = defaultdict(list)
    for (family_id, spawn_id), child_ids in children.items():
        spawn = spawns[family_id, spawn_id]
        finish = [((sessions[family_id, child]["completion_us"] - spawn["completion_us"]) / 1e6, child)
                  for child in sorted(child_ids)]
        require(all(value >= 0 for value, _ in finish), f"{spec.id}: child finishes before spawn completion")
        source = family_map[family_id]
        spawn_request = parse_spawn(spawn_id)
        row = {
            "run_id": spec.id, "kind": spec.kind, "router": spec.router, "scheduler": spec.scheduler,
            "prefix_caching": spec.prefix_caching, "mean_interarrival_s": spec.mean_interarrival_s,
            "seed": spec.analysis_seed, "family_id": family_id, **source,
            "spawn_request": spawn_request, "spawn_request_id": spawn_id,
            "child_count": len(finish), "children": json.dumps([child for _, child in finish]),
            "makespan_s": max(value for value, _ in finish),
            "mean_child_finish_s": statistics.fmean(value for value, _ in finish),
            "median_child_finish_s": statistics.median(value for value, _ in finish),
        }
        by_family[family_id].append(row)
    for family_id, rows in by_family.items():
        for ordinal, row in enumerate(sorted(rows, key=lambda item: item["spawn_request"]), 1):
            row["fork_ordinal"] = ordinal
            row["fork_label"] = f"rootspawn-{row['spawn_request']}"
            # Independent session-summary check used by the published slowdown analysis.
            key = family_id, row["spawn_request_id"]
            expected = max(sessions[family_id, child]["completion_us"] for child in children[key])
            actual = requests[row["spawn_request_id"]]["completion_us"] + round(row["makespan_s"] * 1e6)
            require(expected == actual, f"{spec.id}: fork/session completion disagreement")
            result.append(row)
    return sorted(result, key=lambda row: (row["family_id"], row["spawn_request"]))


def time_series_load(metrics: dict, workload: dict, required: bool) -> dict:
    series = metrics.get("server_series")
    if not series:
        if required:
            raise StudyError("loaded run has no server_series for load diagnostics")
        return {"available": False}
    engines = series.get("engines", [])
    require(engines, "server_series has no engines")
    width = series.get("interval_s")
    require(isinstance(width, (int, float)) and width > 0, "invalid server_series interval")
    last_birth = max(row["birth_us"] for row in workload["families"]) / 1e6
    start_bin = math.ceil(.1 * last_birth / width)
    end_bin = math.floor(.9 * last_birth / width)
    require(end_bin > start_bin, "central root-arrival window has no complete time-series bins")
    for engine in engines:
        require(len(engine.get("num_requests_running", [])) >= end_bin and
                len(engine.get("num_requests_waiting", [])) >= end_bin,
                f"server_series engine {engine.get('id')} is shorter than the analysis window")
    divisor = len(engines)
    running = [sum(engine["num_requests_running"][i] for engine in engines) / divisor
               for i in range(start_bin, end_bin)]
    waiting = [sum(engine["num_requests_waiting"][i] for engine in engines) / divisor
               for i in range(start_bin, end_bin)]
    return {
        "available": True, "window_start_s": start_bin * width, "window_end_s": end_bin * width,
        "bins": end_bin - start_bin, "replicas": divisor,
        "running_per_replica": distribution(running), "waiting_per_replica": distribution(waiting),
        "waiting_nonzero_fraction": sum(value > 0 for value in waiting) / len(waiting),
    }


def request_statistics(metrics: dict) -> dict:
    requests = metrics["requests"]
    return {
        "ttft_ms": distribution(row.get("ttft_ms") for row in requests),
        "e2e_ms": distribution(row.get("e2e_ms") for row in requests),
        "scheduling_delay_ms": distribution(row["scheduling_delay_ms"] for row in requests),
    }


def analyze_run(spec: RunSpec, source_totals: dict, input_sha: str,
                family_map: dict[str, dict], binary_hash: str | None = None) -> RunResult:
    validate_run_provenance(spec, binary_hash)
    workload_path, metrics_path, verification_path = run_files(spec)
    workload, metrics, verification = map(read_json, (workload_path, metrics_path, verification_path))
    logical_hash, edges = validate_run(spec, workload, metrics, verification,
                                       source_totals, input_sha, family_map)
    families = family_rows(spec, workload, family_map)
    forks = fork_rows(spec, workload, family_map)
    summary = {
        "run_id": spec.id, "kind": spec.kind, "router": spec.router, "scheduler": spec.scheduler,
        "prefix_caching": spec.prefix_caching, "mean_interarrival_s": spec.mean_interarrival_s,
        "arrival_seed": spec.arrival_seed, "simulation_seed": spec.simulation_seed,
        "output": str(spec.output), "input_sha256": input_sha, "logical_work_sha256": logical_hash,
        "dependency_edges": edges, "preemptions": metrics.get("preemption_count", 0),
        "cache_hit_rate": metrics.get("cache_hit_rate"),
        "statistics": {
            "family_makespan_s": distribution(row["makespan_s"] for row in families),
            "fork_makespan_s": distribution(row["makespan_s"] for row in forks),
            "fork_multi_child_makespan_s": distribution(row["makespan_s"] for row in forks
                                                          if row["child_count"] > 1),
            **request_statistics(metrics),
        },
        "load": time_series_load(metrics, workload, required=spec.kind == "loaded"),
    }
    feedback = metrics.get("research_scheduler")
    if feedback:
        calls = feedback["requests"].values()
        require(len(feedback["requests"]) == source_totals["requests"]
                and all(row["completed"] for row in calls), f"{spec.id}: scheduler accounting is incomplete")
        summary["scheduler_activity"] = {
            "mode": feedback["mode"], "backend": feedback["backend"], "config": feedback["config"],
            "inherited_calls": sum(row["inherited_service_us"] > 0 for row in calls),
            **{field: sum(row[field] for row in calls) for field in (
                "model_service_us", "queue_wait_us", "demotions", "promotions", "displacements",
                "resumes", "memory_preemptions", "recomputed_tokens")},
        }
    audit_path = spec.output / "router-decisions.json"
    if audit_path.exists():
        audit = read_json(audit_path)
        require(audit["stats"]["decisions"] == source_totals["requests"], f"{spec.id}: router decision count differs")
        summary["router_activity"] = audit["stats"]
    return RunResult(spec, families, forks, summary, logical_hash, edges)


def load_published_baselines(repo: Path) -> tuple[dict, dict]:
    baselines, hashes = {}, {}
    root = repo / "research/outputs/optrouting"
    for dataset, relative in (("commit0", Path("data")), ("oolong", Path("data/oolong"))):
        for kind, filename in (("family", "root_e2e.csv"), ("fork", "forks.csv")):
            path = root / relative / filename
            require(path.exists(), f"published optrouting baseline missing: {path}")
            hashes[str(path.relative_to(repo))] = sha256(path)
            with path.open(newline="") as stream:
                for row in csv.DictReader(stream):
                    key = (kind, row["family"], int(row["spawn_request"]) if kind == "fork" else None)
                    require(key not in baselines, f"duplicate published baseline {key}")
                    baselines[key] = {"optimal_s": float(row["optimal_s"]), "dataset": dataset,
                                      "children": int(row.get("children", 0) or 0)}
    return baselines, hashes


def isolated_references(results: Sequence[RunResult], expected_families: set[str],
                        allow_partial: bool) -> tuple[dict, dict]:
    references = {}
    for result in results:
        if result.spec.kind != "isolated":
            continue
        cache = result.spec.prefix_caching
        for kind, rows in (("family", result.family_rows), ("fork", result.fork_rows)):
            for row in rows:
                object_key = row["family_id"] if kind == "family" else (row["family_id"], row["spawn_request"])
                key = cache, kind, object_key
                candidate = (row["makespan_s"], result.spec.id, object_key)
                if key not in references or candidate[0] < references[key]["duration_s"]:
                    references[key] = {"duration_s": candidate[0], "run_id": candidate[1],
                                       "object_key": candidate[2]}
    coverage = {}
    cache_modes = {result.spec.prefix_caching for result in results if result.spec.kind == "loaded"}
    for cache in sorted(cache_modes):
        families = {key[2] for key in references if key[0] == cache and key[1] == "family"}
        expected_forks = {
            (row["family_id"], row["spawn_request"])
            for result in results if result.spec.kind == "loaded" and result.spec.prefix_caching == cache
            for row in result.fork_rows
        }
        forks = {key[2] for key in references if key[0] == cache and key[1] == "fork"}
        missing_families = sorted(expected_families - families)
        missing_forks = sorted(expected_forks - forks)
        coverage[str(cache).lower()] = {
            "families": len(families), "forks": len(forks),
            "missing_families": missing_families,
            "missing_forks": [{"family_id": family, "spawn_request": spawn}
                              for family, spawn in missing_forks],
        }
        if (missing_families or missing_forks) and not allow_partial:
            raise StudyError(
                f"isolated cache={cache} reference misses {len(missing_families)} families "
                f"and {len(missing_forks)} forks")
    return references, coverage


def attach_slowdowns(results: Sequence[RunResult], references: dict, published: dict) -> list[dict]:
    optrouting = []
    for result in results:
        for kind, rows in (("family", result.family_rows), ("fork", result.fork_rows)):
            for row in rows:
                object_key = row["family_id"] if kind == "family" else (row["family_id"], row["spawn_request"])
                reference = references.get((result.spec.prefix_caching, kind, object_key))
                row["isolated_reference_s"] = reference["duration_s"] if reference else None
                row["isolated_reference_run"] = reference["run_id"] if reference else None
                row["isolated_slowdown"] = row["makespan_s"] / reference["duration_s"] if reference else None
                key = (kind, row["source_family_id"], row.get("spawn_request") if kind == "fork" else None)
                baseline = published.get(key) if not result.spec.prefix_caching else None
                row["published_optimal_s"] = baseline["optimal_s"] if baseline else None
                row["published_cacheoff_slowdown"] = (row["makespan_s"] / baseline["optimal_s"]
                                                          if baseline else None)
                if baseline and result.spec.kind == "loaded":
                    if kind == "fork":
                        require(row["child_count"] == baseline["children"],
                                f"{result.spec.id}: published fork child count differs for {key}")
                    optrouting.append({
                        "run_id": result.spec.id, "router": result.spec.router,
                        "scheduler": result.spec.scheduler, "mean_interarrival_s": result.spec.mean_interarrival_s,
                        "seed": result.spec.arrival_seed, "dataset": baseline["dataset"], "kind": kind,
                        "family_id": row["family_id"], "source_family_id": row["source_family_id"],
                        "spawn_request": row.get("spawn_request"), "child_count": row.get("child_count", 0),
                        "loaded_s": row["makespan_s"], "published_optimal_s": baseline["optimal_s"],
                        "slowdown": row["published_cacheoff_slowdown"],
                    })
        stats = result.summary["statistics"]
        stats["family_isolated_slowdown"] = distribution(
            row["isolated_slowdown"] for row in result.family_rows if row["isolated_slowdown"] is not None)
        stats["fork_isolated_slowdown"] = distribution(
            row["isolated_slowdown"] for row in result.fork_rows if row["isolated_slowdown"] is not None)
        stats["fork_multi_child_isolated_slowdown"] = distribution(
            row["isolated_slowdown"] for row in result.fork_rows
            if row["child_count"] > 1 and row["isolated_slowdown"] is not None)
        stats["family_published_cacheoff_slowdown"] = distribution(
            row["published_cacheoff_slowdown"] for row in result.family_rows
            if row["published_cacheoff_slowdown"] is not None)
        stats["fork_multi_child_published_cacheoff_slowdown"] = distribution(
            row["published_cacheoff_slowdown"] for row in result.fork_rows
            if row["child_count"] > 1 and row["published_cacheoff_slowdown"] is not None)
    return optrouting


def flatten_statistics(summary: dict) -> Iterable[tuple[str, str, float]]:
    for metric, stats in summary["statistics"].items():
        for statistic in STATS:
            value = stats.get(statistic)
            if value is not None:
                yield metric, statistic, value
    load = summary.get("load", {})
    if load.get("available"):
        for metric in ("running_per_replica", "waiting_per_replica"):
            for statistic in STATS:
                value = load[metric].get(statistic)
                if value is not None:
                    yield f"load_{metric}", statistic, value
        yield "load_waiting_nonzero_fraction", "mean", load["waiting_nonzero_fraction"]


def aggregate_runs(results: Sequence[RunResult]) -> tuple[list[dict], list[dict]]:
    grouped = defaultdict(list)
    for result in results:
        grouped[result.spec.condition].append(result)
    payload, rows = [], []
    for condition, members in sorted(grouped.items(), key=lambda item: str(item[0])):
        by_metric = defaultdict(list)
        for result in members:
            for metric, statistic, value in flatten_statistics(result.summary):
                by_metric[metric, statistic].append(value)
        kind, router, scheduler, cache, gap = condition
        aggregates = {}
        for (metric, statistic), values in sorted(by_metric.items()):
            across = {"median": statistics.median(values), "min": min(values), "max": max(values)}
            aggregates.setdefault(metric, {})[statistic] = across
            rows.append({"kind": kind, "router": router, "scheduler": scheduler,
                         "prefix_caching": cache, "mean_interarrival_s": gap,
                         "metric": metric, "statistic": statistic, "seed_count": len(values), **across})
        payload.append({"kind": kind, "router": router, "scheduler": scheduler,
                        "prefix_caching": cache, "mean_interarrival_s": gap,
                        "seeds": sorted(member.spec.analysis_seed for member in members),
                        "seed_count": len(members), "statistics": aggregates})
    return payload, rows


def paired_deltas(results: Sequence[RunResult]) -> list[dict]:
    loaded = {result.spec.loaded_key: result for result in results if result.spec.kind == "loaded"}
    comparisons = []
    for result in loaded.values():
        spec = result.spec
        jsq = loaded.get(("jsq", "fcfs", spec.prefix_caching,
                          spec.mean_interarrival_s, spec.arrival_seed))
        if jsq and result is not jsq:
            comparisons.append(("vs_jsq_fcfs", result, jsq))
        if spec.mean_interarrival_s == 4:
            eight = loaded.get((spec.router, spec.scheduler, spec.prefix_caching, 8, spec.arrival_seed))
            if eight:
                comparisons.append(("4s_vs_8s", result, eight))
    rows = []
    for comparison, candidate, baseline in comparisons:
        for metric, attr, cohort in (
            ("family_makespan_s", "family_rows", lambda row: True),
            ("fork_multi_child_makespan_s", "fork_rows", lambda row: row["child_count"] > 1),
        ):
            def identity(row):
                return row["family_id"] if attr == "family_rows" else (row["family_id"], row["spawn_request"])
            left = {identity(row): row["makespan_s"] for row in getattr(candidate, attr) if cohort(row)}
            right = {identity(row): row["makespan_s"] for row in getattr(baseline, attr) if cohort(row)}
            require(left.keys() == right.keys(), f"paired object coverage differs: {candidate.spec.id} vs {baseline.spec.id}")
            deltas = [left[key] - right[key] for key in left]
            ratios = [left[key] / right[key] for key in left]
            for quantity, stats in (("delta_s", distribution(deltas)), ("ratio", distribution(ratios))):
                rows.append({
                    "comparison": comparison, "candidate_run": candidate.spec.id,
                    "baseline_run": baseline.spec.id, "router": candidate.spec.router,
                    "scheduler": candidate.spec.scheduler, "prefix_caching": candidate.spec.prefix_caching,
                    "mean_interarrival_s": candidate.spec.mean_interarrival_s,
                    "seed": candidate.spec.arrival_seed, "metric": metric, "quantity": quantity, **stats,
                })
    return rows


def optrouting_summary(rows: list[dict], hashes: dict) -> dict:
    grouped = defaultdict(list)
    metadata = {}
    for row in rows:
        cohorts = ["families"] if row["kind"] == "family" else ["forks_all"]
        if row["kind"] == "fork" and row["child_count"] > 1:
            cohorts.append("forks_multi_child")
        for dataset in (row["dataset"], "all"):
            for cohort in cohorts:
                key = row["run_id"], dataset, cohort
                grouped[key].append(row["slowdown"])
                metadata[key] = {name: row[name] for name in
                                 ("router", "scheduler", "mean_interarrival_s", "seed")}
    per_run = []
    for (run_id, dataset, cohort), values in sorted(grouped.items()):
        per_run.append({"run_id": run_id, "dataset": dataset, "cohort": cohort,
                        **metadata[run_id, dataset, cohort], "statistics": distribution(values),
                        "below_one": sum(value < 1 for value in values),
                        "max_deficit_below_one": max((1 - value for value in values if value < 1), default=0)})
    across = defaultdict(list)
    for row in per_run:
        condition = (row["router"], row["scheduler"], row["mean_interarrival_s"],
                     row["dataset"], row["cohort"])
        for statistic in STATS:
            value = row["statistics"].get(statistic)
            if value is not None:
                across[(*condition, statistic)].append(value)
    aggregate = []
    for key, values in sorted(across.items(), key=lambda item: str(item[0])):
        router, scheduler, gap, dataset, cohort, statistic = key
        aggregate.append({
            "router": router, "scheduler": scheduler, "mean_interarrival_s": gap,
            "dataset": dataset, "cohort": cohort, "statistic": statistic,
            "seed_count": len(values), "median": statistics.median(values),
            "min": min(values), "max": max(values),
        })
    return {
        "definition": "loaded duration / published cache-off optimal_s; fork duration starts at root spawn completion",
        "aggregation": "per-object ratios before cohort quantiles; values below one are retained",
        "source_sha256": hashes, "per_run": per_run, "aggregate": aggregate,
    }


def write_csv(path: Path, rows: Sequence[dict], fieldnames: Sequence[str] | None = None) -> None:
    if fieldnames is None:
        fieldnames = list(rows[0]) if rows else []
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def write_request_csv(path: Path, results: Sequence[RunResult]) -> None:
    fields = ("run_id", "kind", "router", "scheduler", "prefix_caching", "mean_interarrival_s", "seed",
              "request_id", "trace_id", "session_id", "step", "arrival_s", "completion_s", "ttft_ms",
              "e2e_ms", "scheduling_delay_ms", "admission_tokens", "output_tokens", "instance")
    with gzip.open(path, "wt", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for result in results:
            workload_path, metrics_path, _ = run_files(result.spec)
            workload, metrics = read_json(workload_path), read_json(metrics_path)
            work, engine, _ = request_indexes(workload, metrics)
            spec = result.spec
            for request_id, source in work.items():
                metric = engine[request_id]
                writer.writerow({
                    "run_id": spec.id, "kind": spec.kind, "router": spec.router,
                    "scheduler": spec.scheduler, "prefix_caching": spec.prefix_caching,
                    "mean_interarrival_s": spec.mean_interarrival_s, "seed": spec.analysis_seed,
                    "request_id": request_id, "trace_id": source["trace_id"],
                    "session_id": source["session_id"], "step": source["step"],
                    "arrival_s": source["arrival_us"] / 1e6,
                    "completion_s": source["completion_us"] / 1e6,
                    "ttft_ms": metric.get("ttft_ms"), "e2e_ms": metric.get("e2e_ms"),
                    "scheduling_delay_ms": metric["scheduling_delay_ms"],
                    "admission_tokens": metric.get("num_prefill_tokens"),
                    "output_tokens": metric.get("output_tokens", metric.get("num_decode_tokens")),
                    "instance": metric.get("handled_by"),
                })


def per_run_csv_rows(results: Sequence[RunResult]) -> list[dict]:
    rows = []
    for result in results:
        spec = result.spec
        for metric, statistic, value in flatten_statistics(result.summary):
            rows.append({"run_id": spec.id, "kind": spec.kind, "router": spec.router,
                         "scheduler": spec.scheduler, "prefix_caching": spec.prefix_caching,
                         "mean_interarrival_s": spec.mean_interarrival_s, "seed": spec.analysis_seed,
                         "metric": metric, "statistic": statistic, "value": value})
    return rows


def publish(study_dir: Path, study: dict, results: Sequence[RunResult], skipped: list[dict],
            matrix_missing: list[dict], references_coverage: dict, published_hashes: dict,
            optrouting_rows: list[dict], include_requests: bool, provenance: dict) -> Path:
    destination = study_dir / "analysis"
    temporary = Path(tempfile.mkdtemp(prefix=".analysis-", dir=study_dir))
    try:
        families = [row for result in results for row in result.family_rows]
        forks = [row for result in results for row in result.fork_rows]
        aggregates, aggregate_rows = aggregate_runs(results)
        paired = paired_deltas(results)
        per_run = [result.summary for result in results]
        write_csv(temporary / "families.csv", families)
        write_csv(temporary / "forks.csv", forks)
        write_csv(temporary / "per-run.csv", per_run_csv_rows(results))
        write_csv(temporary / "aggregate.csv", aggregate_rows)
        write_csv(temporary / "paired_deltas.csv", paired)
        write_csv(temporary / "optrouting_slowdown.csv", optrouting_rows)
        (temporary / "per-run.json").write_text(json.dumps(per_run, indent=2) + "\n")
        (temporary / "aggregate.json").write_text(json.dumps(aggregates, indent=2) + "\n")
        (temporary / "optrouting_slowdown.json").write_text(
            json.dumps(optrouting_summary(optrouting_rows, published_hashes), indent=2) + "\n")
        if include_requests:
            write_request_csv(temporary / "requests.csv.gz", results)
        final = {
            "format_version": 1, "study": str(study_dir / "study.json"),
            "input_sha256": study["input_sha256"], "source_totals": study["source_totals"],
            "provenance": provenance,
            "complete": not skipped and not matrix_missing,
            "analyzed_runs": len(results), "skipped_runs": skipped,
            "matrix_missing": matrix_missing, "isolated_reference_coverage": references_coverage,
            "definitions": {
                "family": "root birth to last request completion",
                "fork": "root spawn request completion to last direct child-session completion",
                "isolated_reference": "minimum observed matching-cache duration across isolated runs; best observed, not proven optimal",
                "load_window": "complete server-series bins in the central 80% of root-arrival time",
            },
            "files": sorted(path.name for path in temporary.iterdir()),
        }
        # summary.json is the publication marker and is deliberately written last.
        (temporary / "summary.json").write_text(json.dumps(final, indent=2) + "\n")
        if destination.exists():
            shutil.rmtree(destination)
        os.replace(temporary, destination)
        return destination
    except Exception:
        shutil.rmtree(temporary, ignore_errors=True)
        raise


def analyze(study_dir: Path, allow_partial: bool = False, include_requests: bool = False) -> Path:
    study_dir = study_dir.expanduser().resolve()
    study = read_json(study_dir / "study.json")
    require(isinstance(study.get("input_sha256"), str) and len(study["input_sha256"]) == 64,
            "study.json: invalid input_sha256")
    require(isinstance(study.get("source_totals"), dict), "study.json: source_totals is required")
    specs = parse_specs(study)
    missing_matrix = matrix_gaps(specs)
    if missing_matrix and not allow_partial:
        raise StudyError(f"study matrix is missing {len(missing_matrix)} cells")
    manifest_path, composition_path = locate_trace_metadata(study_dir, study)
    binary_hash, provenance = validate_study_provenance(
        study_dir, study, manifest_path, composition_path)
    trace_manifest, composition = read_json(manifest_path), read_json(composition_path)
    require(trace_manifest.get("sha256") == study["input_sha256"], "trace manifest sha256 differs from study")
    require(trace_manifest.get("source_totals") == study["source_totals"], "trace source_totals differ from study")
    mapping = source_mapping(composition, study["source_totals"])
    require(len(mapping) == study["source_totals"].get("families"), "composition family count differs")

    results, skipped = [], []
    expected_logical_hash = None
    expected_edges = None
    arrival_schedules = {}
    for spec in specs:
        try:
            result = analyze_run(spec, study["source_totals"], study["input_sha256"], mapping,
                                 binary_hash)
        except IncompleteRun as error:
            if not allow_partial:
                raise
            skipped.append({"run_id": spec.id, "reason": str(error)})
            continue
        if expected_logical_hash is None:
            expected_logical_hash, expected_edges = result.logical_hash, result.dependency_edges
        if "dependency_edges" in study:
            require(result.dependency_edges == study["dependency_edges"],
                    f"{spec.id}: dependency edge count differs from study")
        require(result.logical_hash == expected_logical_hash,
                f"{spec.id}: logical requests/tokens/dependencies differ from another run")
        require(result.dependency_edges == expected_edges, f"{spec.id}: dependency edge count differs")
        if spec.kind == "loaded":
            key = spec.mean_interarrival_s, spec.arrival_seed
            schedule = canonical_json_hash(sorted((row["family_id"], row["birth_s"]) for row in result.family_rows))
            expected = arrival_schedules.setdefault(key, schedule)
            require(schedule == expected, f"{spec.id}: paired family arrival schedule differs")
            result.summary["arrival_schedule_sha256"] = schedule
        results.append(result)
    require(results, "no complete runs to analyze")
    references, coverage = isolated_references(results, set(mapping), allow_partial)
    repo = Path(__file__).resolve().parents[2]
    published, published_hashes = load_published_baselines(repo)
    optrouting_rows = attach_slowdowns(results, references, published)
    return publish(study_dir, study, results, skipped, missing_matrix, coverage,
                   published_hashes, optrouting_rows, include_requests, provenance)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study_dir", type=Path, metavar="STUDY_DIR")
    parser.add_argument("--allow-partial", action="store_true",
                        help="skip listed run directories that are absent or incomplete")
    parser.add_argument("--requests", action="store_true",
                        help="also write the potentially large requests.csv.gz plotting table")
    args = parser.parse_args(argv)
    try:
        output = analyze(args.study_dir, args.allow_partial, args.requests)
    except (StudyError, OSError) as error:
        print(f"policy study analysis failed: {error}", file=sys.stderr)
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
