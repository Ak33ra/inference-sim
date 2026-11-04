from __future__ import annotations

from collections import Counter
from dataclasses import asdict, dataclass
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile

import pyarrow

from . import VERSION
from .convert import convert_family
from .model import TraceError, unsupported_reason
from .source import WeekSource, sha256_file

DEFAULT_REVISION = "16d3c04dae6ccaa0c48a9064e204e3a8f1eb9ade"
REPOSITORY = Path(__file__).resolve().parents[2]
DEFAULT_SOURCE = REPOSITORY / "traces/freeinference_agentic_trace/release"


@dataclass(frozen=True)
class Selection:
    weeks: tuple[str, ...]
    model: str
    start_ms: int | None = None
    end_ms: int | None = None
    unsupported: str = "exclude"

    def validate(self) -> None:
        if not self.weeks or len(set(self.weeks)) != len(self.weeks):
            raise TraceError("supply at least one distinct week")
        if any(Path(w).name != w or w in (".", "..") for w in self.weeks):
            raise TraceError("week must be a directory name within the release")
        if not self.model:
            raise TraceError("model must not be empty")
        if self.start_ms is not None and self.end_ms is not None and self.start_ms >= self.end_ms:
            raise TraceError("start-ms must precede end-ms")
        if self.unsupported not in ("exclude", "error"):
            raise TraceError("unsupported policy must be exclude or error")


def write_jsonl(stream, value: dict) -> None:
    json.dump(value, stream, separators=(",", ":"), sort_keys=True, allow_nan=False)
    stream.write("\n")


def converter_provenance() -> dict:
    sources = {p.name: sha256_file(p) for p in sorted(Path(__file__).parent.glob("*.py"))}
    code_hash = hashlib.sha256(json.dumps(sources, sort_keys=True).encode()).hexdigest()
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPOSITORY,
                              capture_output=True, text=True, check=False)
    return {"version": VERSION, "source_sha256": code_hash, "files": sources,
            "repository_commit": revision.stdout.strip() if revision.returncode == 0 else None,
            "python": platform.python_version(), "pyarrow": pyarrow.__version__}


def convert_release(source: Path, output: Path, selection: Selection, *,
                    revision: str = DEFAULT_REVISION, batch_size: int = 64,
                    temp_dir: Path | None = None) -> dict:
    selection.validate()
    if batch_size < 1:
        raise TraceError("batch-size must be positive")
    if not revision:
        raise TraceError("source revision must not be empty")
    if output.exists():
        raise TraceError(f"output already exists: {output}; choose a new directory")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f".{output.name}-", dir=output.parent))
    try:
        counts: Counter = Counter()
        excluded: Counter = Counter()
        stats: Counter = Counter()
        inputs = []
        with (staging / "corpus.jsonl").open("w", encoding="utf-8") as corpus, \
                (staging / "audit.jsonl").open("w", encoding="utf-8") as audit:
            for week in sorted(selection.weeks):
                dataset = WeekSource(source, week)
                inputs.append(dataset.provenance())
                families = dataset.families()
                tool_keys = dataset.subagent_tool_keys()
                selected = []
                for family in families:
                    rows = list(family.all_requests())
                    counts.update({"source_families": 1, "source_requests": len(rows)})
                    models = {r.model for r in rows}
                    start = family.requests[family.root_id][0].start_ms
                    reason = None
                    if selection.model not in models:
                        reason = "other_model"
                    elif models != {selection.model}:
                        reason = "mixed_model_family"
                    elif ((selection.start_ms is not None and start < selection.start_ms) or
                          (selection.end_ms is not None and start >= selection.end_ms)):
                        reason = "outside_root_start_window"
                    else:
                        reason = unsupported_reason(family, tool_keys)
                        if reason and selection.unsupported == "error":
                            raise TraceError(f"{family.trace_id}: {reason}")
                    if reason:
                        excluded[reason] += 1
                        counts.update({"excluded_families": 1, "excluded_requests": len(rows)})
                        write_jsonl(audit, {"trace_id": family.trace_id, "status": "excluded",
                                           "reason": reason, "requests": len(rows),
                                           "matching_model_requests": sum(r.model == selection.model for r in rows)})
                    else:
                        selected.append(family)
                with dataset.prefixes(selected, batch_size, temp_dir) as prefixes:
                    for family in selected:
                        trace, evidence = convert_family(family, prefixes)
                        write_jsonl(corpus, trace)
                        write_jsonl(audit, evidence)
                        stats.update(evidence["stats"])
        if not stats["families"]:
            raise TraceError(f"no eligible families; exclusions: {dict(excluded)}")
        if counts["source_requests"] != counts["excluded_requests"] + stats["requests"]:
            raise TraceError("request conservation failed")
        manifest = {
            "format_version": 1, "converter": converter_provenance(),
            "source": {"dataset": "harvardMadsys/freeinference_agentic_trace",
                       "declared_revision": revision, "weeks": inputs},
            "selection": {**asdict(selection), "weeks": sorted(selection.weeks),
                          "window_policy": "root start in [start_ms,end_ms); retain complete exported family"},
            "conversion": {
                "target": "weka-trace-v7", "time_unit": "seconds relative to family root start",
                "prefix_mapping": "each fourth chained 16-token ID; cap at floor(prompt_tokens/64)",
                "uncovered_prefix": "AgentX synthesizes the suffix; no tokenizer rescaling",
                "token_counts": "absolute provider counts; AgentX adds template/marker overhead and max(out,1)",
                "child_boundary": "first LLM start to last LLM end; final tools are not represented",
                "join_policy": "AgentX inferred joins; audit records candidates, not causal ground truth",
                "recommended_loader": {"split_flattened_agents": False},
                "completeness": "within published weekly tables; publisher omissions cannot be recovered",
            },
            "counts": dict(counts), "excluded_families_by_reason": dict(sorted(excluded.items())),
            "included": dict(stats),
            "artifacts": {name: {"sha256": sha256_file(staging / name),
                                 "bytes": (staging / name).stat().st_size}
                          for name in ("corpus.jsonl", "audit.jsonl")},
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True, allow_nan=False) + "\n", encoding="utf-8")
        if output.exists():
            raise TraceError(f"output appeared during conversion: {output}")
        os.rename(staging, output)
        return manifest
    finally:
        if staging.exists():
            shutil.rmtree(staging)
