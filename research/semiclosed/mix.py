"""Compose independent Weka DAG users and verify their source provenance."""

from __future__ import annotations

import argparse
from copy import deepcopy
from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import subprocess
import sys


@dataclass(frozen=True)
class Source:
    path: Path
    copies: int = 1

    def resolved(self) -> dict:
        return {"path": str(self.path), "copies": self.copies}


def parse_sources(value: object, base: Path) -> tuple[Source, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError("trace.sources must be a nonempty list")
    sources = []
    for index, item in enumerate(value):
        field = f"trace.sources[{index}]"
        if not isinstance(item, dict) or set(item) - {"path", "copies"} or "path" not in item:
            raise ValueError(f"{field} requires path and accepts only path and copies")
        if not isinstance(item["path"], str) or not item["path"].strip():
            raise ValueError(f"{field}.path must be a nonempty string")
        path = (base / Path(item["path"]).expanduser()).resolve()
        copies = item.get("copies", 1)
        if type(copies) is not int or copies < 1:
            raise ValueError(f"{field}.copies must be a positive integer")
        if not path.is_file():
            raise ValueError(f"{field}.path is not a file: {path}")
        sources.append(Source(path, copies))
    return tuple(sources)


def read_source(path: Path) -> tuple[list[dict], str]:
    data = (subprocess.run(["zstd", "-dc", str(path)], capture_output=True, check=True).stdout
            if path.suffix == ".zst" else path.read_bytes())
    rows = [json.loads(line) for line in data.splitlines() if line.strip()]
    seen = set()
    if not rows:
        raise ValueError(f"empty DAG source: {path}")
    for row in rows:
        if not isinstance(row, dict) or not isinstance(row.get("id"), str) or not row["id"] or row["id"] in seen:
            raise ValueError(f"DAG source requires unique nonempty family ids: {path}")
        seen.add(row["id"])
        if row.get("block_size") != 64 or row.get("hash_id_scope") != "local":
            raise ValueError(f"DAG source requires local 64-token blocks: {path}")
        models = row.get("models")
        if not isinstance(models, list) or len(models) != 1 or not isinstance(models[0], str) or not models[0]:
            raise ValueError(f"DAG source requires one model: {path}")
        if not isinstance(row.get("requests"), list) or not row["requests"]:
            raise ValueError(f"DAG family has no requests: {path}: {row['id']}")
    return rows, hashlib.sha256(data).hexdigest()


def namespace(source_index: int, copy_index: int) -> str:
    return f"source{source_index}/copy{copy_index}/"


def rename_sessions(requests: list[dict], prefix: str) -> None:
    for request in requests:
        if request["type"] == "subagent":
            request["agent_id"] = prefix + request["agent_id"]
            rename_sessions(request["requests"], prefix)


def totals(rows: list[dict]) -> dict:
    result = {"families": len(rows), "sessions": len(rows), "requests": 0,
              "prompt_tokens": 0, "output_tokens": 0, "zero_output_requests": 0}

    def collect(requests):
        for request in requests:
            if request["type"] == "subagent":
                result["sessions"] += 1
                collect(request["requests"])
            else:
                result["requests"] += 1
                result["prompt_tokens"] += request["in"]
                result["output_tokens"] += request["out"]
                result["zero_output_requests"] += request["out"] == 0

    for row in rows:
        collect(row["requests"])
    return result


def compose(sources: tuple[Source, ...], output: Path, manifest_path: Path) -> dict:
    if output.resolve() == manifest_path.resolve():
        raise ValueError("composed input and manifest must be different files")
    for target in (output, manifest_path):
        if target.exists() or target.is_symlink():
            raise ValueError(f"composition output already exists: {target}")
    loaded = [read_source(source.path) for source in sources]
    models = {row["models"][0] for rows, _ in loaded for row in rows}
    if len(models) != 1:
        raise ValueError("mixed DAG sources must have the same source model; current deployment serves one model")
    composed, families = [], []
    for ordinal in range(max(len(rows) for rows, _ in loaded)):
        for index, (source, (rows, _)) in enumerate(zip(sources, loaded)):
            if ordinal >= len(rows):
                continue
            original = rows[ordinal]
            for copy in range(source.copies):
                row = deepcopy(original)
                prefix = namespace(index, copy)
                row["id"] = prefix + original["id"]
                rename_sessions(row["requests"], prefix)
                composed.append(row)
                families.append({"family_id": row["id"], "source_index": index,
                                 "copy_index": copy, "source_family_id": original["id"]})
    data = b"".join((json.dumps(row, separators=(",", ":"), allow_nan=False) + "\n").encode() for row in composed)
    manifest = {"version": 1, "ordering": "round-robin", "digest_encoding": "uncompressed",
                "sources": [{**source.resolved(), "sha256": digest, "totals": totals(rows)}
                            for source, (rows, digest) in zip(sources, loaded)],
                "families": families, "included": totals(composed),
                "input_sha256": hashlib.sha256(data).hexdigest()}
    with output.open("xb") as handle:
        handle.write(data)
    with manifest_path.open("x") as handle:
        json.dump(manifest, handle, indent=2, allow_nan=False)
        handle.write("\n")
    return manifest


def verify_composition(input_path: Path, manifest_path: Path) -> dict:
    manifest = json.loads(manifest_path.read_bytes())
    if manifest["version"] != 1 or manifest["ordering"] != "round-robin" or manifest["digest_encoding"] != "uncompressed":
        raise ValueError("unsupported composition manifest")
    data = input_path.read_bytes()
    if hashlib.sha256(data).hexdigest() != manifest["input_sha256"]:
        raise ValueError("composed input digest differs")
    originals, expected, weighted_totals, expected_order = {}, set(), {}, []
    source_specs = parse_sources([{k: s[k] for k in ("path", "copies")} for s in manifest["sources"]], manifest_path.parent)
    loaded = []
    for index, (source, saved) in enumerate(zip(source_specs, manifest["sources"])):
        rows, digest = read_source(source.path)
        counts = totals(rows)
        if digest != saved["sha256"] or counts != saved["totals"]:
            raise ValueError(f"composition source changed: {source.path}")
        loaded.append(rows)
        for name, count in counts.items():
            weighted_totals[name] = weighted_totals.get(name, 0) + count * source.copies
        for row in rows:
            originals[index, row["id"]] = row
            for copy in range(source.copies):
                expected.add((index, copy, row["id"]))
    for ordinal in range(max(map(len, loaded))):
        for index, rows in enumerate(loaded):
            if ordinal < len(rows):
                expected_order.extend((index, copy, rows[ordinal]["id"]) for copy in range(source_specs[index].copies))
    rows = [json.loads(line) for line in data.splitlines() if line.strip()]
    mapping = manifest["families"]
    if len(rows) != len(mapping) or len(rows) != len(expected):
        raise ValueError("composition family count differs")
    actual_order, seen = [], set()
    for row, entry in zip(rows, mapping):
        key = entry["source_index"], entry["copy_index"], entry["source_family_id"]
        if key not in expected or key in seen:
            raise ValueError("composition has missing or duplicate source copies")
        seen.add(key)
        actual_order.append(key)
        original = originals[key[0], key[2]]
        prefix = f"source{key[0]}/copy{key[1]}/"
        if row["id"] != prefix + original["id"] or entry["family_id"] != row["id"]:
            raise ValueError("composition family identity differs")
        restored = deepcopy(row)
        restored["id"] = original["id"]

        def restore(requests):
            for request in requests:
                if request["type"] == "subagent":
                    if not request["agent_id"].startswith(prefix):
                        raise ValueError("composition session identity differs")
                    request["agent_id"] = request["agent_id"][len(prefix):]
                    restore(request["requests"])

        restore(restored["requests"])
        if restored != original:
            raise ValueError(f"composition changed source requests: {key}")
    if actual_order != expected_order:
        raise ValueError("composition family ordering differs")
    if totals(rows) != weighted_totals or manifest["included"] != weighted_totals:
        raise ValueError("composition source totals differ")
    return {"verified": True, "sources": len(source_specs), "source_copies": sum(s.copies for s in source_specs),
            "source_totals": weighted_totals, "ordering": "round-robin"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build")
    build.add_argument("--config", type=Path, required=True, help="resolved run config")
    check = commands.add_parser("verify")
    for name in ("input", "manifest", "report", "metrics"):
        check.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "build":
            config = json.loads(args.config.read_bytes())
            trace = config["trace"]
            sources = parse_sources(trace["sources"], args.config.parent)
            result = compose(sources, Path(trace["input"]), Path(trace["composition"]))
            print(json.dumps({"composed": True, "included": result["included"]}, indent=2))
        else:
            from research.semiclosed.verify_dag import verify
            composition = verify_composition(args.input, args.manifest)
            result = verify(args.input, args.report, args.metrics)
            if composition["source_totals"] != result["source_totals"]:
                raise ValueError("replay totals differ from composed source totals")
            result["composition"] = composition
            print(json.dumps(result, indent=2))
        return 0
    except (OSError, ValueError, KeyError, TypeError, subprocess.CalledProcessError) as error:
        print(f"DAG composition failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
