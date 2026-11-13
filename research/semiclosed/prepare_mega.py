"""Build and verify a fixed, shuffled mixture of complete Weka DAG families."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess

from research.semiclosed.mix import Source, compose, verify_composition


ALGORITHM = "sha256-family-key-v1"


def permutation(families: list[dict], seed: int) -> list[int]:
    def rank(index):
        family = families[index]
        key = [ALGORITHM, seed, family["source_index"], family["copy_index"], family["source_family_id"]]
        return hashlib.sha256(json.dumps(key, separators=(",", ":")).encode()).digest(), index
    return sorted(range(len(families)), key=rank)


def verify(directory: Path) -> dict:
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest["version"] != 1 or manifest["algorithm"] != ALGORITHM:
        raise ValueError("unsupported mega trace manifest")
    base = json.loads((directory / "composition.json").read_text())
    composition = verify_composition(directory / "round-robin.jsonl", directory / "composition.json")
    if any(source["copies"] != 1 for source in base["sources"]):
        raise ValueError("mega trace requires one copy of each source")
    order = permutation(base["families"], manifest["seed"])
    lines = (directory / "round-robin.jsonl").read_bytes().splitlines(keepends=True)
    expected = b"".join(lines[i] for i in order)
    actual = subprocess.run(["zstd", "-dc", str(directory / "input.jsonl.zst")],
                            capture_output=True, check=True).stdout
    expected_ids = [base["families"][i]["family_id"] for i in order]
    if actual != expected or manifest["family_order"] != expected_ids:
        raise ValueError("mega trace differs from the seeded permutation of source families")
    digest = hashlib.sha256(actual).hexdigest()
    if digest != manifest["sha256"] or manifest["source_totals"] != composition["source_totals"]:
        raise ValueError("mega trace digest or totals differ")
    return {**composition, "ordering": "seeded-shuffle", "input_sha256": digest,
            "order_seed": manifest["seed"], "algorithm": ALGORITHM}


def build(paths: list[Path], directory: Path, seed: int) -> dict:
    if len(set(path.resolve() for path in paths)) != len(paths):
        raise ValueError("source paths must be distinct")
    if not paths or not -(2**63) <= seed < 2**63:
        raise ValueError("provide sources and a signed 64-bit order seed")
    directory.mkdir(parents=True, exist_ok=False)
    base = compose(tuple(Source(path.resolve()) for path in paths),
                   directory / "round-robin.jsonl", directory / "composition.json")
    verify_composition(directory / "round-robin.jsonl", directory / "composition.json")
    order = permutation(base["families"], seed)
    lines = (directory / "round-robin.jsonl").read_bytes().splitlines(keepends=True)
    data = b"".join(lines[i] for i in order)
    with (directory / "input.jsonl.zst").open("xb") as handle:
        subprocess.run(["zstd", "-q", "-T1", "-3", "-c"], input=data, stdout=handle, check=True)
    manifest = {"version": 1, "algorithm": ALGORITHM, "seed": seed,
                "sha256": hashlib.sha256(data).hexdigest(), "digest_encoding": "uncompressed",
                "source_totals": base["included"],
                "family_order": [base["families"][i]["family_id"] for i in order],
                "composition": "composition.json", "input": "input.jsonl.zst"}
    (directory / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    result = verify(directory)
    (directory / "verification.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    make = sub.add_parser("build")
    make.add_argument("--source", type=Path, action="append", required=True)
    make.add_argument("--output", type=Path, required=True)
    make.add_argument("--seed", type=int, default=42)
    check = sub.add_parser("verify")
    check.add_argument("directory", type=Path)
    args = parser.parse_args()
    result = build(args.source, args.output, args.seed) if args.command == "build" else verify(args.directory)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
