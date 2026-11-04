from __future__ import annotations

import argparse
from pathlib import Path
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Convert complete FreeInference families to Weka JSONL.")
    parser.add_argument("--source", type=Path, help="release directory (default: repository traces path)")
    parser.add_argument("--week", action="append", required=True, help="weekly directory name; repeatable")
    parser.add_argument("--model", required=True, help="exact model label; mixed-model families are excluded")
    parser.add_argument("--output", type=Path, required=True, help="new output directory")
    parser.add_argument("--start-ms", type=int, help="inclusive root-start Unix timestamp in milliseconds")
    parser.add_argument("--end-ms", type=int, help="exclusive root-start Unix timestamp in milliseconds")
    parser.add_argument("--unsupported", choices=("exclude", "error"), default="exclude")
    parser.add_argument("--source-revision", help="declared dataset revision (defaults to pinned research release)")
    parser.add_argument("--batch-size", type=int, default=64, help="Parquet prefix rows per batch (default: 64)")
    parser.add_argument("--temp-dir", type=Path, help="scratch directory for the temporary prefix index")
    args = parser.parse_args(argv)
    try:
        from .model import TraceError
        from .pipeline import DEFAULT_REVISION, DEFAULT_SOURCE, Selection, convert_release
    except ModuleNotFoundError as error:
        if error.name != "pyarrow":
            raise
        parser.exit(2, "pyarrow is required; install research/freeinference_trace/requirements.txt\n")
    try:
        manifest = convert_release(
            args.source or DEFAULT_SOURCE, args.output,
            Selection(tuple(args.week), args.model, args.start_ms, args.end_ms, args.unsupported),
            revision=args.source_revision or DEFAULT_REVISION,
            batch_size=args.batch_size, temp_dir=args.temp_dir)
    except (TraceError, OSError, ValueError) as error:
        print(f"conversion failed: {error}", file=sys.stderr)
        return 2
    included = manifest["included"]
    print(f"Wrote {included['families']} families / {included['requests']} requests to {args.output}")
    print(f"Corpus SHA256: {manifest['artifacts']['corpus.jsonl']['sha256']}")
    print(f"Excluded families: {manifest['excluded_families_by_reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
