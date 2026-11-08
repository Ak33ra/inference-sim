#!/usr/bin/env python3
"""Create policy-comparison tables, figures, and a notebook from simulator run directories."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", action="append", required=True, metavar="LABEL=DIR",
                        help="named run directory; repeat for policy comparisons")
    parser.add_argument("--output", type=Path, required=True, help="fresh report directory")
    parser.add_argument("--baseline", help="run label used for paired family changes; defaults to first run")
    args = parser.parse_args(argv)
    paths = {}
    for item in args.run:
        label, separator, path = item.partition("=")
        if not separator or not label.strip() or not path:
            parser.error("--run expects LABEL=DIR")
        if label in paths:
            parser.error(f"duplicate run label: {label}")
        paths[label] = Path(path)
    os.environ.setdefault("MPLBACKEND", "Agg")
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    try:
        from research.analysis.report import write_report
        report = write_report(paths, args.output, args.baseline)
    except ImportError as error:
        parser.exit(2, f"Analysis dependency unavailable: {error}\n"
                    "Run with: uv run --with-requirements research/analysis/requirements.txt python "
                    "research/scripts/analyze_semiclosed.py ...\n")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"Analysis failed: {error}\n")
    print(f"Report: {args.output.absolute() / 'report.ipynb'}")
    for run in report["runs"]:
        print(f"{run['run']}: {run['completed_nonwarmup_rows']} completed non-warmup rows; "
              f"{run['complete_families']}/{run['families']} complete families")
        for notice in run["notices"]:
            print(f"  {notice}")
        if run["decomposition_reason"]:
            print(f"  TTFT split unavailable: {run['decomposition_reason']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
