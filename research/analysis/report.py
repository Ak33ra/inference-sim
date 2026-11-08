"""Export analysis tables, figures, and a rerunnable notebook from the same functions."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

import matplotlib.pyplot as plt
import nbformat
import pandas as pd

from .plots import make_figures
from .semiclosed import load_runs, paired_families, run_summary, ttft_statistics


REPOSITORY = Path(__file__).resolve().parents[2]
POPULATION = "Completed, non-warmup request rows; zero-output simulator TTFT retained. No explicit measured-window cutoff inferred."


def notebook(paths: dict[str, Path], baseline: str | None = None, images: dict[str, bytes] | None = None,
             summary_text: str | None = None, template=False):
    images = images or {}
    new = nbformat.v4
    repo_expression = ("next(p for p in [Path.cwd(), *Path.cwd().parents] "
                       "if (p / 'research/analysis/semiclosed.py').is_file())") if template else repr(str(REPOSITORY))
    run_expression = "{\n" + "".join(f"    {label!r}: REPOSITORY / {str(path)!r},\n"
                                       for label, path in paths.items()) + "}"
    cells = [new.new_markdown_cell(
        "# Semi-closed policy analysis\n\n"
        "Edit the named run directories below and run all cells. The first run is the default baseline. "
        "The shared analysis functions read outputs without changing them.\n\n"
        "Request plots cover completed, non-warmup rows. Family makespan includes recorded non-LLM delays "
        "and child dependencies. Incomplete families remain visible in coverage tables. "
        "Each run stays separate; these are descriptive statistics, not confidence intervals."),
        new.new_code_cell("from pathlib import Path\n\n"
                          f"REPOSITORY = Path({repo_expression})\n"
                          f"RUNS = {run_expression}\nBASELINE = {baseline or next(iter(paths))!r}"),
        new.new_code_cell(
            "import sys\nimport pandas as pd\nimport matplotlib.pyplot as plt\nfrom IPython.display import display\n"
            "if str(REPOSITORY) not in sys.path:\n    sys.path.insert(0, str(REPOSITORY))\n"
            "from research.analysis.semiclosed import load_runs, run_summary, ttft_statistics, paired_families\n"
            "from research.analysis.plots import plot_ttft, plot_request_latency, plot_lifetimes, plot_family_timeline, plot_paired, plot_server_series\n\n"
            "runs = load_runs(RUNS)\n"
            "summary = pd.json_normalize([run_summary(run) for run in runs])\n"
            "display(summary[['run', 'requests', 'completed_nonwarmup_rows', 'families', 'complete_families', "
            "'source_complete', 'accounting_match', 'decomposition_available', 'decomposition_reason', 'notices']])"),
        new.new_markdown_cell("## Explore normalized data\n\n"
            "These tables retain request identity, run labels, units, and eligibility. "
            "Use `requests`, `families`, `sessions`, and `summary` for additional plots. "
            "Raw engine E2E includes completion overhead; it is not the measured aggregate E2E definition."),
        new.new_code_cell("requests = pd.concat([run.requests for run in runs], ignore_index=True)\n"
                          "families = pd.concat([run.families for run in runs], ignore_index=True)\n"
                          "sessions = pd.concat([run.sessions for run in runs], ignore_index=True)\n"
                          "display(requests.head())\ndisplay(families.head())"),
        new.new_markdown_cell("## TTFT attribution\n\n"
            "Admission plus scheduling delay includes host preprocessing and waiting. Remaining TTFT includes "
            "prefill and other first-token work. Neither is an isolated compute phase. "
            "Means add directly. Quantile bars interpolate components from the same total-TTFT-ranked requests, "
            "so the segments sum to total p50/p95/p99. Segments are not component percentiles. "
            "A missing/nonzero preemption count or invalid timing split produces an unsplit bar. "
            "KV fetch time is not currently recorded per request."),
        new.new_code_cell("ttft = pd.concat([ttft_statistics(run) for run in runs], ignore_index=True)\ndisplay(ttft)"),
    ]
    if summary_text:
        cells.insert(1, new.new_markdown_cell(summary_text))

    def figure_cell(name, expression):
        cell = new.new_code_cell(f"figure = {expression}\n"
                                 "if figure is not None:\n    display(figure)\n    plt.close(figure)")
        if name in images:
            cell.outputs = [new.new_output("display_data", data={
                "image/png": base64.b64encode(images[name]).decode(), "text/plain": name})]
        cells.append(cell)

    figure_cell("ttft_statistics", "plot_ttft(runs)")
    figure_cell("request_latency_ecdf", "plot_request_latency(runs)")
    cells.append(new.new_markdown_cell("## Agent lifetimes and policy changes\n\n"
        "Family makespan spans root arrival to the last completion in the family. Session lifetime spans "
        "that session's first arrival to last completion. Sample counts matter, especially for tail statistics. "
        "Pairing requires identical source requests, tokens, timestamps, dependency graphs, and source/converter provenance. "
        "Negative makespan change means improvement. Read the coverage table before interpreting paired curves."))
    figure_cell("agent_lifetimes_ecdf", "plot_lifetimes(runs)")
    cells.append(new.new_code_cell("pairs, pair_coverage = paired_families(runs, BASELINE)\n"
                                   "display(pair_coverage)\ndisplay(pairs.head())"))
    figure_cell("paired_family_makespan", "plot_paired(runs, BASELINE)")
    figure_cell("family_progress", "plot_family_timeline(runs)")
    cells.append(new.new_markdown_cell("## Optional server time series\n\n"
        "The run must have recorded `server_series`. Set `run.series_interval_s` to a positive value "
        "in the semi-closed run config before simulation. Gauges below are time-weighted bin means. "
        "Missing time series cannot be recovered from aggregate metrics."))
    series_cell = new.new_code_cell("for run in runs:\n    figure = plot_server_series(run)\n"
                                    "    if figure is None:\n        print(f'{run.label}: server series unavailable')\n"
                                    "    else:\n        display(figure)\n        plt.close(figure)")
    series_cell.outputs = [new.new_output("display_data", data={
        "image/png": base64.b64encode(data).decode(), "text/plain": name})
        for name, data in images.items() if name.startswith("server_series_")]
    cells.append(series_cell)
    result = new.new_notebook(cells=cells, metadata={
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"}})
    nbformat.validate(result)
    return result


def write_report(paths: dict[str, str | Path], output: str | Path, baseline: str | None = None) -> dict:
    output = Path(output).expanduser().absolute()
    if os.path.lexists(output):
        raise ValueError(f"Output already exists; choose a fresh directory: {output}")
    runs = load_runs(paths)
    pairs, coverage = paired_families(runs, baseline)
    summaries = [run_summary(run) for run in runs]
    source_files = {}
    for run in runs:
        source_files[run.label] = {name: hashlib.sha256((run.path / name).read_bytes()).hexdigest()
                                  for name in ("metrics.json", "workload.json", "config.resolved.json", "verification.json")
                                  if (run.path / name).is_file()}
    report = {"format_version": 1, "population": POPULATION, "percentile_method": "numpy_linear",
              "baseline": baseline or runs[0].label, "runs": summaries,
              "pairing_coverage": coverage.to_dict(orient="records"), "source_sha256": source_files}
    figures = make_figures(runs, baseline)
    output.mkdir(parents=True)
    images = {}
    try:
        (output / "summary.json").write_text(json.dumps(report, indent=2, allow_nan=False) + "\n")
        tables = {"requests": pd.concat([run.requests for run in runs], ignore_index=True),
                  "families": pd.concat([run.families for run in runs], ignore_index=True),
                  "sessions": pd.concat([run.sessions for run in runs], ignore_index=True),
                  "ttft_statistics": pd.concat([ttft_statistics(run) for run in runs], ignore_index=True),
                  "paired_families": pairs, "pairing_coverage": coverage}
        for name, table in tables.items():
            table.to_csv(output / f"{name}.csv", index=False)
        for name, figure in figures.items():
            figure.savefig(output / f"{name}.png", dpi=160, bbox_inches="tight")
            figure.savefig(output / f"{name}.pdf", bbox_inches="tight")
            images[name] = (output / f"{name}.png").read_bytes()
        observations = "## Saved report coverage\n\n" + "\n".join(
            f"- {s['run']}: {s['completed_nonwarmup_rows']}/{s['requests']} completed non-warmup request rows; "
            f"{s['complete_families']}/{s['families']} complete families."
            + (" " + s["decomposition_reason"] if s["decomposition_reason"] else "")
            + (" " + " ".join(s["notices"]) if s["notices"] else "") for s in summaries)
        nbformat.write(notebook({run.label: run.path for run in runs}, baseline, images, observations),
                       output / "report.ipynb")
        return report
    finally:
        for figure in figures.values():
            plt.close(figure)
