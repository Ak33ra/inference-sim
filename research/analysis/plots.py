"""Figures shared by the batch report and the exploratory notebook."""

from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import numpy as np
import pandas as pd

from .semiclosed import RunData, paired_families, ttft_statistics, values


def ecdf(ax, data, label):
    data = np.asarray(data, dtype=float)
    data = np.sort(data[np.isfinite(data) & (data >= 0)])
    if not len(data):
        return
    ax.step(np.r_[data[0], data], np.r_[0, np.arange(1, len(data) + 1) / len(data)],
            where="post", label=f"{label} (n={len(data):,})")
    ax.set_ylim(0, 1.02)
    ax.set_ylabel("Completed fraction")
    ax.grid(alpha=.2)


def finish_axis(ax):
    if ax.lines:
        ax.legend(fontsize=8)
    else:
        ax.text(.5, .5, "No eligible observations", transform=ax.transAxes, ha="center")


def plot_ttft(runs: list[RunData]):
    figure, axes = plt.subplots(1, 4, figsize=(max(12, len(runs) * 2.4), 4.8), sharey=True)
    for ax, statistic in zip(axes, ("mean", "p50", "p95", "p99")):
        for position, run in enumerate(runs):
            row = ttft_statistics(run).set_index("statistic").loc[statistic]
            if row["count"] == 0:
                ax.text(position, 0, "n=0", ha="center", va="bottom", fontsize=8)
            elif row["decomposed"]:
                ax.bar(position, row["scheduling_ms"], color="#4477AA")
                ax.bar(position, row["remaining_ms"], bottom=row["scheduling_ms"], color="#EEAA33")
            else:
                ax.bar(position, row["total_ms"], color="#999999", hatch="//")
        ax.set_xticks(range(len(runs)), [f"{r.label}\nn={len(values(r, 'ttft_ms')):,}" for r in runs], rotation=25, ha="right")
        ax.set_title("Median (p50)" if statistic == "p50" else statistic.capitalize())
        ax.grid(axis="y", alpha=.2)
    axes[0].set_ylabel("Simulator TTFT (ms)")
    figure.suptitle("TTFT statistics | completed, non-warmup requests")
    figure.legend(handles=[Patch(color="#4477AA", label="Admission + scheduling delay"),
                           Patch(color="#EEAA33", label="Remaining time to first token"),
                           Patch(facecolor="#999999", hatch="//", label="Total only; split unavailable")],
                  loc="lower center", ncol=3, fontsize=9, bbox_to_anchor=(.5, .075))
    figure.text(.5, .02, "Quantile components use the same TTFT-ranked requests and interpolation weights.\n"
                "They are not separate component quantiles. Neither segment measures pure queueing or prefill compute.",
                ha="center", fontsize=9)
    figure.tight_layout(rect=(0, .19, 1, .95))
    return figure


def plot_request_latency(runs: list[RunData]):
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for ax, metric, title in zip(axes, ("ttft_ms", "engine_e2e_ms"),
                                  ("Simulator TTFT", "Raw engine E2E, including host completion")):
        for run in runs:
            ecdf(ax, values(run, metric), run.label)
        ax.set_xlabel(f"{title} (ms)")
        finish_axis(ax)
    figure.suptitle("Request latency distributions | completed, non-warmup requests")
    figure.tight_layout()
    return figure


def plot_lifetimes(runs: list[RunData]):
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for run in runs:
        ecdf(axes[0], run.families["makespan_s"], run.label)
        ecdf(axes[1], run.sessions["lifetime_s"], run.label)
    axes[0].set_xlabel("Complete family makespan (s)")
    axes[1].set_xlabel("Complete session lifetime (s)")
    for ax in axes:
        finish_axis(ax)
    figure.suptitle("Agent lifetimes | includes recorded non-LLM delays and dependency waits")
    figure.tight_layout()
    return figure


def plot_paired(runs: list[RunData], baseline: str | None):
    pairs, _ = paired_families(runs, baseline)
    if pairs.empty:
        return None
    figure, ax = plt.subplots(figsize=(9, 4.5))
    for label, group in pairs.groupby("run", sort=False):
        data = np.sort(group["change_percent"].dropna().to_numpy())
        if len(data):
            ax.step(np.r_[data[0], data], np.r_[0, np.arange(1, len(data) + 1) / len(data)],
                    where="post", label=f"{label} (n={len(data):,})")
    ax.axvline(0, color="black", lw=.8, linestyle="--")
    ax.set(xlabel="Family makespan change vs baseline (%) | negative is faster", ylabel="Matched-family fraction",
           title=f"Paired makespan | baseline: {baseline or runs[0].label}", ylim=(0, 1.02))
    ax.grid(alpha=.2)
    ax.legend(fontsize=9)
    figure.tight_layout()
    return figure


def plot_family_timeline(runs: list[RunData]):
    figure, axes = plt.subplots(1, 2, figsize=(11, 4.5))
    for run in runs:
        families = run.families
        arrivals = families["arrival_s"].dropna().to_numpy(dtype=float)
        ends = families.loc[families["complete"].eq(True), "completion_s"].dropna().to_numpy(dtype=float)
        if not len(arrivals):
            continue
        events = pd.Series(np.r_[np.ones(len(arrivals)), -np.ones(len(ends))], index=np.r_[arrivals, ends])
        active = events.groupby(level=0).sum().sort_index().cumsum()
        horizon = run.workload.get("end_us", 0) / 1e6
        times = np.r_[0, active.index.to_numpy(), max(horizon, active.index[-1])]
        counts = np.r_[0, active.to_numpy(), active.iloc[-1]]
        axes[0].step(times, counts, where="post", label=run.label)
        end_times = np.sort(ends)
        axes[1].step(np.r_[0, end_times, max(horizon, end_times[-1] if len(ends) else 0)],
                     np.r_[0, np.arange(1, len(ends) + 1), len(ends)], where="post", label=run.label)
    axes[0].set_ylabel("Arrived families not yet fully completed")
    axes[1].set_ylabel("Cumulative completed families")
    for ax in axes:
        ax.set_xlabel("Simulation time from selected trace origin (s)")
        ax.grid(alpha=.2)
        finish_axis(ax)
    figure.suptitle("Family progress | failed/incomplete families remain in the unfinished count")
    figure.tight_layout()
    return figure


def plot_server_series(run: RunData):
    series = run.metrics.get("server_series")
    if not series or not series.get("engines"):
        return None
    figure, axes = plt.subplots(1, 3, figsize=(13, 4))
    interval = float(series["interval_s"])
    for engine in series["engines"]:
        for ax, key in zip(axes, ("num_requests_waiting", "num_requests_running", "kv_cache_usage")):
            data = engine[key]
            if len(data):
                ax.step(np.arange(len(data) + 1) * interval, np.r_[data, data[-1]],
                        where="post", label=engine["id"])
    for ax, ylabel in zip(axes, ("Waiting requests", "Running requests", "KV occupancy fraction")):
        ax.set(xlabel="Simulation time (s)", ylabel=ylabel)
        ax.grid(alpha=.2)
        finish_axis(ax)
    figure.suptitle(f"{run.label} | per-engine time-weighted bin means, interval {interval:g} s")
    figure.tight_layout()
    return figure


def make_figures(runs: list[RunData], baseline: str | None = None) -> dict:
    figures = {"ttft_statistics": plot_ttft(runs), "request_latency_ecdf": plot_request_latency(runs)}
    if any(len(run.families) or len(run.sessions) for run in runs):
        figures["agent_lifetimes_ecdf"] = plot_lifetimes(runs)
        figures["family_progress"] = plot_family_timeline(runs)
    paired = plot_paired(runs, baseline)
    if paired is not None:
        figures["paired_family_makespan"] = paired
    for index, run in enumerate(runs, 1):
        figure = plot_server_series(run)
        if figure is not None:
            figures[f"server_series_{index:02d}"] = figure
    return figures
