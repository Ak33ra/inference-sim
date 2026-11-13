"""Plot arrival calibration and the seed-42 baseline from saved run summaries."""

import collections
import json
import math
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from analyze import occupied


ROOT = Path(__file__).resolve().parents[3]
OUTPUT = ROOT / "research/outputs/analysis/mega-8b200-baseline"


def main():
    pilots = json.loads((OUTPUT / "pilot-summary.json").read_text())
    repeats = json.loads((OUTPUT / "baseline-summary.json").read_text())
    fig, axes = plt.subplots(1, 2, figsize=(10, 3.6), layout="constrained")
    rates = [60 * run["arrivals"]["rate_per_s"] for run in pilots]
    axes[0].axhspan(65, 75, color="#d9ead3", label="Target activity")
    axes[0].plot(rates, [100 * r["request_active_fraction"] for r in pilots], "o-", label="Pilot, seed 42")
    axes[0].scatter([5] * len(repeats), [100 * r["request_active_fraction"] for r in repeats],
                    marker="x", color="#b94e1d", label="Baseline, seeds 42–46", zorder=5)
    axes[0].set(ylabel="Replica request-active time (%)", ylim=(0, 105))
    axes[0].legend(fontsize=8, loc="lower right")
    axes[1].plot(rates, [r["family_makespan_s"]["p50"] for r in pilots], "o-", label="Family median")
    axes[1].plot(rates, [r["family_makespan_s"]["p90"] for r in pilots], "s-", label="Family p90")
    axes[1].set(ylabel="Family makespan (s)", ylim=(0, None))
    axes[1].legend(fontsize=8)
    for ax in axes:
        ax.set(xlabel="Mean root arrivals per minute")
        ax.grid(axis="y", alpha=.2)
    fig.suptitle("Eight B200 TP1 replicas · GPT-OSS-120B · JSQ · prefix caching off")
    fig.savefig(OUTPUT / "calibration.png", dpi=170)
    plt.close(fig)

    run = next(r for r in repeats if r["arrivals"]["seed"] == 42)
    path = ROOT / run["output"]
    metrics = json.loads((path / "metrics.json").read_text())
    workload = json.loads((path / "workload.json").read_text())
    intervals = collections.defaultdict(list)
    for request in metrics["requests"]:
        intervals[request["handled_by"]].append((
            request["arrived_at"] + request["scheduling_delay_ms"] / 1000,
            request["arrived_at"] + request["e2e_ms"] / 1000))
    end = run["drained_at_s"]
    edges = list(range(0, math.ceil(end), 60)) + [end]
    centers = [(a + b) / 120 for a, b in zip(edges, edges[1:])]
    active = [100 * sum(occupied(v, a, b) for v in intervals.values()) / (8 * (b - a))
              for a, b in zip(edges, edges[1:])]
    births = [f["birth_us"] / 1e6 for f in workload["families"]]
    fig, axes = plt.subplots(2, 1, figsize=(10, 5), sharex=True, layout="constrained")
    axes[0].hist([b / 60 for b in births], bins=[b / 60 for b in edges], color="#466b91")
    axes[0].set(ylabel="Roots arriving per minute", title="Baseline seed 42: Poisson arrivals, 12s mean gap")
    axes[1].plot(centers, active, color="#466b91")
    axes[1].set(ylabel="Request-active time (%)", xlabel="Simulated time (minutes)", ylim=(0, 105))
    a, b = run["measurement_window_s"]
    for ax in axes:
        ax.axvspan(a / 60, b / 60, alpha=.12, color="green", label="Load measurement window")
        ax.axvline(run["last_root_arrival_s"] / 60, color="#b94e1d", linestyle="--", label="Last root arrival")
        ax.grid(axis="y", alpha=.2)
    axes[0].legend(fontsize=8, loc="upper right")
    fig.savefig(OUTPUT / "baseline-timeline.png", dpi=170)
    plt.close(fig)


if __name__ == "__main__":
    main()
