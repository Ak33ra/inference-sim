"""Plot the saved per-condition statistics without re-reading simulator outputs."""

import argparse
import csv
import json
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from .run import CONDITIONS


def label(router: str, scheduler: str) -> str:
    names = {"llmd-jsq": "llm-d JSQ", "jsq": "JSQ", "least-token": "Least token",
             "random": "Random", "agentix": "Agentix affinity", "preble-e2": "Preble E2 cost"}
    return f"{names[router]} / {scheduler.upper()}"


def plot(study: Path) -> Path:
    analysis = study / "analysis"
    summary = json.loads((analysis / "summary.json").read_text())
    if not summary["complete"]:
        raise ValueError("plots require a complete verified analysis")
    with (analysis / "aggregate.csv").open() as source:
        rows = [row for row in csv.DictReader(source) if row["kind"] == "loaded"]
    values = {(row["prefix_caching"] == "True", row["router"], row["scheduler"],
               int(row["mean_interarrival_s"]), row["metric"], row["statistic"]): row for row in rows}
    output = analysis / "plots"
    output.mkdir(exist_ok=True)
    for name, metric, unit in (
        ("family-makespan", "family_makespan_s", "seconds"),
        ("family-slowdown", "family_isolated_slowdown", "best-observed isolated ratio"),
        ("fork-makespan", "fork_multi_child_makespan_s", "seconds"),
        ("fork-slowdown", "fork_multi_child_isolated_slowdown", "best-observed isolated ratio"),
    ):
        figure, axes = plt.subplots(2, 3, figsize=(17, 10), layout="constrained")
        for index, cache in enumerate((False, True)):
            conditions = CONDITIONS[cache]
            positions = np.arange(len(conditions))
            for column, stat in enumerate(("mean", "p50", "p99")):
                ax = axes[index, column]
                for gap, offset, color in ((4, -.18, "#356cb6"), (8, .18, "#de8c37")):
                    cells = [values[cache, router, scheduler, gap, metric, stat] for router, scheduler in conditions]
                    mid = np.array([float(cell["median"]) for cell in cells])
                    low = np.array([float(cell["min"]) for cell in cells])
                    high = np.array([float(cell["max"]) for cell in cells])
                    ax.barh(positions + offset, mid, height=.32, color=color, label=f"{gap}s arrivals",
                            xerr=np.array([mid - low, high - mid]), capsize=2, error_kw={"linewidth": .8})
                ax.set_yticks(positions, [label(*condition) for condition in conditions] if column == 0 else [])
                ax.invert_yaxis()
                ax.set_xlabel(unit)
                ax.set_title(f"Cache {'on' if cache else 'off'} · {'median' if stat == 'p50' else stat}")
                ax.spines[["top", "right"]].set_visible(False)
                ax.grid(axis="x", alpha=.2)
                ax.set_axisbelow(True)
        axes[0, 0].legend(loc="lower right", frameon=False)
        figure.suptitle(f"{'Family' if name.startswith('family') else 'Multi-child fork'} {'slowdown' if 'slowdown' in name else 'makespan'}\n"
                       "Median of five per-run statistics; whiskers show the seed range", fontsize=14)
        for extension in ("png", "pdf"):
            figure.savefig(output / f"{name}.{extension}", dpi=160)
        plt.close(figure)
    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", type=Path)
    args = parser.parse_args()
    print(plot(args.study.resolve()))
