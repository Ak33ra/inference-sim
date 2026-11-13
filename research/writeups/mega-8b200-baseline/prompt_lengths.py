"""Report the per-request input-token distribution of the canonical mega trace."""

import collections
import csv
import hashlib
import json
from pathlib import Path
import statistics
import subprocess

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[3]
TRACE = ROOT / "traces/mega-commit0-oolong"
OUTPUT = ROOT / "research/outputs/analysis/mega-trace-prompt-lengths"


def requests(family, entries, session):
    for request in entries:
        if request["type"] == "subagent":
            yield from requests(family, request["requests"], request["agent_id"])
        else:
            yield {"family": family, "session": session, "req_id": request["req_id"],
                   "prompt_tokens": request["in"]}


def summarize(values):
    q = statistics.quantiles(values, n=100, method="inclusive")
    return {"count": len(values), "total_tokens": sum(values), "mean": statistics.mean(values),
            "min": min(values), "p10": q[9], "p25": q[24], "p50": q[49], "p75": q[74],
            "p90": q[89], "p95": q[94], "p99": q[98], "max": max(values)}


def main():
    data = subprocess.check_output(["zstd", "-dc", str(TRACE / "input.jsonl.zst")])
    manifest = json.loads((TRACE / "manifest.json").read_text())
    assert hashlib.sha256(data).hexdigest() == manifest["sha256"]
    rows = [r for f in map(json.loads, data.splitlines())
            for r in requests(f["id"], f["requests"], f["id"])]
    values = [r["prompt_tokens"] for r in rows]
    assert len(values) == manifest["source_totals"]["requests"]
    assert sum(values) == manifest["source_totals"]["prompt_tokens"]
    assert len({(r["family"], r["req_id"]) for r in rows}) == len(rows)
    edges = [0, 1024, 2048, 4096, 8192, 16384, 32768, 65536, 131072]
    assert min(values) >= edges[0] and max(values) < edges[-1]
    histogram = []
    for a, b in zip(edges, edges[1:]):
        count = sum(a <= v < b for v in values)
        histogram.append({"min_tokens": a, "max_tokens": b - 1,
                          "count": count, "percent": count * 100 / len(values)})
    assert sum(r["count"] for r in histogram) == len(rows)
    by_source = collections.defaultdict(list)
    for row in rows:
        by_source[row["family"].split("/")[0]].append(row["prompt_tokens"])
    result = {"definition": "each leaf request's in field, equally weighted; includes root and child sessions",
              "input_sha256": manifest["sha256"], "summary": summarize(values), "histogram": histogram,
              "by_source": {source: summarize(v) for source, v in sorted(by_source.items())}}
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    for name, records in (("requests.csv", rows), ("histogram.csv", histogram)):
        with (OUTPUT / name).open("w") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(records[0]))
            writer.writeheader()
            writer.writerows(records)

    fig, axes = plt.subplots(1, 2, figsize=(11, 4), layout="constrained")
    labels = [f"{r['min_tokens']:,}–{r['max_tokens']:,}" for r in histogram]
    bars = axes[0].barh(labels, [r["percent"] for r in histogram], color="#466b91")
    axes[0].bar_label(bars, labels=[f"{r['percent']:.2f}%" for r in histogram], padding=4, fontsize=9)
    axes[0].invert_yaxis()
    axes[0].set(xlabel="Requests (%)", ylabel="Input tokens per request", xlim=(0, 36))
    ordered = sorted(values)
    axes[1].step(ordered, [100 * (i + 1) / len(ordered) for i in range(len(ordered))], where="post")
    for percentile in (50, 90, 99):
        value = result["summary"][f"p{percentile}"]
        axes[1].scatter([value], [percentile], s=20)
        axes[1].annotate(f"p{percentile}: {value:,.0f}", (value, percentile),
                         xytext=(7, -18 if percentile > 50 else -12), textcoords="offset points", fontsize=9)
    axes[1].set(xlabel="Input tokens per request", ylabel="Requests at or below length (%)", ylim=(0, 105))
    axes[1].ticklabel_format(style="plain", axis="x")
    axes[1].grid(alpha=.2)
    fig.suptitle("Mega trace · 58,741 requests · Total prompt length")
    fig.savefig(OUTPUT / "prompt-lengths.png", dpi=170)
    plt.close(fig)
    print(json.dumps(result["summary"], indent=2))


if __name__ == "__main__":
    main()
