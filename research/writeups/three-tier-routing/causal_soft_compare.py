#!/usr/bin/env python3
"""Soft-reservation ablation for the selected causal observed-work tiers.

Thresholds and completed-output weight are fixed by causal_tier_compare.py.  This
script tunes only the affinity bonus on primary-rate seed 42, then evaluates the
selected bonus on seeds 43-46 and all five doubled-rate seeds.  P2 and P3 prefer
their six- and four-replica homes, but may borrow any replica when JSQ's load
advantage exceeds the affinity bonus.
"""
import argparse
import concurrent.futures as cf
import csv
import importlib.util
import json
import math
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import sys

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[2]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


hard = load_module("causal_tier_compare", HERE / "causal_tier_compare.py")
tr = hard.tr
AFFINITY_WEIGHTS = (0.1, 0.25, 0.5, 1.0)
SOFT_ROUTER = REPO / "research/configs/routers/jsq-3tier-soft.yaml"
SOFT_LABELS = {}
for index in range(2, 8):
    SOFT_LABELS.setdefault(f"instance_{index}", {})["research.blis/soft-p2"] = "P2"
for index in range(4, 8):
    SOFT_LABELS.setdefault(f"instance_{index}", {})["research.blis/soft-p3"] = "P3"
P2_HOME = {f"instance_{index}" for index in range(2, 8)}
P3_HOME = {f"instance_{index}" for index in range(4, 8)}


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def weight_slug(weight):
    return str(weight).replace(".", "p")


def router_config(work, weight):
    text = SOFT_ROUTER.read_text()
    marker = "weight: 1 # SOFT_AFFINITY_WEIGHT"
    assert text.count(marker) == 2
    path = work / f"jsq-3tier-soft-w{weight_slug(weight)}.yaml"
    resolved = text.replace(marker, f"weight: {weight} # SOFT_AFFINITY_WEIGHT")
    if path.exists():
        assert path.read_text() == resolved
    else:
        path.write_text(resolved)
    return path


def run_one(work, path, seed, rate, causal_config, affinity):
    rate_name = "1x" if rate == hard.PRIMARY_RATE else "2x"
    name = f"causal-soft-a{weight_slug(affinity)}-{rate_name}-s{seed}"
    out = work / "runs" / name
    cached = False
    try:
        report = json.loads((out / "workload.json").read_text())
        cached = report["complete"] and report["accounting_match"] and (out / "metrics.json").is_file()
    except (OSError, KeyError, json.JSONDecodeError):
        pass
    if not cached:
        completed_weight, first, second = causal_config
        scenario = tr.oi.template()
        scenario["trace"] = {"format": "weka-dag", "path": str(path)}
        scenario["serving"]["router"]["endpoint_picker_config"] = str(router_config(work, affinity))
        scenario["serving"]["router"]["epp_flags"] = tr.EPP_FLAGS
        scenario["serving"]["cluster"]["replicas"] = tr.GPUS
        assert scenario["serving"]["engine"]["enable_prefix_caching"] is False
        scenario["run"]["seed"] = seed
        scenario["run"]["arrivals"] = {"mode": "poisson", "rate_per_s": rate, "seed": seed}
        scenario["run"]["causal_tiers"] = {
            "thresholds": [first, second], "completed_output_weight": completed_weight}
        env = {key: value for key, value in os.environ.items()
               if key not in ("BLIS_RESEARCH_INSTANCE_LABELS", "BLIS_RESEARCH_ALLOW_HEADER_PROFILE")}
        env |= {"BLIS_RESEARCH_INSTANCE_LABELS": json.dumps(SOFT_LABELS),
                "BLIS_RESEARCH_ALLOW_HEADER_PROFILE": "1"}
        shutil.rmtree(out, ignore_errors=True)
        out.parent.mkdir(parents=True, exist_ok=True)
        config_path = out.parent / f"{out.name}.config.json"
        config_path.write_text(json.dumps(dict(scenario, output=str(out)), indent=1))
        log(f"run {name} started")
        with (out.parent / f"{out.name}.log").open("w") as handle:
            subprocess.run([str(tr.oi.RUNNER), "run", str(config_path)], check=True,
                           stdout=handle, stderr=handle, env=env)
        (out / "blis").unlink(missing_ok=True)
        log(f"run {name} done")
    return out


def stats(values):
    return tr.stats(values)


def analyse_run(out, seed, rate, causal_config, affinity, selected=False):
    workload = json.loads((out / "workload.json").read_text())
    metrics = json.loads((out / "metrics.json").read_text())
    completed_weight, first, second = causal_config
    assert workload["complete"] and workload["accounting_match"] and workload["states"]["failed"] == 0
    assert workload["causal_tiers"] == {
        "thresholds": [first, second], "completed_output_weight": completed_weight}
    requests = {row["request_id"]: row for row in workload["requests"]}
    class_counts = {name: 0 for name in ("P1", "P2", "P3")}
    audit = {"bad_classes": 0, "bad_charges": 0, "bad_final_family_work": 0,
             "metric_class_mismatches": 0}
    expected_final = {}
    for row in requests.values():
        after = row.get("observed_work_after_admission", 0)
        expected = "P1" if after <= first else "P2" if after <= second else "P3"
        audit["bad_classes"] += row.get("routing_class") != expected
        audit["bad_charges"] += after - row.get("observed_work_before", 0) != row["prompt_tokens"]
        audit["bad_charges"] += row.get("completed_output_charge", 0) != row["output_tokens"] * completed_weight
        class_counts[row["routing_class"]] += 1
        expected_final[row["trace_id"]] = expected_final.get(row["trace_id"], 0) + \
            row["prompt_tokens"] + row["output_tokens"] * completed_weight
    audit["bad_final_family_work"] = sum(
        row.get("observed_work", 0) != expected_final[row["trace_id"]] for row in workload["families"])

    overflow = {"P2": 0, "P3": 0}
    routed = {name: 0 for name in ("P1", "P2", "P3")}
    for row in metrics["requests"]:
        request_class = requests[row["requestID"]]["routing_class"]
        audit["metric_class_mismatches"] += row.get("slo_class") != request_class
        routed[request_class] += 1
        overflow["P2"] += request_class == "P2" and row["handled_by"] not in P2_HOME
        overflow["P3"] += request_class == "P3" and row["handled_by"] not in P3_HOME

    families = sorted(workload["families"], key=lambda row: (row["birth_us"], row["trace_id"]))
    cut = math.floor(tr.TRIM * len(families))
    oracle = tr.classes()
    rows = [{"rate_per_s": rate, "seed": seed, "affinity_weight": affinity, "selected": selected,
             "family": row["trace_id"], "oracle_class": oracle[row["trace_id"]],
             "kept": cut <= index < len(families) - cut, "arrival_s": row["birth_us"] / 1e6,
             "e2e_s": row["makespan_us"] / 1e6, "optimal_s": tr.OPT[row["trace_id"]],
             "slowdown": row["makespan_us"] / 1e6 / tr.OPT[row["trace_id"]]}
            for index, row in enumerate(families)]
    kept = [row["slowdown"] for row in rows if row["kept"]]
    return rows, {"mean": statistics.fmean(kept), "stats": stats(kept),
                  "class_counts": class_counts, "routed": routed, "overflow": overflow,
                  "audit": audit}


def aggregate(run_results, rows, rate, affinity, causal_config):
    selected = [row for row in rows if row["selected"] and row["rate_per_s"] == rate
                and row["affinity_weight"] == affinity]
    kept = [row for row in selected if row["kept"]]
    return {"causal_config": {"completed_output_weight": causal_config[0],
                              "thresholds": [causal_config[1], causal_config[2]]},
            "affinity_weight": affinity, "rate_per_s": rate,
            "pooled": stats(row["slowdown"] for row in kept),
            "by_oracle_class": {name: stats(row["slowdown"] for row in kept
                                            if row["oracle_class"] == name)
                                for name in ("P1", "P2", "P3")},
            "per_seed": {str(seed): run_results[rate, affinity, seed] for seed in tr.SEEDS}}


def percent(value, baseline):
    return 100 * (value / baseline - 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path,
                        default=REPO / "research/outputs/three-tier-routing-causal-soft")
    parser.add_argument("--hard-results", type=Path,
                        default=REPO / "research/outputs/three-tier-routing-causal/results.json")
    parser.add_argument("--jobs", type=int, default=6)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    for variable in ("BLIS_CATALOG", "BLIS_REGISTRY", "GOWORK"):
        if not os.environ.get(variable):
            sys.exit(f"{variable} is not set; run through causal_soft_compare.sh")
    hard_results = json.loads(args.hard_results.resolve().read_text())
    selected = hard_results["selected"]
    causal_config = (selected["completed_output_weight"], *selected["thresholds"])

    out = args.out.resolve()
    work = out / "work"
    work.mkdir(parents=True, exist_ok=True)
    lines = tr.load_families(work)
    paths = {seed: tr.trace(work, lines, seed, False) for seed in tr.SEEDS}
    # Materialize each immutable router config before worker threads can read it. Evaluation
    # reuses the selected file from this seed-42 sweep.
    for weight in AFFINITY_WEIGHTS:
        router_config(work, weight)

    tune_jobs = [(paths[42], 42, hard.PRIMARY_RATE, causal_config, weight)
                 for weight in AFFINITY_WEIGHTS]
    with cf.ThreadPoolExecutor(args.jobs) as executor:
        tune_outs = list(executor.map(lambda job: run_one(work, *job), tune_jobs))
    results, rows, tuning = {}, [], []
    for weight, run_out in zip(AFFINITY_WEIGHTS, tune_outs):
        family, result = analyse_run(run_out, 42, hard.PRIMARY_RATE, causal_config, weight)
        rows += family
        results[hard.PRIMARY_RATE, weight, 42] = result
        tuning.append({"affinity_weight": weight, **result})
    affinity = min(AFFINITY_WEIGHTS,
                   key=lambda weight: (results[hard.PRIMARY_RATE, weight, 42]["mean"], weight))
    log(f"selected soft affinity on seed 42: {affinity}")

    jobs = ([(paths[seed], seed, hard.PRIMARY_RATE, causal_config, affinity)
             for seed in tr.SEEDS[1:]] +
            [(paths[seed], seed, hard.DOUBLE_RATE, causal_config, affinity)
             for seed in tr.SEEDS])
    with cf.ThreadPoolExecutor(args.jobs) as executor:
        outputs = list(executor.map(lambda job: run_one(work, *job), jobs))
    for job, run_out in zip(jobs, outputs):
        _, seed, rate, config, weight = job
        family, result = analyse_run(run_out, seed, rate, config, weight, selected=True)
        rows += family
        results[rate, weight, seed] = result
    for row in rows:
        if (row["rate_per_s"] == hard.PRIMARY_RATE and row["seed"] == 42
                and row["affinity_weight"] == affinity):
            row["selected"] = True

    primary = aggregate(results, rows, hard.PRIMARY_RATE, affinity, causal_config)
    doubled = aggregate(results, rows, hard.DOUBLE_RATE, affinity, causal_config)
    if any(any(result["audit"].values()) for result in results.values()):
        raise AssertionError("causal soft audit failed")
    report = {"selection_protocol": "lowest trimmed mean slowdown on seed 42 at the primary rate",
              "tuning": tuning, "selected_affinity_weight": affinity,
              "primary": primary, "double_rate": doubled,
              "hard_results": str(args.hard_results.resolve())}
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(report, indent=1))
    with (out / "families.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    print(f"selected soft affinity {affinity} on seed 42")
    for label, policy, hard_policy in (("1x", primary, hard_results["primary"]),
                                       ("2x", doubled, hard_results["double_rate"])):
        jsq = hard_results["baselines"]["primary" if label == "1x" else "double_rate"]["jsq"]["pooled"]
        pooled = policy["pooled"]
        print(f"{label}: soft {pooled['median']:.3f} / {pooled['mean']:.3f} / {pooled['p99']:.3f}; "
              f"mean vs JSQ {percent(pooled['mean'], jsq['mean']):+.1f}%; "
              f"hard {hard_policy['pooled']['median']:.3f} / {hard_policy['pooled']['mean']:.3f} / "
              f"{hard_policy['pooled']['p99']:.3f}")
        print(label + " per-seed mean: " + ", ".join(
            f"{seed} {policy['per_seed'][str(seed)]['mean']:.3f}"
            for seed in tr.SEEDS))


if __name__ == "__main__":
    main()
