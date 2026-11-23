#!/usr/bin/env python3
"""Tune and evaluate non-clairvoyant, family-level observed-work tiers.

The policy charges prompt tokens when a request is admitted and charges completion
tokens only after they are returned.  Its two thresholds select P1/P2/P3; routing
within each eligible pool is the same JSQ policy as the clairvoyant three-tier run.

Only seed 42 at the original arrival rate selects the parameters.  The chosen
configuration is then evaluated on held-out seeds 43-46 and, without retuning, on
all five seeds at twice the arrival rate.
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
SPEC = importlib.util.spec_from_file_location("three_tier_replicate", HERE / "replicate.py")
tr = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(tr)

# A 3x3 sweep around the 35/55, 40/60 and 45/65 percentiles of final observed
# work.  Output weights span the plausible decode-to-prefill token cost range.
CANDIDATES = (
    (32, 1_900_000, 3_600_000), (32, 2_250_000, 4_150_000), (32, 2_800_000, 5_200_000),
    (64, 2_400_000, 4_600_000), (64, 3_000_000, 5_400_000), (64, 3_600_000, 6_500_000),
    (128, 3_600_000, 6_600_000), (128, 4_400_000, 8_400_000), (128, 5_400_000, 9_400_000),
)
PRIMARY_RATE = tr.RATE
DOUBLE_RATE = 2 * tr.RATE


def slug(config):
    weight, first, second = config
    return f"w{weight}-t{first}-{second}"


def log(*args):
    print(*args, file=sys.stderr, flush=True)


def trace(work, lines, seed):
    return tr.trace(work, lines, seed, False)


def run_one(work, path, seed, rate, config):
    rate_name = "1x" if rate == PRIMARY_RATE else "2x"
    name = f"causal-{slug(config)}-{rate_name}-s{seed}"
    out = work / "runs" / name
    cached = False
    try:
        report = json.loads((out / "workload.json").read_text())
        cached = report["complete"] and report["accounting_match"] and (out / "metrics.json").is_file()
    except (OSError, KeyError, json.JSONDecodeError):
        pass
    if not cached:
        weight, first, second = config
        scenario = tr.oi.template()
        scenario["trace"] = {"format": "weka-dag", "path": str(path)}
        scenario["serving"]["router"]["endpoint_picker_config"] = str(tr.ROUTERS["tier-40-60"])
        scenario["serving"]["router"]["epp_flags"] = tr.EPP_FLAGS
        scenario["serving"]["cluster"]["replicas"] = tr.GPUS
        assert scenario["serving"]["engine"]["enable_prefix_caching"] is False
        scenario["run"]["seed"] = seed
        scenario["run"]["arrivals"] = {"mode": "poisson", "rate_per_s": rate, "seed": seed}
        scenario["run"]["causal_tiers"] = {
            "thresholds": [first, second], "completed_output_weight": weight}
        env = {key: value for key, value in os.environ.items()
               if key not in ("BLIS_RESEARCH_INSTANCE_LABELS", "BLIS_RESEARCH_ALLOW_HEADER_PROFILE")}
        env |= {"BLIS_RESEARCH_INSTANCE_LABELS": json.dumps(tr.LABELS),
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


def family_rows(workload, seed, rate, config, selected):
    families = sorted(workload["families"], key=lambda row: (row["birth_us"], row["trace_id"]))
    assert {row["trace_id"] for row in families} == set(tr.OPT)
    cut = math.floor(tr.TRIM * len(families))
    oracle = tr.classes()
    return [{"rate_per_s": rate, "seed": seed, "candidate": slug(config), "selected": selected,
             "family": row["trace_id"], "oracle_class": oracle[row["trace_id"]],
             "kept": cut <= index < len(families) - cut, "arrival_s": row["birth_us"] / 1e6,
             "e2e_s": row["makespan_us"] / 1e6, "optimal_s": tr.OPT[row["trace_id"]],
             "slowdown": row["makespan_us"] / 1e6 / tr.OPT[row["trace_id"]]}
            for index, row in enumerate(families)]


def analyse_run(out, seed, rate, config, selected=False):
    workload = json.loads((out / "workload.json").read_text())
    metrics = json.loads((out / "metrics.json").read_text())
    weight, first, second = config
    assert workload["complete"] and workload["accounting_match"]
    assert workload["states"]["failed"] == 0
    assert workload["causal_tiers"] == {
        "thresholds": [first, second], "completed_output_weight": weight}

    requests = {row["request_id"]: row for row in workload["requests"]}
    assert len(requests) == workload["selected"]["requests"]
    class_counts = {name: 0 for name in ("P1", "P2", "P3")}
    bad_classes = bad_charges = 0
    for row in requests.values():
        after = row.get("observed_work_after_admission", 0)
        expected = "P1" if after <= first else "P2" if after <= second else "P3"
        bad_classes += row.get("routing_class") != expected
        bad_charges += after - row.get("observed_work_before", 0) != row["prompt_tokens"]
        bad_charges += row.get("completed_output_charge", 0) != row["output_tokens"] * weight
        class_counts[row["routing_class"]] += 1

    expected_final = {}
    for row in workload["requests"]:
        expected_final[row["trace_id"]] = expected_final.get(row["trace_id"], 0) + \
            row["prompt_tokens"] + row["output_tokens"] * weight
    bad_final = sum(row.get("observed_work", 0) != expected_final[row["trace_id"]]
                    for row in workload["families"])

    server_tier = {instance: labels["research.blis/tier"] for instance, labels in tr.LABELS.items()}
    violations = slo_mismatches = 0
    for row in metrics["requests"]:
        request_class = requests[row["requestID"]]["routing_class"]
        violations += server_tier.get(row["handled_by"]) not in tr.ALLOWED[request_class]
        slo_mismatches += row.get("slo_class") != request_class

    rows = family_rows(workload, seed, rate, config, selected)
    kept = [row["slowdown"] for row in rows if row["kept"]]
    return rows, {
        "mean": statistics.fmean(kept), "stats": tr.stats(kept), "class_counts": class_counts,
        "audit": {"bad_classes": bad_classes, "bad_charges": bad_charges,
                  "bad_final_family_work": bad_final, "placement_violations": violations,
                  "metric_class_mismatches": slo_mismatches},
    }


def aggregate(run_results, all_rows, rate, config):
    chosen = [row for row in all_rows if row["selected"] and row["rate_per_s"] == rate
              and row["candidate"] == slug(config)]
    kept = [row for row in chosen if row["kept"]]
    oracle = {name: tr.stats(row["slowdown"] for row in kept if row["oracle_class"] == name)
              for name in ("P1", "P2", "P3")}
    return {"config": {"completed_output_weight": config[0],
                        "thresholds": [config[1], config[2]]},
            "rate_per_s": rate, "pooled": tr.stats(row["slowdown"] for row in kept),
            "by_oracle_class": oracle,
            "per_seed": {str(seed): run_results[(rate, config, seed)] for seed in tr.SEEDS}}


def load_baseline(path, label):
    value = json.loads(path.read_text())
    assert "jsq" in value and "tier-40-60" in value, f"{label} lacks baseline policies"
    return {"jsq": value["jsq"], "three_tier": value["tier-40-60"], "path": str(path)}


def percent(value, baseline):
    return 100 * (value / baseline - 1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path,
                        default=REPO / "research/outputs/three-tier-routing-causal")
    parser.add_argument("--jobs", type=int, default=6,
                        help="parallel BLIS runs; each needs a few GB of RAM")
    parser.add_argument("--primary-baseline", type=Path,
                        default=REPO / "research/outputs/three-tier-routing/results.json")
    parser.add_argument("--double-baseline", type=Path,
                        default=REPO / "research/outputs/three-tier-routing-2x/results.json")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    for variable in ("BLIS_CATALOG", "BLIS_REGISTRY", "GOWORK"):
        if not os.environ.get(variable):
            sys.exit(f"{variable} is not set; run through causal_tier_compare.sh")
    primary_baseline = load_baseline(args.primary_baseline.resolve(), "primary baseline")
    double_baseline = load_baseline(args.double_baseline.resolve(), "double baseline")

    out = args.out.resolve()
    work = out / "work"
    work.mkdir(parents=True, exist_ok=True)
    lines = tr.load_families(work)
    paths = {seed: trace(work, lines, seed) for seed in tr.SEEDS}

    tune_jobs = [(paths[42], 42, PRIMARY_RATE, config) for config in CANDIDATES]
    with cf.ThreadPoolExecutor(args.jobs) as executor:
        tune_outs = list(executor.map(lambda job: run_one(work, *job), tune_jobs))
    run_results, rows = {}, []
    tuning = []
    for config, run_out in zip(CANDIDATES, tune_outs):
        family, result = analyse_run(run_out, 42, PRIMARY_RATE, config)
        rows += family
        run_results[PRIMARY_RATE, config, 42] = result
        tuning.append({"config": {"completed_output_weight": config[0],
                                   "thresholds": [config[1], config[2]]}, **result})
    selected = min(CANDIDATES, key=lambda config: (run_results[PRIMARY_RATE, config, 42]["mean"], config))
    log(f"selected on seed 42: {slug(selected)}")

    evaluation_jobs = []
    for seed in tr.SEEDS[1:]:
        evaluation_jobs.append((paths[seed], seed, PRIMARY_RATE, selected))
    for seed in tr.SEEDS:
        evaluation_jobs.append((paths[seed], seed, DOUBLE_RATE, selected))
    with cf.ThreadPoolExecutor(args.jobs) as executor:
        evaluation_outs = list(executor.map(lambda job: run_one(work, *job), evaluation_jobs))
    for job, run_out in zip(evaluation_jobs, evaluation_outs):
        _, seed, rate, config = job
        family, result = analyse_run(run_out, seed, rate, config, selected=True)
        rows += family
        run_results[rate, config, seed] = result

    # Mark the tuning run of the selected configuration as selected too.
    for row in rows:
        if row["rate_per_s"] == PRIMARY_RATE and row["seed"] == 42 and row["candidate"] == slug(selected):
            row["selected"] = True
    primary = aggregate(run_results, rows, PRIMARY_RATE, selected)
    doubled = aggregate(run_results, rows, DOUBLE_RATE, selected)
    audits = [result["audit"] for result in run_results.values()]
    if any(any(audit.values()) for audit in audits):
        raise AssertionError("causal or placement audit failed")

    result = {"selection_protocol": "lowest trimmed mean slowdown on seed 42 at the primary rate",
              "candidates": tuning, "selected": primary["config"],
              "primary": primary, "double_rate": doubled,
              "baselines": {"primary": primary_baseline, "double_rate": double_baseline}}
    out.mkdir(parents=True, exist_ok=True)
    (out / "results.json").write_text(json.dumps(result, indent=1))
    with (out / "families.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

    print(f"selected {slug(selected)} on seed 42")
    print("rate    policy       median   mean     p99      mean vs JSQ")
    for rate_name, policy, baseline in (("1x", primary, primary_baseline),
                                        ("2x", doubled, double_baseline)):
        pooled = policy["pooled"]
        jsq = baseline["jsq"]["pooled"]
        print(f"{rate_name:<7} causal       {pooled['median']:.3f}    {pooled['mean']:.3f}    "
              f"{pooled['p99']:.3f}    {percent(pooled['mean'], jsq['mean']):+.1f}%")
        for name in ("jsq", "three_tier"):
            base = baseline[name]["pooled"]
            print(f"{rate_name:<7} {name:<12} {base['median']:.3f}    {base['mean']:.3f}    {base['p99']:.3f}")
    for rate_name, policy, baseline in (("1x", primary, primary_baseline),
                                        ("2x", doubled, double_baseline)):
        print(rate_name + " per-seed mean: " + ", ".join(
            f"{seed} {policy['per_seed'][str(seed)]['mean']:.3f} "
            f"({percent(policy['per_seed'][str(seed)]['mean'], baseline['jsq']['per_seed'][str(seed)]['mean']):+.1f}%)"
            for seed in tr.SEEDS))


if __name__ == "__main__":
    main()
