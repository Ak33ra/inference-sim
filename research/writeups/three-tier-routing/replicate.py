#!/usr/bin/env python3
"""Replicate the three-tier routing result (README.md). Run it through replicate.sh, which sets
BLIS_CATALOG, BLIS_REGISTRY and GOWORK (the patched router fork).

Ten semi-closed BLIS runs: JSQ and the three-tier 40/20/40 policy, seeds 42-46, on all 189
commit0 + Oolong optrouting families at Poisson rate 0.223837 families/s. Prints the pooled
slowdown statistics and checks them against the recorded result. Runs are cached in --out.
"""
import argparse, concurrent.futures as cf, csv, json, math, os, pathlib, random, shutil
import statistics, subprocess, sys, urllib.parse

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
sys.path.insert(0, str(REPO / "research/outputs/optrouting"))
import optimal_isolated as oi  # noqa: E402  (family files, config template, semi-closed runner)

RATE = 0.223837            # root families/s: JSQ mean slowdown 2.0 on commit0 alone (calibrated)
SEEDS = [42, 43, 44, 45, 46]
TRIM = 0.10                # arrivals dropped at each end, by arrival rank, as warmup and drain
GPUS = 8
TIERS = (0.4, 0.6)         # P1 = shortest 40% of families by optimum, P2 = up to 60%, P3 = the rest
ROUTERS = {"jsq": REPO / "research/configs/routers/jsq.yaml",
           "tier-40-60": REPO / "research/configs/routers/jsq-3tier.yaml"}
# Servers instance_0-1 serve P1 only, instance_2-3 P1 and P2, instance_4-7 everyone.
LABELS = {**{f"instance_{i}": {"research.blis/tier": "p1"} for i in (0, 1)},
          **{f"instance_{i}": {"research.blis/tier": "p12"} for i in (2, 3)}}
ALLOWED = {"P1": {"p1", "p12", None}, "P2": {"p12", None}, "P3": {None}}
GROUPS = {"p1": ["instance_0", "instance_1"], "p12": ["instance_2", "instance_3"],
          "shared": [f"instance_{i}" for i in range(4, 8)]}
# header-profile-handler is an Alpha plugin: BLIS's default router flags plus the opt-in.
EPP_FLAGS = ["--endpoint-selector=llm-d.ai/inference-serving=true", "--endpoint-target-ports=8000",
             "--allow-experimental-plugins=true"]
# The recorded result (pooled over the five seeds; trimmed families, n = 765).
EXPECTED = {
    "jsq": {"median": 2.069189912428907, "mean": 2.144251339727433, "p99": 4.493550515874677,
            "per_seed_mean": {42: 2.0443036858168524, 43: 2.2375398545035923, 44: 2.128672144545466,
                              45: 1.9801793142607396, 46: 2.330561699510515}},
    "tier-40-60": {"median": 1.833568078378399, "mean": 1.9601509426251664, "p99": 3.6077572963436624,
                   "per_seed_mean": {42: 1.9549048905028379, 43: 2.017031033488076, 44: 1.8929728690921628,
                                     45: 1.816184945912025, 46: 2.11966097413073}},
}
OPT = {}  # family id -> optimal isolated root latency (s), from optrouting


def log(*a):
    print(*a, file=sys.stderr, flush=True)


def load_families(work):
    """All commit0 then all Oolong families, in optrouting's order, with their optima in OPT."""
    lines = []
    for name in ("commit0", "oolong"):
        oi.DS = oi.DATASETS[name]
        sub = work / "datasets" / name; sub.mkdir(parents=True, exist_ok=True)
        oi.build_families(sub)
        lines += (sub / "families.jsonl").read_text().splitlines()
        OPT.update({r["family"]: float(r["optimal_s"]) for r in csv.DictReader((oi.DS["out"] / "root_e2e.csv").open())})
    oi.DS = oi.DATASETS["commit0"]  # the config template: commit0's (Oolong's serving block is identical)
    assert len(lines) == len(OPT) == 189 and {json.loads(l)["id"] for l in lines} == set(OPT)
    return lines


def classes():
    order = sorted(OPT, key=lambda k: (OPT[k], k))
    n1, n2 = round(TIERS[0] * len(order)), round(TIERS[1] * len(order))
    return {k: "P1" if i < n1 else "P2" if i < n2 else "P3" for i, k in enumerate(order)}


def trace(work, lines, seed, tiered):
    """The families in a seed-shuffled order (Poisson arrivals follow file order), tagged with
    their class as slo_class for the tiered policy."""
    lines = list(lines)
    random.Random(seed).shuffle(lines)
    if tiered:
        cls = classes()
        lines = [json.dumps({**(f := json.loads(l)), "slo_class": cls[f["id"]]}, separators=(",", ":"))
                 for l in lines]
    path = work / f"families-s{seed}{'-tiers' if tiered else ''}.jsonl"
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text("\n".join(lines) + "\n"); tmp.replace(path)
    return path


def run_one(work, router, path, seed, rate):
    out = work / "runs" / f"{router}-s{seed}"
    if not (out / "workload.json").exists():
        tiered = router != "jsq"
        c = oi.template()
        c["trace"] = {"format": "weka-dag", "path": str(path)}
        c["serving"]["router"]["endpoint_picker_config"] = str(ROUTERS[router])
        c["serving"]["cluster"]["replicas"] = GPUS
        assert c["serving"]["engine"]["enable_prefix_caching"] is False
        c["run"]["seed"] = seed
        c["run"]["arrivals"] = {"mode": "poisson", "rate_per_s": rate, "seed": seed}
        env = {k: v for k, v in os.environ.items()
               if k not in ("BLIS_RESEARCH_INSTANCE_LABELS", "BLIS_RESEARCH_ALLOW_HEADER_PROFILE")}
        if tiered:
            c["serving"]["router"]["epp_flags"] = EPP_FLAGS
            env |= {"BLIS_RESEARCH_INSTANCE_LABELS": json.dumps(LABELS), "BLIS_RESEARCH_ALLOW_HEADER_PROFILE": "1"}
        shutil.rmtree(out, ignore_errors=True); out.parent.mkdir(parents=True, exist_ok=True)
        cfg = out.parent / f"{out.name}.config.json"
        cfg.write_text(json.dumps(dict(c, output=str(out)), indent=1))
        log(f"run {out.name} started")
        with open(out.parent / f"{out.name}.log", "w") as fh:
            subprocess.run([str(oi.RUNNER), "run", str(cfg)], check=True, stdout=fh, stderr=fh, env=env)
        (out / "blis").unlink(missing_ok=True)
        log(f"run {out.name} done")
    return out


def analyse(out, seed):
    """Per-family rows (arrival order, trim flag) plus per-run placement and load checks."""
    wl = json.loads((out / "workload.json").read_text())
    assert wl["complete"] and wl["accounting_match"] and wl["states"]["failed"] == 0, out
    fams = sorted(wl["families"], key=lambda f: (f["birth_us"], f["trace_id"]))
    assert {f["trace_id"] for f in fams} == set(OPT), out
    cut = math.floor(TRIM * len(fams))
    cls = classes()
    rows = [{"family": f["trace_id"], "seed": seed, "class": cls[f["trace_id"]], "kept": cut <= i < len(fams) - cut,
             "arrival_s": f["birth_us"] / 1e6, "e2e_s": f["makespan_us"] / 1e6, "optimal_s": OPT[f["trace_id"]],
             "slowdown": f["makespan_us"] / 1e6 / OPT[f["trace_id"]]} for i, f in enumerate(fams)]
    reqs = json.loads((out / "metrics.json").read_text())["requests"]
    tier = {i: l["research.blis/tier"] for i, l in LABELS.items()}
    fam = lambda r: urllib.parse.unquote(r["requestID"].rsplit("/", 1)[0])
    violations = sum(tier.get(r["handled_by"]) not in ALLOWED[cls[fam(r)]] for r in reqs)
    # Time-averaged active requests per server (arrival to completion) over the trimmed window.
    t0, t1 = min(r["arrival_s"] for r in rows if r["kept"]), max(r["arrival_s"] for r in rows if r["kept"])
    busy = {}
    for r in reqs:
        a, b = max(r["arrived_at"], t0), min(r["arrived_at"] + r["e2e_ms"] / 1e3, t1)
        busy[r["handled_by"]] = busy.get(r["handled_by"], 0.0) + max(0.0, b - a)
    active = {g: statistics.fmean(busy.get(i, 0.0) / (t1 - t0) for i in ids) for g, ids in GROUPS.items()}
    return rows, {"violations": violations, "active": active}


def stats(v):
    v = sorted(v)
    return {"n": len(v), "median": statistics.median(v), "mean": statistics.fmean(v),
            "p99": statistics.quantiles(v, n=100, method="inclusive")[98]}


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=pathlib.Path, default=REPO / "research/outputs/three-tier-routing")
    p.add_argument("--jobs", type=int, default=10, help="parallel BLIS runs (each needs a few GB of RAM)")
    p.add_argument("--rate", type=float, default=RATE,
                   help=f"Poisson root-family rate/s (default: recorded result, {RATE})")
    a = p.parse_args()
    if not math.isfinite(a.rate) or a.rate <= 0:
        p.error("--rate must be finite and positive")
    for v in ("BLIS_CATALOG", "BLIS_REGISTRY", "GOWORK"):
        if not os.environ.get(v):
            sys.exit(f"{v} is not set; run replicate.sh")
    work = a.out.resolve() / "work"; work.mkdir(parents=True, exist_ok=True)
    lines = load_families(work)
    jobs = [(router, seed, trace(work, lines, seed, router != "jsq")) for router in ROUTERS for seed in SEEDS]
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        outs = list(ex.map(lambda j: run_one(work, j[0], j[2], j[1], a.rate), jobs))
    results, ok, all_rows = {}, True, []
    for (router, seed, _), out in zip(jobs, outs):
        rows, checks = analyse(out, seed)
        all_rows += [dict(r, router=router) for r in rows]
        res = results.setdefault(router, {"per_seed": {}})
        res["per_seed"][seed] = {"mean": statistics.fmean(r["slowdown"] for r in rows if r["kept"]), **checks}
    for router, res in results.items():
        kept = [r for r in all_rows if r["router"] == router and r["kept"]]
        res["pooled"] = stats(r["slowdown"] for r in kept)
        res["by_class"] = {c: stats(r["slowdown"] for r in kept if r["class"] == c) for c in ("P1", "P2", "P3")}
        res["rate_per_s"] = a.rate
        if a.rate == RATE:
            exp = EXPECTED[router]
            match = all(math.isclose(res["pooled"][k], exp[k], rel_tol=1e-12) for k in ("median", "mean", "p99")) and \
                all(math.isclose(res["per_seed"][s]["mean"], exp["per_seed_mean"][s], rel_tol=1e-12) for s in SEEDS)
            res["matches_recorded"] = match; ok &= match
        else:
            res["matches_recorded"] = None
    if results["tier-40-60"]["per_seed"] and any(results["tier-40-60"]["per_seed"][s]["violations"] for s in SEEDS):
        ok = False
    a.out.mkdir(parents=True, exist_ok=True)
    (a.out / "results.json").write_text(json.dumps(results, indent=1))
    with (a.out / "families.csv").open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(all_rows[0])); w.writeheader(); w.writerows(all_rows)
    j, t = results["jsq"], results["tier-40-60"]
    pc = lambda x, y: f"{100 * (x / y - 1):+.1f}%"
    print("policy      median   mean     p99      (pooled, seeds 42-46, n = 765)")
    for name, r in (("JSQ", j), ("3-tier", t)):
        print(f"{name:<11} {r['pooled']['median']:.3f}    {r['pooled']['mean']:.3f}    {r['pooled']['p99']:.3f}")
    print(f"3-tier vs JSQ: median {pc(t['pooled']['median'], j['pooled']['median'])}, "
          f"mean {pc(t['pooled']['mean'], j['pooled']['mean'])}, p99 {pc(t['pooled']['p99'], j['pooled']['p99'])}")
    print("by class (mean, 3-tier vs JSQ): " + ", ".join(
        f"{c} {t['by_class'][c]['mean']:.3f} vs {j['by_class'][c]['mean']:.3f} ({pc(t['by_class'][c]['mean'], j['by_class'][c]['mean'])})"
        for c in ("P1", "P2", "P3")))
    for s in SEEDS:
        a_ = t["per_seed"][s]["active"]
        print(f"seed {s}: mean {t['per_seed'][s]['mean']:.3f} vs JSQ {j['per_seed'][s]['mean']:.3f} "
              f"({pc(t['per_seed'][s]['mean'], j['per_seed'][s]['mean'])}); active per server "
              f"p1 {a_['p1']:.1f} / p12 {a_['p12']:.1f} / shared {a_['shared']:.1f}; "
              f"violations {t['per_seed'][s]['violations']}")
    if a.rate == RATE:
        print("REPLICATED: matches the recorded result" if ok else "MISMATCH: see results.json")
    else:
        print(f"CUSTOM RATE COMPLETE: {a.rate:.6f} families/s" if ok else "INVALID: see results.json")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
