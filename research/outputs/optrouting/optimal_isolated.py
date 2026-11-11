#!/usr/bin/env python3
"""Optimal isolated root-session end-to-end latency for every commit0 or Oolong family on 8 B200 replicas.

For each family (root session plus its child sessions) run alone on 8 replicas:
  * observed root end-to-end latency under JSQ and least-token (one BLIS run per router and seed,
    all families in one run, spaced far enough apart that they never overlap), and
  * the optimal latency under any work-conserving routing, found by pinning child sessions to GPUs.

Why pinning is exact here: the root issues nothing between a spawn and its join, so during a fork
the cluster serves only that fork's children. With each child pinned to one GPU, the GPUs are
independent, and each GPU is simulated as a single replica serving its subset of children. A
fork's makespan can never beat its slowest child run alone on an idle GPU (the lower bound); a
longest-first greedy assignment, followed by move/swap local search if needed, reaches it. Root
requests run one at a time and routing cannot change them, so

    optimal root latency = observed root latency - sum over forks (observed fork - optimal fork).

The script checks that this gives the same value from every observed run.

Usage (BLIS_CATALOG and BLIS_REGISTRY must be set; replicate.sh does that):
    optimal_isolated.py [--dataset commit0|oolong] [--work DIR] [--out DIR] [--seeds 42 43 ...]
                        [--jobs N] [--verify N]
Work is checkpointed in WORK/cache.json and WORK/vacuum/; rerunning resumes.
"""
import argparse, collections, concurrent.futures as cf, csv, hashlib, json, math, os, pathlib, random
import re, shutil, statistics, subprocess, sys, threading

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[2]
TRACES = REPO / "traces/commit0-fanout-buckets"
# Per dataset: (bucket, trace file) sources in report order, which family ids to keep, the tracked
# config the serving setup is copied from, and the default work/output directories.
DATASETS = {
    "commit0": {
        "sources": [(b, TRACES / b / "traces.jsonl.zst") for b in
                    ["w00-04", "w05-09", "w10-14", "w15-19", "w20-24", "w25-29", "w30-34", "w65-69"]],
        "keep": lambda fid: fid.startswith("commit0"),
        "template": REPO / "research/configs/semiclosed/commit0-overlap-jsq-8replicas.json",
        "work": HERE / "work", "out": HERE / "data"},
    "oolong": {
        "sources": [("oolong", REPO / "natural-oolong-fanout.jsonl.zst")],
        "keep": lambda fid: True,
        "template": REPO / "research/configs/semiclosed/natural-oolong-8nodes.json",
        "work": HERE / "work" / "oolong", "out": HERE / "data" / "oolong"},
}
DS = DATASETS["commit0"]  # set from --dataset in main()
ROUTERS = {"jsq": REPO / "research/configs/routers/jsq.yaml",
           "least-token": REPO / "research/configs/routers/least-token.yaml"}
RUNNER = REPO / "research/scripts/semi_closed.sh"
GPUS = 8
FAMILY_SPACING_S = 12000  # longest isolated family is ~3400 s
SUBSET_SPACING_S = 4000
MAX_BATCH = 300
EXHAUSTIVE_MAX = 200_000  # enumerate every pinning (into all GPUs) when there are at most this many: <= 12 children


def log(*a):
    print(*a, file=sys.stderr, flush=True)


# ---------------------------------------------------------------- families and fork structure

def build_families(work, only=None):
    """Write the dataset's families (ids matching `only`, if given) to WORK/families.jsonl; return {id: bucket}."""
    path, index = work / "families.jsonl", work / "buckets.json"
    if path.exists() and index.exists():
        bucket = json.loads(index.read_text())
        assert all(re.search(only, f) for f in bucket) if only else True, "--only differs from WORK's families; use a fresh --work"
        return bucket
    lines, bucket = [], {}
    for b, src in DS["sources"]:
        text = subprocess.run(["zstd", "-dc", str(src)], capture_output=True, text=True, check=True).stdout
        for line in text.splitlines():
            fid = json.loads(line)["id"]
            if DS["keep"](fid) and (not only or re.search(only, fid)):
                assert fid not in bucket, ("duplicate family id", fid)
                lines.append(line); bucket[fid] = b
    path.write_text("\n".join(lines) + "\n"); index.write_text(json.dumps(bucket, indent=1))
    log(f"{len(lines)} families")
    return bucket


def forks_of(fam):
    """{spawn request id: sorted child ids}, after checking the structure the method relies on."""
    child = {e["agent_id"]: e["requests"] for e in fam["requests"] if e.get("type") == "subagent"}
    root = {r["req_id"]: r for r in fam["requests"] if r.get("type") != "subagent"}
    for rs in child.values():
        assert not any(e.get("type") == "subagent" for e in rs), (fam["id"], "nesting deeper than 1")
    owner = {r["req_id"]: k for k, rs in child.items() for r in rs}
    spawn, join = {}, collections.defaultdict(set)
    for k, rs in child.items():
        sp = {p for r in rs for p in r["parent_ids"] if p in root}
        assert len(sp) == 1, (fam["id"], k, "child must have exactly one spawn", sp)
        spawn[k] = sp.pop()
    for r in root.values():
        for p in r["parent_ids"]:
            if p in owner: join[owner[p]].add(r["req_id"])
    forks = collections.defaultdict(list)
    for k, sp in spawn.items():
        forks[sp].append(k)
    for sp, kids in forks.items():
        nxt = [r["req_id"] for r in root.values() if sp in r["parent_ids"]]
        joins = set().union(*(join[k] for k in kids))
        if nxt:  # the root's next request after the spawn must be the join of every child
            assert len(nxt) == 1 and all(join[k] == set(nxt) for k in kids), (fam["id"], sp, "root not idle during fork")
        else:    # spawned by the root's last request: children are never joined, the family ends with them
            assert not joins, (fam["id"], sp)
    return child, root, {sp: sorted(k) for sp, k in forks.items()}


# ---------------------------------------------------------------- running BLIS

def template():
    c = json.loads(DS["template"].read_text())
    c["run"]["series_interval_s"] = 0
    c["serving"].setdefault("router", {})
    return c


def run_blis(cfg, out):
    """Run the semi-closed runner once; return the parsed workload.json."""
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.rmtree(out, ignore_errors=True)
    cfg = dict(cfg, output=str(out))
    cfg_path = out.parent / f"{out.name}.config.json"
    cfg_path.write_text(json.dumps(cfg, indent=1))
    with open(out.parent / f"{out.name}.log", "w") as fh:
        subprocess.run([str(RUNNER), "run", str(cfg_path)], check=True, stdout=fh, stderr=fh)
    (out / "blis").unlink(missing_ok=True)
    wl = json.loads((out / "workload.json").read_text())
    assert wl["complete"] and wl["accounting_match"] and wl["states"]["failed"] == 0, out
    fams = sorted(wl["families"], key=lambda f: f["birth_us"])
    assert all(a["completion_us"] < b["birth_us"] for a, b in zip(fams, fams[1:])), (out, "families overlapped")
    return wl


def vacuum_runs(work, seeds, jobs):
    """Every family alone on 8 replicas, per router and seed. Returns {(router, seed): workload path}."""
    todo, paths = [], {}
    for router in ROUTERS:
        for seed in seeds:
            out = work / "vacuum" / f"{router}-s{seed}"; paths[(router, seed)] = out / "workload.json"
            if not paths[(router, seed)].exists():
                c = template()
                c["trace"] = {"format": "weka-dag", "path": str(work / "families.jsonl")}
                c["serving"]["router"]["endpoint_picker_config"] = str(ROUTERS[router])
                c["serving"]["cluster"]["replicas"] = GPUS
                c["run"]["seed"] = seed
                c["run"]["arrivals"] = {"mode": "fixed", "interval_s": FAMILY_SPACING_S, "seed": 42}
                todo.append((c, out))
    with cf.ThreadPoolExecutor(jobs) as ex:
        for (c, out), _ in zip(todo, ex.map(lambda a: run_blis(*a), todo)):
            log(f"vacuum run {out.name} done")
    return paths


def observed(workload_path):
    """Per-fork makespan (last child completion - spawn completion) and per-family root latency."""
    wl = json.loads(workload_path.read_text())
    reqs = {r["request_id"]: r for r in wl["requests"]}
    root_arrival = {s["session_id"]: s["arrival_us"] for s in wl["sessions"]}
    fams = {}
    for f in wl["families"]:  # root e2e = first root request to the family's last request
        assert root_arrival[f["trace_id"]] == f["birth_us"], f["trace_id"]
        fams[f["trace_id"]] = f["makespan_us"] / 1e6
    by_spawn = collections.defaultdict(list)
    for r in wl["requests"]:
        if r["session_id"] == r["trace_id"]: continue
        for p in r["prerequisites"]:
            if reqs[p["request_id"]]["session_id"] == r["trace_id"]: by_spawn[p["request_id"]].append(r)
    by_sess = collections.defaultdict(list)
    for r in wl["requests"]: by_sess[r["session_id"]].append(r)
    forks = {}
    for sp, firsts in by_spawn.items():
        rs = [r for s in {x["session_id"] for x in firsts} for r in by_sess[s]]
        key = (rs[0]["trace_id"], int(sp.rsplit("/", 1)[1]))
        forks[key] = (max(r["completion_us"] for r in rs) - reqs[sp]["completion_us"]) / 1e6
    return forks, fams


class Scorer:
    """Finish time (s after spawn completion) of a set of one fork's children pinned to one GPU.
    Results are checkpointed in WORK/cache.json; batches run in parallel."""

    def __init__(self, work, families, forks, jobs):
        self.work, self.fams, self.forks, self.jobs = work, families, forks, jobs
        self.path = work / "cache.json"; self.lock = threading.Lock()
        self.cache = ({(tuple(k[0]), tuple(k[1])): v for k, v in json.loads(self.path.read_text())}
                      if self.path.exists() else {})
        self.simulated = 0

    def _save(self):
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps([[[list(f), list(s)], v] for (f, s), v in self.cache.items()]))
        os.replace(tmp, self.path)

    def _family(self, i, fork_key, kids):
        fam = self.fams[fork_key[0]]; child = self.forks[fork_key]["child"]; reqs = []
        for k in kids:
            ids = {r["req_id"] for r in child[k]}
            for r in child[k]:
                r = dict(r); r["parent_ids"] = [p for p in r["parent_ids"] if p in ids]; reqs.append(r)
        return {"id": f"subset{i:05d}", "models": fam["models"], "block_size": fam["block_size"],
                "hash_id_scope": fam["hash_id_scope"], "requests": reqs}

    def _offset(self, fork_key, kids):
        fk = self.forks[fork_key]; child = fk["child"]
        tmin = min(r["t"] for k in kids for r in child[k] if not set(r["parent_ids"]) & {q["req_id"] for q in child[k]})
        return tmin - fk["spawn_end"]

    def simulate(self, chunk):
        text = "".join(json.dumps(self._family(j, f, s)) + "\n" for j, (f, s) in enumerate(chunk))
        tag = hashlib.sha256(text.encode()).hexdigest()[:16]
        inp = self.work / "subsets" / f"{tag}.jsonl"; inp.parent.mkdir(parents=True, exist_ok=True)
        inp.write_text(text)
        c = template(); c["trace"] = {"format": "weka-dag", "path": str(inp)}
        c["serving"]["cluster"]["replicas"] = 1; c["serving"]["router"]["endpoint_picker_config"] = str(ROUTERS["jsq"])
        c["run"]["seed"] = 42; c["run"]["arrivals"] = {"mode": "fixed", "interval_s": SUBSET_SPACING_S, "seed": 42}
        wl = run_blis(c, self.work / "subsets" / tag)
        fams = sorted(wl["families"], key=lambda f: int(f["trace_id"].removeprefix("subset")))
        res = [fr["makespan_us"] / 1e6 + self._offset(f, s) for (f, s), fr in zip(chunk, fams)]
        for p in (inp, self.work / "subsets" / f"{tag}.config.json", self.work / "subsets" / f"{tag}.log"):
            p.unlink(missing_ok=True)
        shutil.rmtree(self.work / "subsets" / tag)  # the cache is the checkpoint; run output is not kept
        return res

    def __call__(self, items):
        norm = [(f, tuple(sorted(s))) for f, s in items]
        todo = sorted({x for x in norm if x[1] and x not in self.cache})
        if todo:
            size = min(MAX_BATCH, max(10, math.ceil(len(todo) / self.jobs)))
            chunks = [todo[i:i + size] for i in range(0, len(todo), size)]
            with cf.ThreadPoolExecutor(self.jobs) as ex:
                futs = {ex.submit(self.simulate, ch): ch for ch in chunks}
                for fut in cf.as_completed(futs):
                    ch = futs[fut]; vals = fut.result()
                    with self.lock:
                        self.cache.update(zip(ch, vals)); self.simulated += len(ch); self._save()
            log(f"  simulated {len(todo)} subsets in {len(chunks)} batches (cache {len(self.cache)})")
        return [self.cache[x] if x[1] else 0.0 for x in norm]


# ---------------------------------------------------------------- search

def search(score, forks):
    keys = sorted(forks)
    solo_items = [(f, (k,)) for f in keys for k in forks[f]["kids"]]
    solo = dict(zip(solo_items, score(solo_items)))
    bound = {f: max(solo[(f, (k,))] for k in forks[f]["kids"]) for f in keys}
    order = {f: sorted(forks[f]["kids"], key=lambda k: -solo[(f, (k,))]) for f in keys}
    bins = {f: [[] for _ in range(GPUS)] for f in keys}
    for step in range(max((len(v) for v in order.values()), default=0)):  # longest-first greedy, all forks in lockstep
        active = [f for f in keys if step < len(order[f])]
        cand = [(f, tuple(b + [order[f][step]])) for f in active for b in bins[f]]
        vals = dict(zip(cand, score(cand)))
        for f in active:
            k = order[f][step]
            j = min(range(GPUS), key=lambda j: (vals[(f, tuple(bins[f][j] + [k]))], len(bins[f][j])))
            bins[f][j].append(k)
        log(f"greedy step {step + 1}: {len(active)} forks")
    best = {f: max(score([(f, tuple(b)) for b in bins[f]])) for f in keys}
    for f in keys:  # move/swap local search on any fork the greedy left above its lower bound
        while best[f] > bound[f] + 1e-6:
            vals = score([(f, tuple(b)) for b in bins[f]]); w = max(range(GPUS), key=lambda j: vals[j])
            moves = []
            for k in bins[f][w]:
                for j in range(GPUS):
                    if j == w: continue
                    moves.append(([x for x in bins[f][w] if x != k], bins[f][j] + [k], j))
                    moves += [([x for x in bins[f][w] if x != k] + [m], [x for x in bins[f][j] if x != m] + [k], j)
                              for m in bins[f][j]]
            score([(f, tuple(a)) for a, _, _ in moves] + [(f, tuple(b)) for _, b, _ in moves])
            a, b, j = min(moves, key=lambda m: max(score([(f, tuple(m[0])), (f, tuple(m[1]))])))
            if max(score([(f, tuple(a)), (f, tuple(b))])) >= vals[w] - 1e-6: break
            bins[f][w], bins[f][j] = a, b
            best[f] = max(score([(f, tuple(b)) for b in bins[f]]))
    proof = {f: "lower_bound" if best[f] <= bound[f] + 1e-6 else "none" for f in keys}
    for f in keys:  # small forks above the bound: try every pinning that uses all GPUs
        kids = forks[f]["kids"]
        if proof[f] != "none" or stirling2(len(kids), GPUS) > EXHAUSTIVE_MAX: continue
        parts = list(partitions(kids, GPUS))
        score([(f, tuple(b)) for pt in parts for b in pt if len(b) > 1])
        top = min(parts, key=lambda pt: max(score([(f, tuple(b)) for b in pt])))
        val = max(score([(f, tuple(b)) for b in top]))
        if val < best[f] - 1e-6: best[f], bins[f] = val, [list(b) for b in top]
        proof[f] = "exhaustive_pinned"
        log(f"exhaustive: {f[0]} spawn {f[1]}: {len(parts)} pinnings, best {best[f]:.3f} s (bound {bound[f]:.3f})")
    return bound, best, bins, proof


def stirling2(n, k):
    """Number of ways to split n children into exactly k non-empty GPU groups."""
    return sum((-1) ** i * math.comb(k, i) * (k - i) ** n for i in range(k + 1)) // math.factorial(k)


def partitions(items, k):
    """Every split of items into exactly k non-empty groups. Using every GPU is enough: under the
    assumption the lower bound rests on, splitting a group never makes it finish later."""
    def rec(i, groups):
        if len(groups) + len(items) - i < k: return
        if i == len(items):
            yield [tuple(g) for g in groups]; return
        for g in groups:
            g.append(items[i]); yield from rec(i + 1, groups); g.pop()
        if len(groups) < k:
            groups.append([items[i]]); yield from rec(i + 1, groups); groups.pop()
    yield from rec(0, [])


# ---------------------------------------------------------------- report

def report(out, bucket, forks, bound, best, bins, proof, runs):
    out.mkdir(parents=True, exist_ok=True)
    obs = {k: observed(p) for k, p in runs.items()}
    seeds = sorted({s for _, s in runs})
    med = statistics.median
    fork_rows, partitions = [], collections.defaultdict(dict)
    for f in sorted(forks):
        o = {r: [obs[(r, s)][0][f] for s in seeds] for r in ROUTERS}
        fork_rows.append({"family": f[0], "spawn_request": f[1], "children": len(forks[f]["kids"]),
                          "lower_bound_s": round(bound[f], 3), "optimal_s": round(best[f], 3), "proof": proof[f],
                          "jsq_median_s": round(med(o["jsq"]), 3), "jsq_min_s": round(min(o["jsq"]), 3),
                          "jsq_max_s": round(max(o["jsq"]), 3),
                          "least_token_median_s": round(med(o["least-token"]), 3),
                          "least_token_min_s": round(min(o["least-token"]), 3),
                          "least_token_max_s": round(max(o["least-token"]), 3),
                          "gpus_used": sum(1 for b in bins[f] if b)})
        partitions[f[0]][str(f[1])] = [sorted(b) for b in bins[f] if b]
    fam_rows = []
    order = [b for b, _ in DS["sources"]]
    for fid in sorted(bucket, key=lambda x: (order.index(bucket[x]), x)):
        fk = [f for f in forks if f[0] == fid]
        lat = {r: [obs[(r, s)][1][fid] for s in seeds] for r in ROUTERS}
        opt = [obs[k][1][fid] - sum(obs[k][0][f] - best[f] for f in fk) for k in runs]
        lb = [obs[k][1][fid] - sum(obs[k][0][f] - bound[f] for f in fk) for k in runs]
        o = med(opt)
        unproven = [f for f in fk if proof[f] != "lower_bound"]
        fam_rows.append({"family": fid, "bucket": bucket[fid], "child_sessions": sum(len(forks[f]["kids"]) for f in fk),
                         "forks": len(fk), "widest_fork": max((len(forks[f]["kids"]) for f in fk), default=0),
                         "optimal_s": round(o, 3), "optimal_lower_bound_s": round(med(lb), 3),
                         "forks_not_at_bound": len(unproven), "optimal_spread_s": round(max(opt) - min(opt), 3),
                         "jsq_median_s": round(med(lat["jsq"]), 3), "jsq_min_s": round(min(lat["jsq"]), 3),
                         "jsq_max_s": round(max(lat["jsq"]), 3),
                         "least_token_median_s": round(med(lat["least-token"]), 3),
                         "least_token_min_s": round(min(lat["least-token"]), 3),
                         "least_token_max_s": round(max(lat["least-token"]), 3),
                         "jsq_excess_pct": round(100 * (med(lat["jsq"]) / o - 1), 3),
                         "least_token_excess_pct": round(100 * (med(lat["least-token"]) / o - 1), 3)})
    for name, rows in (("root_e2e.csv", fam_rows), ("forks.csv", fork_rows)):
        with open(out / name, "w", newline="") as fh:
            w = csv.DictWriter(fh, rows[0].keys()); w.writeheader(); w.writerows(rows)
    (out / "partitions.json").write_text(json.dumps(partitions, indent=1))
    by_bucket = {}
    for b in order:
        rs = [r for r in fam_rows if r["bucket"] == b]
        if not rs: continue
        by_bucket[b] = {"families": len(rs),
                        "median_jsq_excess_pct": round(med(r["jsq_excess_pct"] for r in rs), 3),
                        "max_jsq_excess_pct": round(max(r["jsq_excess_pct"] for r in rs), 3),
                        "median_least_token_excess_pct": round(med(r["least_token_excess_pct"] for r in rs), 3),
                        "max_least_token_excess_pct": round(max(r["least_token_excess_pct"] for r in rs), 3)}
    summary = {"families": len(fam_rows), "forks": len(fork_rows), "seeds": seeds, "gpus": GPUS,
               "forks_by_proof": dict(collections.Counter(r["proof"] for r in fork_rows)),
               "families_optimal_equals_lower_bound": sum(r["forks_not_at_bound"] == 0 for r in fam_rows),
               "max_optimal_minus_lower_bound_s": max(round(r["optimal_s"] - r["optimal_lower_bound_s"], 3) for r in fam_rows),
               "max_optimal_spread_s": max(r["optimal_spread_s"] for r in fam_rows),
               "by_bucket": by_bucket}
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    print(json.dumps(summary, indent=1))


def verify(score, n):
    """Re-simulate n random cached subsets from scratch and compare with the cache."""
    items = random.Random(0).sample(sorted(score.cache), min(n, len(score.cache)))
    fresh = []
    size = math.ceil(len(items) / score.jobs)
    with cf.ThreadPoolExecutor(score.jobs) as ex:
        for vals in ex.map(score.simulate, [items[i:i + size] for i in range(0, len(items), size)]):
            fresh += vals
    diff = max(abs(a - score.cache[k]) for k, a in zip(items, fresh))
    print(f"verify: {len(items)} cached subsets re-simulated, max |difference| = {diff:.6f} s")
    assert diff < 1e-6


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dataset", choices=sorted(DATASETS), default="commit0")
    ap.add_argument("--work", type=pathlib.Path, help="default: work/ (commit0), work/oolong/ (oolong)")
    ap.add_argument("--out", type=pathlib.Path, help="default: data/ (commit0), data/oolong/ (oolong)")
    ap.add_argument("--seeds", type=int, nargs="+", default=[42, 43, 44, 45, 46])
    mem_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    ap.add_argument("--jobs", type=int, default=max(1, min((os.cpu_count() or 2) // 3, int(mem_gb // 4))),
                    help="parallel single-replica subset batches (each needs up to ~4 GB RAM)")
    ap.add_argument("--vacuum-jobs", type=int, default=3, help="parallel 8-replica runs (each needs ~5 GB RAM)")
    ap.add_argument("--only", metavar="REGEX", help="only families whose id matches (needs a fresh --work)")
    ap.add_argument("--verify", type=int, default=0, metavar="N", help="also re-simulate N cached subsets")
    a = ap.parse_args()
    global DS
    DS = DATASETS[a.dataset]
    a.work = a.work or DS["work"]; a.out = a.out or DS["out"]
    for v in ("BLIS_CATALOG", "BLIS_REGISTRY"):
        if not os.environ.get(v): sys.exit(f"{v} is not set (see README.md)")
    a.work.mkdir(parents=True, exist_ok=True)
    bucket = build_families(a.work, a.only)
    families = {json.loads(l)["id"]: json.loads(l) for l in open(a.work / "families.jsonl")}
    forks = {}
    for fid, fam in families.items():
        child, root, by_spawn = forks_of(fam)
        for sp, kids in by_spawn.items():
            forks[(fid, sp)] = {"kids": kids, "child": child, "spawn_end": root[sp]["t"] + root[sp]["api_time"]}
    log(f"{len(forks)} forks across {len(families)} families")
    runs = vacuum_runs(a.work, a.seeds, a.vacuum_jobs)
    score = Scorer(a.work, families, forks, a.jobs)
    bound, best, bins, proof = search(score, forks)
    if a.verify: verify(score, a.verify)
    report(a.out, bucket, forks, bound, best, bins, proof, runs)


if __name__ == "__main__":
    main()
