"""Independently recompute full-work and paired session/family statistics."""

import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def read(path):
    return json.loads(path.read_text())


def quantile(values, probability):
    values = sorted(values)
    position = (len(values) - 1) * probability
    lower = int(position)
    return values[lower] + (values[min(lower + 1, len(values) - 1)] - values[lower]) * (position - lower)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", type=Path)
    parser.add_argument("--references", type=Path, default=Path(
        "research/outputs/mega-session-reservation-4s/provenance/session-classes.json"))
    args = parser.parse_args()
    study = read(args.study / "study.json")
    references = read(args.references)
    expected_totals = dict(families=287, sessions=2385, requests=58741,
                           prompt_tokens=722128759, output_tokens=10121700,
                           zero_output_requests=0)
    results, schedules, diagnostics, services = [], {}, [], {}
    for run in study["runs"]:
        output = Path(run["output"])
        workload, metrics = read(output / "workload.json"), read(output / "metrics.json")
        assert workload["complete"] and workload["accounting_match"], run["id"]
        assert workload["selected"] == expected_totals, run["id"]
        assert metrics["completed_requests"] == 58741, run["id"]
        assert metrics["total_input_tokens"] == 722128759, run["id"]
        assert metrics["total_output_tokens"] == 10121700, run["id"]
        engines = metrics["server_series"]["engines"]
        assert sum(sum(e["prompt_tokens_local_compute"]) for e in engines) == 722128759
        assert sum(sum(e["prompt_tokens_local_cache_hit"]) for e in engines) == 0
        assert sum(sum(e["generation_tokens"]) for e in engines) == 10121700
        assert all(metrics[key] == 0 for key in (
            "still_queued", "still_running", "dropped_unservable", "length_capped_requests",
            "timed_out_requests", "preemption_count", "failed_requests", "cancelled_requests",
            "context_overflow_requests")), run["id"]
        requests = {row["request_id"]: row for row in workload["requests"]}
        assert len(requests) == 58741
        assert sum(len(row["prerequisites"]) for row in requests.values()) == 60551
        for row in requests.values():
            parents = row["prerequisites"]
            assert all(parent["on"] == "completion" for parent in parents)
            if parents:
                assert row["arrival_us"] == max(
                    requests[parent["request_id"]]["completion_us"] for parent in parents
                ) + row["delay_us"]
        schedule = sorted((r["trace_id"], r["birth_us"], r["arrival_us"]) for r in workload["families"])
        seed = run["arrival_seed"]
        assert seed not in schedules or schedules[seed] == schedule
        schedules[seed] = schedule
        native = metrics["requests"]
        row = dict(run_id=run["id"], router=run["router"], arrival_seed=seed,
                   mean_request_scheduling_delay_ms=statistics.mean(r["scheduling_delay_ms"] for r in native),
                   mean_ttft_ms=statistics.mean(r["ttft_ms"] for r in native),
                   mean_request_e2e_ms=statistics.mean(r["e2e_ms"] for r in native),
                   mean_decode_span_ms=statistics.mean(r["e2e_ms"] - r["ttft_ms"] for r in native),
                   request_weighted_itl_mean_ms=statistics.mean(r["itl_ms"] for r in native),
                   inflight_max=metrics["in_flight_requests_max"],
                   sampled_peak_kv_fraction=max(max(e["kv_cache_usage"]) for e in metrics["server_series"]["engines"]))
        if run["router"] == "session-split":
            audit = read(output / "router-decisions.json")
            claims = [e for e in audit["events"] if e["kind"] == "claim"]
            preparations = [e for e in audit["events"] if e["kind"] == "prepare"]
            assert len(preparations) == 58741
            for event in preparations:
                ident, service = event["request_id"], event["preparation_service_us"]
                assert ident not in services or services[ident] == service
                services[ident] = service
            assert len(claims) == 58741
            waits = [(e["at_us"] - e["prepared_us"]) / 1000 for e in claims]
            row.update(mean_central_wait_ms=statistics.mean(waits), p99_central_wait_ms=quantile(waits, .99),
                       max_central_wait_ms=max(waits), max_ready_queue=max(e["ready_count"] for e in audit["events"]),
                       fraction_claims_with_multiple_ready=statistics.mean(e["ready_count"] > 1 for e in claims),
                       max_batch_at_admission=max(e["capacity"]["batch_size"] for e in claims),
                       admission_refusals=sum(s["admission_refusals"] for s in audit["reservations"].values()),
                       max_promised_fraction=max(e["capacity"]["promised_kv_blocks"] / e["capacity"]["total_kv_blocks"] for e in claims))
            row.update(mean_preparation_service_ms=statistics.mean(e["preparation_service_us"] / 1000 for e in preparations),
                       mean_preparation_wait_ms=statistics.mean(e["preparation_wait_us"] / 1000 for e in preparations))
            assert math.isclose(row["mean_request_scheduling_delay_ms"],
                row["mean_preparation_service_ms"] + row["mean_preparation_wait_ms"] + row["mean_central_wait_ms"],
                rel_tol=1e-12)
            row["by_instance"] = []
            for instance in sorted({e["instance"] for e in claims}):
                subset = [e for e in claims if e["instance"] == instance]
                row["by_instance"].append(dict(instance=instance, requests=len(subset),
                    prompt_tokens=sum(requests[e["request_id"]]["prompt_tokens"] for e in subset),
                    output_tokens=sum(requests[e["request_id"]]["output_tokens"] for e in subset),
                    mean_remaining_s=statistics.mean(e["remaining_us"] / 1e6 for e in subset)))
        diagnostics.append(row)
        for unit, source in (("session", "sessions"), ("family", "families")):
            objects = workload[source]
            keys = [r["trace_id"] + "/" + r["session_id"] if unit == "session" else r["trace_id"]
                    for r in objects]
            assert len(set(keys)) == len(keys) and set(keys) == set(references[source])
            elapsed = [(r["completion_us"] - r["arrival_us"]) if unit == "session" else r["makespan_us"]
                       for r in objects]
            slowdown = [value / references[source][key]["reference_elapsed_us"]
                        for key, value in zip(keys, elapsed)]
            for metric, values in (("elapsed_s", [v / 1e6 for v in elapsed]), ("slowdown", slowdown)):
                for name, value in (("mean", statistics.mean(values)),
                                    ("p50", quantile(values, .5)), ("p99", quantile(values, .99))):
                    results.append(dict(run_id=run["id"], router=run["router"], arrival_seed=seed,
                                        unit=unit, metric=metric, statistic=name, value=value))
    assert len(results) == 120 and len(schedules) == 5
    for run in study["runs"]:
        if run["router"] != "jsq":
            continue
        output = Path(run["output"])
        audit, metrics = read(output / "router-decisions.json"), read(output / "metrics.json")
        assert audit["config"]["LatencyUs"] == 0
        native = {r["requestID"]: r for r in metrics["requests"]}
        free, waits, engine_waits = defaultdict(int), [], []
        for event in audit["decisions"]:
            ident, endpoint, reached = event["request_id"], event["instance"], event["at_us"]
            assert endpoint == native[ident]["handled_by"]
            assert math.isclose(reached / 1e6, native[ident]["arrived_at"], abs_tol=1e-9)
            start = max(reached, free[endpoint])
            free[endpoint] = start + services[ident]
            wait = (start - reached) / 1000
            waits.append(wait)
            engine_wait = native[ident]["scheduling_delay_ms"] - services[ident] / 1000 - wait
            assert engine_wait >= -1e-8, (ident, engine_wait)
            engine_waits.append(max(engine_wait, 0))
        row = next(r for r in diagnostics if r["run_id"] == run["id"])
        row.update(mean_preparation_service_ms=statistics.mean(services.values()) / 1000,
                   mean_preparation_wait_ms=statistics.mean(waits),
                   mean_engine_wait_ms=statistics.mean(engine_waits))
    reported = list(csv.DictReader((args.study / "analysis/per-run.csv").open()))
    index = {(r["run_id"], r["unit"], r["metric"], r["statistic"]): float(r["value"]) for r in reported}
    assert len(index) == len(results)
    for row in results:
        metric = "elapsed_us" if row["metric"] == "elapsed_s" else row["metric"]
        expected = row["value"] * (1e6 if metric == "elapsed_us" else 1)
        assert math.isclose(index[(row["run_id"], row["unit"], metric, row["statistic"])], expected,
                            rel_tol=1e-12, abs_tol=1e-8), row
    raw_index = {(r['router'], r['arrival_seed'], r['unit'], r['metric'], r['statistic']): r['value'] for r in results}
    paired = list(csv.DictReader((args.study / 'analysis/paired-deltas.csv').open()))
    assert len(paired) == 60
    for row in paired:
        metric = 'elapsed_s' if row['metric'] == 'elapsed_us' else row['metric']
        key = (int(row['arrival_seed']), row['unit'], metric, row['statistic'])
        expected = 100 * (raw_index[('session-split', *key)] / raw_index[('jsq', *key)] - 1)
        assert math.isclose(float(row['percent_change']), expected, rel_tol=1e-12, abs_tol=1e-10)
    groups = defaultdict(list)
    for row in results:
        groups[tuple(row[k] for k in ("router", "unit", "metric", "statistic"))].append(row["value"])
    aggregate = [dict(zip(("router", "unit", "metric", "statistic"), key),
                      median=statistics.median(values), min=min(values), max=max(values))
                 for key, values in sorted(groups.items())]
    destination = args.study / "independent-numerical-audit.json"
    destination.write_text(json.dumps(dict(
        checked_runs=len(study["runs"]), checked_statistics=len(results), checked_paired_changes=len(paired),
        reference_sha256=hashlib.sha256(args.references.read_bytes()).hexdigest(),
        per_run=results, aggregate=aggregate, diagnostics=diagnostics,
        reported_csv_matches=True,
        preparation_method='SPLIT native audit; JSQ FIFO reconstructed from ordered dispatch decisions and matched immutable kernel service. Eight endpoints, one API server and worker each.'), indent=2) + "\n")
    print(json.dumps(aggregate, indent=2))
    print(destination)


if __name__ == "__main__":
    main()
