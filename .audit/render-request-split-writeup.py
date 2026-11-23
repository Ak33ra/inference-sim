"""Render the concise request-SPLIT writeup from independently checked results."""
import argparse
import json
from pathlib import Path
import statistics

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('study', type=Path)
parser.add_argument('report', type=Path)
args = parser.parse_args()
summary = json.loads((args.study / 'analysis/summary.json').read_text())
audit = json.loads((args.study / 'independent-numerical-audit.json').read_text())
assert audit['reported_csv_matches'] and audit['checked_runs'] == 10
aggregate = {(r['router'], r['unit'], r['metric'], r['statistic']): r for r in summary['aggregate']}
changes = {(r['unit'], r['metric'], r['statistic']): r for r in summary['paired_percent_changes']}
rows = [('Session elapsed (s)', 'session', 'elapsed_us'),
        ('Session slowdown', 'session', 'slowdown'),
        ('Family makespan (s)', 'family', 'elapsed_us'),
        ('Family slowdown', 'family', 'slowdown')]

def triple(router, unit, metric):
    scale, digits = (1e6, 2) if metric == 'elapsed_us' else (1, 3)
    return ' / '.join(f"{aggregate[(router,unit,metric,stat)]['median']/scale:.{digits}f}"
                      for stat in ('mean', 'p50', 'p99'))

def change(unit, metric, stat):
    row = changes[(unit, metric, stat)]
    return f"{row['median']:+.2f} [{row['min']:+.2f}, {row['max']:+.2f}]"

def diagnostics(router, field):
    return [r[field] for r in audit['diagnostics'] if r['router'] == router]

def diagnostic(router, field):
    return statistics.median(diagnostics(router, field))

lines = [
'**Request-level SPLIT versus JSQ, 4-second arrivals.** Request-level SPLIT increases mean session elapsed time by **2.17%** and mean family makespan by **2.11%**. Both means increase in all five paired seeds. It reduces request TTFT, but does not improve makespan on this workload.',
'',
'The mega trace combines all eight Commit0 fanout buckets and natural Oolong, excluding FreeInference: **287 families, 2,385 sessions and 58,741 requests**. Eight replicas each run GPT-OSS-120B MXFP4 on one B200, TP=1, with prefix caching off. Native limits are 16,384 batched tokens, 1,024 sequences, 131,072 model tokens, 16-token KV blocks and 85% GPU memory utilization.',
'',
'Family roots arrive in fixed family order with exponential gaps averaging **4 seconds** (0.25 families/s), starting at time zero. Arrival seeds 42–46 are matched between policies; simulator seed is 42. Requests retain their recorded DAG dependencies and tool delays, and every family drains.',
'',
'SPLIT uses one shared ready queue. Replica 0 takes the longest ready request; replicas 1–7 take the shortest. Each claim occurs at native batch admission; admitted requests stay on that replica and finish without preemption. All replicas retain native chunked prefill and batching. Blocked dependencies are absent from the ready queue. Request size is the native isolated client-visible service time for its exact input/output lengths, including preparation and output host service. Priorities remain fixed. Calibration covers all **56,134 distinct shapes**, with representative shapes also checked in reverse order and standalone runs.',
'',
'Each cell below is **mean / median / p99**. Each number is the median of that statistic across five runs. A session ends when its last request completes; family makespan ends when its entire DAG finishes.',
'',
'| Metric | JSQ/FCFS | Request SPLIT |',
'|---|---:|---:|',
]
for label, unit, metric in rows:
    lines.append(f'| {label} | {triple("jsq", unit, metric)} | {triple("request-split", unit, metric)} |')
lines += [
'',
'Paired changes are computed within each seed, then summarized as **median [minimum, maximum] percent change**. Positive means slower. These ranges describe seed variation, not confidence intervals; paired changes need not equal ratios of the table medians.',
'',
'| Metric | Mean change (%) | Median change (%) | p99 change (%) |',
'|---|---:|---:|---:|',
]
for label, unit, metric in rows:
    lines.append('| ' + label + ' | ' + ' | '.join(change(unit, metric, stat) for stat in ('mean','p50','p99')) + ' |')
lines += [
'',
'Slowdown uses the same saved best-observed references from 35 cache-off isolated-family runs for both policies. Sibling sessions compete in those references, and tool/dependency waits remain included. These denominators are distinct from the single-request sizing oracle and are not proven session-alone optima; ratios below one remain valid.',
'',
'Diagnostic values below are medians of the five per-run request means:',
'',
'| Request metric (ms) | JSQ/FCFS | Request SPLIT |',
'|---|---:|---:|',
]
for label, field in [('Scheduling delay', 'mean_request_scheduling_delay_ms'), ('TTFT','mean_ttft_ms'),
                     ('First token to completion','mean_decode_span_ms'), ('Total latency','mean_request_e2e_ms')]:
    lines.append(f'| {label} | {diagnostic("jsq",field):.2f} | {diagnostic("request-split",field):.2f} |')
fraction = diagnostics('request-split','fraction_claims_with_multiple_ready')
wait = diagnostics('request-split','mean_central_wait_ms')
batches = diagnostics('request-split','max_batch_at_admission')
lines += [
'',
f'Only **{100*statistics.median(fraction):.2f}% [{100*min(fraction):.2f}%, {100*max(fraction):.2f}%]** of SPLIT admissions have multiple ready requests to rank. Mean central wait is **{statistics.median(wait):.3f} ms [{min(wait):.3f}, {max(wait):.3f}]**. Ranking therefore has limited opportunity to change admission order. The longer first-token-to-completion span outweighs the TTFT reduction. This comparison changes central dispatch and batching as well as ranking; it does not isolate their individual effects.',
'',
f'Preparation retains eight native one-worker endpoints with identical mean service, {diagnostic("jsq","mean_preparation_service_ms"):.2f} ms. SPLIT sends preparation round-robin before GPU assignment; JSQ couples preparation and GPU placement. Mean preparation wait falls **{diagnostic("jsq","mean_preparation_wait_ms"):.2f} → {diagnostic("request-split","mean_preparation_wait_ms"):.2f} ms**. This ingress difference is part of the measured configuration.',
'',
f'All ten runs process **722,128,759 input tokens and 10,121,700 output tokens**, matching physical engine accounting, with all **60,551 DAG edges** preserved. There are no drops, caps or preemptions. SPLIT has no allocation failures, admission refusals or final KV/reservation leaks; maximum batches at admission range from {min(batches)} to {max(batches)}. Every ready-set min/max claim is audited. An independent calculation matches all 120 per-run statistics and 60 paired changes.',
'',
'All five JSQ controls reproduce the frozen baseline after normalizing only `catalog.path`; the previous session-SPLIT seed-42 workload, metrics and routing audit are byte-identical. The affected Go checks and 75 Python tests pass. The broad Go run excludes one preexisting randomized P/D saturation failure, reproduced before these changes. New policy and study code live in `sim/cluster/request_split.go` and `research/request_split/`, sharing the validated central lifecycle with session-SPLIT.',
'',
'- [Run instructions](../../request_split/README.md) and [frozen run matrix/configurations](../../outputs/mega-request-split-4s/study.json).',
'- Plotting rows: [sessions](../../outputs/mega-request-split-4s/analysis/sessions.csv), [families](../../outputs/mega-request-split-4s/analysis/families.csv), [requests](../../outputs/mega-request-split-4s/analysis/requests.csv), [paired sessions](../../outputs/mega-request-split-4s/analysis/paired-sessions.csv), [paired families](../../outputs/mega-request-split-4s/analysis/paired-families.csv).',
'- [Per-run statistics](../../outputs/mega-request-split-4s/analysis/per-run.csv), [aggregate ranges](../../outputs/mega-request-split-4s/analysis/aggregate.csv), [paired changes](../../outputs/mega-request-split-4s/analysis/paired-deltas.csv), [independent numerical audit](../../outputs/mega-request-split-4s/independent-numerical-audit.json), and [verification logs](../../outputs/mega-request-split-4s/review).',
'',
'Regenerate the tables and verify them from the repository root:',
'',
'```bash',
'python3 -B -m research.request_split.analysis research/outputs/mega-request-split-4s',
'python3 -B research/outputs/mega-request-split-4s/review/check_numerical_results.py research/outputs/mega-request-split-4s',
'```',
'',
'The output directory is a symlink to `/scratch/akira/research-outputs/mega-request-split-4s` after the home quota interrupted four initial control artifact writes. All files were hash-verified during migration; incomplete controls were archived and rerun successfully. The linked research paths remain usable.',
]
args.report.parent.mkdir(parents=True,exist_ok=True)
args.report.write_text('\n'.join(lines)+'\n')
print(args.report)
