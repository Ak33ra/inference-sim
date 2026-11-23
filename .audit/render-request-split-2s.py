"""Render the two-second result and the matched four-second context."""
import csv
import json
from pathlib import Path
import statistics
import sys

study=Path(sys.argv[1])
report=Path(sys.argv[2])
base=Path('research/outputs/mega-request-split-4s')
current=json.loads((study/'analysis/summary.json').read_text())
previous=json.loads((base/'analysis/summary.json').read_text())
audit=json.loads((study/'independent-numerical-audit.json').read_text())
old_audit=json.loads((base/'independent-numerical-audit.json').read_text())
assert audit['reported_csv_matches'] and audit['checked_runs']==10
index={(r['router'],r['unit'],r['metric'],r['statistic']):r for r in current['aggregate']}
changes={(r['unit'],r['metric'],r['statistic']):r for r in current['paired_percent_changes']}
rows=[('Session elapsed (s)','session','elapsed_us'),('Session slowdown','session','slowdown'),
      ('Family makespan (s)','family','elapsed_us'),('Family slowdown','family','slowdown')]

def cell(router,unit,metric):
 scale,digits=(1e6,2) if metric=='elapsed_us' else (1,3)
 return ' / '.join(f"{index[(router,unit,metric,s)]['median']/scale:.{digits}f}" for s in ['mean','p50','p99'])

def delta(unit,metric,stat):
 r=changes[(unit,metric,stat)]
 return f"{r['median']:+.2f} [{r['min']:+.2f}, {r['max']:+.2f}]"

def diagnostic(data,router,field):
 return statistics.median(r[field] for r in data['diagnostics'] if r['router']==router)

family=changes[('family','elapsed_us','mean')]
session=changes[('session','elapsed_us','mean')]
old_family=next(r for r in previous['paired_percent_changes'] if (r['unit'],r['metric'],r['statistic'])==('family','elapsed_us','mean'))
lines=[
f'**Request SPLIT at 2-second arrivals.** Median paired mean family makespan changes by **{family["median"]:+.2f}%** versus JSQ/FCFS, with a five-seed range of {family["min"]:+.2f}% to {family["max"]:+.2f}%. Mean session elapsed time changes by **{session["median"]:+.2f}%**.',
f'The mean-family gap shrinks from {old_family["median"]:+.2f}% at 4-second arrivals to {family["median"]:+.2f}% at 2 seconds. JSQ has the lower mean family makespan in all five new seed pairs.',
'',
'This repeats the existing comparison with the fully aggregated mega trace. It includes all eight Commit0 fanout buckets and natural Oolong, excludes FreeInference, and contains **287 families, 2,385 sessions and 58,741 requests**. Each source appears once.',
'',
'The cluster has eight replicas, each with one B200 running GPT-OSS-120B MXFP4 at TP=1. Prefix caching is off. Native limits remain 16,384 batched tokens, 1,024 sequences, 131,072 model tokens and 85% GPU memory utilization.',
'',
'The only workload change is a **2-second mean Poisson family-root interarrival**, or 0.5 families/s. The first root arrives at zero. Family order, arrival seeds 42–46 and simulator seed 42 are unchanged. Fresh root schedules match exactly between policies within each seed. Compared with the 4-second runs, root birth times halve to microsecond rounding. Tool delays and DAG dependencies remain unchanged, and every family drains.',
'',
'SPLIT uses the same shared ready queue, seven replicas taking the shortest request and one taking the longest. Requests finish without preemption and retain native batching. Request size uses the unchanged isolated-service oracle for exact input/output lengths. Both policies use the exact frozen binary and slowdown references from the 4-second study.',
'',
'Each cell is **mean / median / p99**. Each number is the median of that statistic across five runs. Elapsed times are seconds.',
'',
'| Metric | JSQ/FCFS | Request SPLIT |',
'|---|---:|---:|',
]
for label,unit,metric in rows:
 lines.append(f'| {label} | {cell("jsq",unit,metric)} | {cell("request-split",unit,metric)} |')
lines += ['', 'Paired changes are **median [minimum, maximum] percent change** across seeds. Positive means slower. These ranges describe seed variation, not confidence intervals. Paired changes need not equal ratios of the displayed cross-seed medians.', '',
          '| Metric | Mean change (%) | Median change (%) | p99 change (%) |', '|---|---:|---:|---:|']
for label,unit,metric in rows:
 lines.append('| '+label+' | '+' | '.join(delta(unit,metric,s) for s in ['mean','p50','p99'])+' |')
lines += ['', 'Slowdown divides each object’s elapsed time by the same saved best-observed isolated-family reference used before. Sibling sessions compete in those references and tool delays remain included. These are distinct from the request-size oracle and are not proven session-alone optima.', '',
          'The following diagnostics show medians of five per-run request means, or admission fractions. GPU admission wait excludes preparation service and its queue.', '',
          '| Diagnostic | JSQ 4s | JSQ 2s | SPLIT 4s | SPLIT 2s |', '|---|---:|---:|---:|---:|']
for label,field in [('Request latency (ms)','mean_request_e2e_ms'),('TTFT (ms)','mean_ttft_ms'),('First token to completion (ms)','mean_decode_span_ms'),('Preparation wait (ms)','mean_preparation_wait_ms')]:
 values=[diagnostic(a,r,field) for a,r in [(old_audit,'jsq'),(audit,'jsq'),(old_audit,'request-split'),(audit,'request-split')]]
 lines.append('| '+label+' | '+' | '.join(f'{v:.2f}' for v in values)+' |')
lines += [f'| GPU admission wait (ms) | {diagnostic(old_audit,"jsq","mean_engine_wait_ms"):.3f} | {diagnostic(audit,"jsq","mean_engine_wait_ms"):.3f} | {diagnostic(old_audit,"request-split","mean_central_wait_ms"):.3f} | {diagnostic(audit,"request-split","mean_central_wait_ms"):.3f} |',
 f'| Multiple ready requests at admission (%) | n/a | n/a | {100*diagnostic(old_audit,"request-split","fraction_claims_with_multiple_ready"):.2f} | {100*diagnostic(audit,"request-split","fraction_claims_with_multiple_ready"):.2f} |',
 '',
'As in the earlier comparison, SPLIT sends preparation round-robin before GPU assignment, while JSQ couples preparation and GPU placement. This experiment measures the complete policies, including central dispatch and batching. It does not isolate priority ranking.',
 '',
'Every run completes all **722,128,759 input tokens and 10,121,700 output tokens** with matching physical engine totals. All **60,551 dependency edges** and recorded delays are preserved. No requests fail, time out, drop, hit length caps or preempt. SPLIT admission priorities, renderer timing, native capacity and final KV/reservation drain are audited. Independent calculations match 120 per-run statistics and 60 paired changes. The new configs differ from the 4-second configs only in arrival rate and output directory.',
 '',
'- [Study/configurations and frozen provenance](../../outputs/mega-request-split-2s/study.json).',
'- Plotting data: [sessions](../../outputs/mega-request-split-2s/analysis/sessions.csv), [families](../../outputs/mega-request-split-2s/analysis/families.csv), [requests](../../outputs/mega-request-split-2s/analysis/requests.csv), [paired changes](../../outputs/mega-request-split-2s/analysis/paired-deltas.csv), [per-run statistics at both arrival rates](../../outputs/mega-request-split-2s/analysis/comparison-by-arrival.csv).',
'- [Independent numerical and arrival audit](../../outputs/mega-request-split-2s/independent-numerical-audit.json) and [previous 4-second report](../mega-request-split-4s/README.md).',
 '',
'Rerun or reuse the verified experiments and regenerate the tables from the repository root:',
 '',
'```bash',
'python3 -B research/scripts/request_split_arrivals.py run research/outputs/mega-request-split-2s --jobs 4',
'python3 -B research/scripts/request_split_arrivals.py analyze research/outputs/mega-request-split-2s',
'python3 -B research/outputs/mega-request-split-2s/review/check_numerical_results.py research/outputs/mega-request-split-2s',
'```',
 '',
'Raw outputs live in `/scratch/akira/research-outputs/mega-request-split-2s`, linked from `research/outputs/mega-request-split-2s`.',
]
combined=[]
for gap,path in [(4,base),(2,study)]:
 for row in csv.DictReader((path/'analysis/per-run.csv').open()):
  combined.append({'mean_interarrival_s':gap,**row})
with (study/'analysis/comparison-by-arrival.csv').open('w') as f:
 w=csv.DictWriter(f,fieldnames=list(combined[0]));w.writeheader();w.writerows(combined)
report.parent.mkdir(parents=True,exist_ok=True)
report.write_text('\n'.join(lines)+'\n')
print(report)
