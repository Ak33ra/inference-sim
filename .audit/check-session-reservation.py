"""Independent raw-output audit; does not import the experiment implementation."""

import collections
import csv
import json
import math
from pathlib import Path
import statistics
import sys


def read(path):
    return json.loads(Path(path).read_text())


def quantile(values, probability):
    ordered = sorted(values)
    index = (len(ordered) - 1) * probability
    low, high = math.floor(index), math.ceil(index)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


study = Path(sys.argv[1]).resolve()
manifest = read(study / 'study.json')
oracle = read(Path(__file__).with_name('session-reservation-reference-check.json'))
refs = {row['runtime_session_id']: row for row in oracle['sessions']}
classes = read(manifest['session_class_manifest'])
assert len(refs) == len(classes['sessions']) == 2385
for key, row in refs.items():
    actual = classes['sessions'][key]
    assert (row['class'], row['reference_us'], row['reference_run']) == (
        actual['class'], actual['reference_elapsed_us'], actual['reference_run'])

calculated, evidence = [], []
for run in manifest['runs']:
    output = Path(run['output'])
    work, metrics = read(output / 'workload.json'), read(output / 'metrics.json')
    assert work['complete'] and work['accounting_match']
    assert len(work['sessions']) == 2385 and len(work['families']) == 287
    requests = {r['request_id']: r for r in work['requests']}
    engine = {r['requestID']: r for r in metrics['requests']}
    decisions = {r['request_id']: r for r in read(output / 'router-decisions.json')['decisions']}
    assert len(requests) == len(engine) == len(decisions) == 58741
    assert requests.keys() == engine.keys() == decisions.keys()
    assert sum(r['prompt_tokens'] for r in requests.values()) == metrics['total_input_tokens'] == 722128759
    assert sum(r['output_tokens'] for r in requests.values()) == metrics['total_output_tokens'] == 10121700
    assert sum(len(r['prerequisites']) for r in requests.values()) == 60551
    for field in ('still_queued', 'still_running', 'dropped_unservable', 'timed_out_requests',
                  'cancelled_requests', 'context_overflow_requests', 'failed_requests'):
        assert metrics[field] == 0, (run['id'], field, metrics[field])
    counts = collections.Counter()
    for key, row in requests.items():
        session = row['trace_id'] + '/' + row['session_id']
        cohort = refs[session]['class']
        endpoint = engine[key]['handled_by']
        assert row['state'] == 'completed'
        assert engine[key]['session_id'] == session
        assert decisions[key]['instance'] == endpoint
        if run['router'] == 'session-reservation':
            assert decisions[key]['session_id'] == session
            assert decisions[key]['session_class'] == cohort
            assert cohort == 'short' or endpoint not in ('instance_0', 'instance_1')
        counts[(cohort, endpoint)] += 1
    groups = collections.defaultdict(list)
    for row in work['sessions']:
        reference = refs[row['trace_id'] + '/' + row['session_id']]
        slowdown = (row['completion_us'] - row['arrival_us']) / reference['reference_us']
        groups[('session', 'all')].append(slowdown)
        groups[('session', reference['class'])].append(slowdown)
    assert len(groups[('session', 'short')]) == 954 and len(groups[('session', 'long')]) == 1431
    for row in work['families']:
        groups[('family', 'all')].append(row['makespan_us'] / oracle['families'][row['trace_id']]['reference_us'])
    for (unit, cohort), values in groups.items():
        for stat, value in [('mean', statistics.mean(values)), ('p50', quantile(values, .5)),
                            ('p99', quantile(values, .99))]:
            calculated.append(dict(run_id=run['id'], router=run['router'], arrival_seed=run['arrival_seed'],
                                   unit=unit, cohort=cohort, statistic=stat, value=value))
    evidence.append(dict(run_id=run['id'], placements={f'{a}/{b}': n for (a,b),n in sorted(counts.items())},
                         below_one_sessions=sum(v < 1 for v in groups[('session', 'all')]),
                         end_us=work['end_us']))

with (study / 'analysis/per-run.csv').open() as stream:
    reported = {(r['run_id'], r['unit'], r['cohort'], r['statistic']): float(r['value'])
                for r in csv.DictReader(stream)}
assert len(reported) == len(calculated) == 120
for row in calculated:
    key = tuple(row[k] for k in ('run_id', 'unit', 'cohort', 'statistic'))
    assert math.isclose(row['value'], reported[key], rel_tol=1e-12), (key, row['value'], reported[key])

summary = read(study / 'analysis/summary.json')
for row in summary['aggregate']:
    values = [r['value'] for r in calculated if all(r[k] == row[k] for k in ('router','unit','cohort','statistic'))]
    assert len(values) == 5
    for name, value in [('median', statistics.median(values)), ('min', min(values)), ('max', max(values))]:
        assert math.isclose(row[name], value, rel_tol=1e-12)
for row in summary['paired_percent_changes']:
    values = []
    for seed in range(42,47):
        pair = {r['router']: r['value'] for r in calculated if r['arrival_seed'] == seed and
                all(r[k] == row[k] for k in ('unit','cohort','statistic'))}
        values.append((pair['session-reservation'] / pair['jsq'] - 1) * 100)
    for name, value in [('median', statistics.median(values)), ('min', min(values)), ('max', max(values))]:
        assert math.isclose(row[name], value, rel_tol=1e-12, abs_tol=1e-12)

audit = {'verified': True, 'runs': len(evidence), 'session_reference_matches': len(refs),
         'per_run_statistics_matched': len(calculated), 'evidence': evidence}
(study / 'analysis/independent-audit.json').write_text(json.dumps(audit, indent=2) + '\n')
print(json.dumps({k:v for k,v in audit.items() if k != 'evidence'}))
