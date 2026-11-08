from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path
import tempfile
import unittest

import nbformat
import numpy as np
import pandas as pd

from research.analysis.report import notebook, write_report
from research.analysis.plots import plot_server_series
from research.analysis.semiclosed import load_run, paired_families, run_summary, ttft_statistics, values


class AnalysisTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def save(self, name, metrics, workload=None):
        path = self.root / name
        path.mkdir()
        (path / 'metrics.json').write_text(json.dumps(metrics))
        if workload is not None:
            (path / 'workload.json').write_text(json.dumps(workload))
        return load_run(name, path)

    def metrics(self):
        return {'preemption_count': 0, 'requests': [
            {'requestID': 'a', 'arrived_at': 0, 'completion_index': 1, 'ttft_ms': 10,
             'scheduling_delay_ms': 9, 'e2e_ms': 100, 'num_prefill_tokens': 20, 'output_tokens': 2},
            {'requestID': 'b', 'arrived_at': 1, 'completion_index': 2, 'ttft_ms': 20,
             'scheduling_delay_ms': 1, 'e2e_ms': 200, 'num_prefill_tokens': 30, 'output_tokens': 3},
        ]}

    def workload(self):
        return {'format_version': 1, 'complete': True, 'accounting_match': True, 'end_us': 1_200_000,
                'source': {'selection': {'model': 'trace-model'}, 'converter': {'source_sha256': 'converter-code'},
                           'conversion': {'prefix_mapping': 'fixed-mapping'},
                           'source': {'dataset': 'fixture', 'weeks': [{'week': 'f', 'files': {
                               name: {'sha256': name} for name in ('requests.parquet', 'sessions.parquet', 'tool_calls.parquet')}}]}},
                'requests': [
                    {'request_id': 'a', 'trace_id': 'f', 'session_id': 's', 'step': 0, 'start_ms': 10000,
                     'end_ms': 10100, 'prompt_tokens': 20, 'output_tokens': 2, 'delay_us': 0,
                     'prerequisites': [], 'state': 'completed', 'arrival_us': 0, 'completion_us': 100000, 'e2e_us': 100000},
                    {'request_id': 'b', 'trace_id': 'f', 'session_id': 's', 'step': 1, 'start_ms': 11000,
                     'end_ms': 11200, 'prompt_tokens': 30, 'output_tokens': 3, 'delay_us': 900000,
                     'prerequisites': [{'request_id': 'a', 'on': 'completion'}], 'state': 'completed',
                     'arrival_us': 1000000, 'completion_us': 1200000, 'e2e_us': 200000},
                ],
                'families': [{'trace_id': 'f', 'arrival_us': 0, 'completion_us': 1200000,
                              'makespan_us': 1200000, 'states': {'completed': 2}}],
                'sessions': [{'trace_id': 'f', 'session_id': 's', 'arrival_us': 0,
                              'completion_us': 1200000, 'states': {'completed': 2}}]}

    def test_quantile_attribution_sums_to_total_not_component_quantiles(self):
        run = self.save('ranked', self.metrics())
        rows = ttft_statistics(run).set_index('statistic')
        self.assertAlmostEqual(rows.loc['p99', 'total_ms'], 19.9)
        self.assertAlmostEqual(rows.loc['p99', 'scheduling_ms'], 1.08)
        self.assertAlmostEqual(rows.loc['p99', 'remaining_ms'], 18.82)
        independent = np.quantile([9, 1], .99) + np.quantile([1, 19], .99)
        self.assertNotAlmostEqual(rows.loc['p99', 'total_ms'], independent)
        for row in rows.itertuples():
            self.assertAlmostEqual(row.total_ms, row.scheduling_ms + row.remaining_ms)
        self.assertAlmostEqual(rows.loc['mean', 'scheduling_ms'], 5)

    def test_tied_totals_use_stable_request_identity(self):
        data = self.metrics()
        data['requests'][1]['ttft_ms'] = 10
        first = self.save('same', data)
        data['requests'].reverse()
        second = self.save('reversed', data)
        a, b = ttft_statistics(first), ttft_statistics(second)
        pd.testing.assert_frame_equal(a.drop(columns='run'), b.drop(columns='run'))

    def test_preemption_or_invalid_delay_never_creates_a_fake_split(self):
        for count in (None, 1, -1, False, '0'):
            data = self.metrics()
            data['preemption_count'] = count
            run = self.save(f'count-{count}', data)
            result = ttft_statistics(run)
            self.assertFalse(result['decomposed'].any())
            self.assertTrue(result['scheduling_ms'].isna().all())
            self.assertEqual(result.loc[0, 'total_ms'], 15)
        for label, value in (('missing', None), ('negative', -1), ('late', 11)):
            data = self.metrics()
            data['requests'][0]['scheduling_delay_ms'] = value
            self.assertFalse(ttft_statistics(self.save(label, data))['decomposed'].any())
        data = self.metrics()
        del data['preemption_count']
        self.assertFalse(ttft_statistics(self.save('unknown', data))['decomposed'].any())

    def test_population_retains_completed_zero_output_but_excludes_warmup_and_unfinished(self):
        data = self.metrics()
        data['requests'][0]['output_tokens'] = 0
        data['requests'][1]['phase'] = 'warmup'
        data['requests'].append({'requestID': 'pending', 'arrived_at': 2, 'ttft_ms': 0, 'e2e_ms': 0})
        data['measured_window_source'] = 'explicit'
        data['e2e_mean_ms'] = 99
        run = self.save('population', data)
        self.assertEqual(values(run, 'ttft_ms').tolist(), [10])
        self.assertEqual(values(run, 'engine_e2e_ms').tolist(), [100])
        summary = run_summary(run)
        self.assertEqual(summary['zero_output_rows'], 1)
        self.assertEqual(summary['warmup_rows'], 1)
        self.assertEqual(summary['completed_nonwarmup_rows'], 1)
        self.assertTrue(any('window' in notice for notice in run.notices))
        self.assertEqual(summary['emitted_aggregates']['e2e_mean_ms'], 99)

    def test_workload_status_overrules_engine_rows_and_preserves_zero_timestamp(self):
        work = self.workload()
        work['requests'][1].update(state='censored', completion_us=None, e2e_us=None)
        work['families'][0].update(states={'completed': 1, 'censored': 1}, completion_us=None, makespan_us=None)
        work['sessions'][0].update(states={'completed': 1, 'censored': 1}, completion_us=None)
        work.update(complete=False, accounting_match=False)
        run = self.save('unfinished', self.metrics(), work)
        self.assertEqual(values(run, 'ttft_ms').tolist(), [10])
        self.assertEqual(run.families.iloc[0]['arrival_s'], 0)
        self.assertTrue(pd.isna(run.families.iloc[0]['makespan_s']))
        self.assertTrue(pd.isna(run.sessions.iloc[0]['lifetime_s']))
        self.assertEqual(run_summary(run)['family_makespan_s']['count'], 0)

    def test_pairing_requires_matching_work_and_complete_families(self):
        base = self.save('baseline', self.metrics(), self.workload())
        changed = self.workload()
        changed['families'][0].update(completion_us=2400000, makespan_us=2400000)
        changed['sessions'][0]['completion_us'] = 2400000
        changed['requests'][1].update(completion_us=2400000, e2e_us=1400000)
        candidate = self.save('policy', self.metrics(), changed)
        pairs, coverage = paired_families([base, candidate])
        self.assertEqual(pairs.iloc[0]['change_percent'], 100)
        self.assertEqual(coverage.iloc[0]['paired'], 1)
        for key, new_value in (('prompt_tokens', 999), ('delay_us', 1), ('prerequisites', [])):
            changed = self.workload()
            changed['requests'][1][key] = new_value
            bad = self.save(key, self.metrics(), changed)
            pairs, coverage = paired_families([base, bad])
            self.assertTrue(pairs.empty)
            self.assertEqual(coverage.iloc[0]['incompatible_work'], 1)
        unfinished = self.workload()
        unfinished.update(complete=False, accounting_match=False)
        unfinished['requests'][1].update(state='censored', completion_us=None)
        unfinished['families'][0].update(completion_us=None, makespan_us=None, states={'completed': 1, 'censored': 1})
        unfinished['sessions'][0].update(completion_us=None, states={'completed': 1, 'censored': 1})
        pairs, coverage = paired_families([base, self.save('incomplete', self.metrics(), unfinished)])
        self.assertTrue(pairs.empty)
        self.assertEqual(coverage.iloc[0]['incomplete'], 1)

    def test_pairing_rejects_different_source_model_prefix_evidence_or_missing_provenance(self):
        baseline = self.save('base', self.metrics(), self.workload())
        changes = [('model', lambda w: w['source']['selection'].update(model='other-model')),
                   ('prefix', lambda w: w['source']['source']['weeks'][0]['files']['requests.parquet'].update(sha256='other')),
                   ('mapping', lambda w: w['source']['conversion'].update(prefix_mapping='different'))]
        for label, change in changes:
            work = self.workload()
            change(work)
            pairs, coverage = paired_families([baseline, self.save(label, self.metrics(), work)])
            self.assertTrue(pairs.empty)
            self.assertEqual(coverage.iloc[0]['incompatible_work'], 1)
        work = self.workload()
        del work['source']
        pairs, coverage = paired_families([baseline, self.save('unknown', self.metrics(), work)])
        self.assertTrue(pairs.empty)
        self.assertEqual(coverage.iloc[0]['insufficient_provenance'], 1)

    def test_summary_cannot_contradict_member_states_or_completion_times(self):
        work = self.workload()
        work['requests'][1]['state'] = 'censored'
        work['complete'] = False
        with self.assertRaisesRegex(ValueError, 'states contradict'):
            self.save('bad-state', self.metrics(), work)
        work = self.workload()
        work['families'][0].update(completion_us=600000, makespan_us=600000)
        with self.assertRaisesRegex(ValueError, 'lifecycle contradicts'):
            self.save('bad-completion', self.metrics(), work)
        work = self.workload()
        work['sessions'] = []
        with self.assertRaisesRegex(ValueError, 'cover every request'):
            self.save('missing-session', self.metrics(), work)

    def test_server_series_draws_the_entire_final_bin(self):
        import matplotlib.pyplot as plt
        for count in (1, 3):
            metrics = self.metrics()
            metrics['server_series'] = {'interval_s': 10, 'engines': [{
                'id': 'engine-1', 'bins': count, 'num_requests_waiting': [5] * count,
                'num_requests_running': [3] * count, 'kv_cache_usage': [.6] * count}]}
            figure = plot_server_series(self.save(f'bins-{count}', metrics))
            self.addCleanup(plt.close, figure)
            self.assertEqual(figure.axes[0].lines[0].get_xdata()[-1], 10 * count)

    def test_missing_work_and_inconsistent_identity_fail_clearly(self):
        data = self.metrics()
        data['requests'].append(deepcopy(data['requests'][0]))
        with self.assertRaisesRegex(ValueError, 'duplicate requestID'):
            self.save('duplicate', data)
        work = self.workload()
        work['requests'].pop()
        with self.assertRaisesRegex(ValueError, 'absent from workload'):
            self.save('extra', self.metrics(), work)
        work = self.workload()
        work['families'][0]['makespan_us'] = 1
        with self.assertRaisesRegex(ValueError, 'inconsistent.*makespan'):
            self.save('wrong-span', self.metrics(), work)

    def test_report_exports_notebook_tables_and_figures_without_changing_inputs(self):
        run = self.save('input with spaces', self.metrics(), self.workload())
        before = {p.name: p.read_bytes() for p in run.path.iterdir()}
        output = self.root / 'report'
        report = write_report({'base': run.path}, output)
        self.assertEqual(report['runs'][0]['ttft_ms']['mean'], 15)
        for name in ('summary.json', 'requests.csv', 'families.csv', 'ttft_statistics.csv',
                     'report.ipynb', 'ttft_statistics.png', 'ttft_statistics.pdf'):
            self.assertTrue((output / name).is_file(), name)
        doc = nbformat.read(output / 'report.ipynb', as_version=4)
        nbformat.validate(doc)
        self.assertTrue(any(cell.get('outputs') for cell in doc.cells))
        self.assertEqual(before, {p.name: p.read_bytes() for p in run.path.iterdir()})
        with self.assertRaisesRegex(ValueError, 'already exists'):
            write_report({'base': run.path}, output)

    def test_metrics_only_and_empty_rows_do_not_invent_makespans(self):
        run = self.save('empty', {'requests': [], 'preemption_count': 0})
        self.assertTrue(run.families.empty)
        self.assertEqual(run_summary(run)['ttft_ms']['count'], 0)
        report = write_report({'empty': run.path}, self.root / 'empty-report')
        self.assertEqual(report['runs'][0]['complete_families'], 0)
        self.assertFalse((self.root / 'empty-report/agent_lifetimes_ecdf.png').exists())
        nbformat.validate(notebook({'empty': run.path}))


if __name__ == '__main__':
    unittest.main()
