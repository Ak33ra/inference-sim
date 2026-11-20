from copy import deepcopy
import unittest

from research.request_split import common, oracle
from research.request_split.analysis import paired_statistics, per_run_statistics, request_rows


class RequestSplitTests(unittest.TestCase):
    def test_native_chain_and_client_elapsed_service(self):
        shapes = [(100, 2), (200, 3)]
        chain = oracle.build_chain(shapes)
        self.assertEqual([r['parent_ids'] for r in chain['requests']], [[], [0]])
        self.assertTrue(all(r['t'] == r['api_time'] == 0 and r['hash_ids'] == [] for r in chain['requests']))
        rows = [{'request_id': 'oracle/0', 'step': 0, 'prompt_tokens': 100, 'output_tokens': 2,
                 'state': 'completed', 'delay_us': 0, 'arrival_us': 0, 'completion_us': 20,
                 'e2e_us': 20, 'prerequisites': []},
                {'request_id': 'oracle/1', 'step': 1, 'prompt_tokens': 200, 'output_tokens': 3,
                 'state': 'completed', 'delay_us': 0, 'arrival_us': 20, 'completion_us': 55,
                 'e2e_us': 35, 'prerequisites': [{'request_id': 'oracle/0', 'on': 'completion'}]}]
        work = {'complete': True, 'accounting_match': True, 'engine_available': True, 'requests': rows}
        metrics = dict.fromkeys(('still_queued', 'still_running', 'preemption_count', 'cache_hit_rate',
                                 'dropped_unservable', 'length_capped_requests', 'timed_out_requests',
                                 'cancelled_requests', 'context_overflow_requests', 'failed_requests'), 0)
        metrics.update(completed_requests=2, injected_requests=2, total_input_tokens=300, total_output_tokens=5,
                       requests=[{'requestID': r['request_id'], 'handled_by': 'instance_0', 'scheduling_delay_ms': .002} for r in rows],
                       server_series={'engines': [{'num_requests_running': [.5], 'num_requests_running_last': [0],
                                                   'num_requests_waiting': [0], 'num_requests_waiting_last': [0],
                                                   'kv_cache_usage_last': [0], 'prompt_tokens_local_compute': [300],
                                                   'prompt_tokens_local_cache_hit': [0], 'prompt_tokens_external_kv_transfer': [0]}]})
        result = oracle.verify_oracle(shapes, work, metrics)
        self.assertEqual(result['200/3']['service_us'], 35)
        self.assertEqual(result['200/3']['preparation_us'], 2)
        rows[1]['arrival_us'] = 19
        with self.assertRaisesRegex(ValueError, 'client-serial'):
            oracle.verify_oracle(shapes, work, metrics)
        rows[1]['arrival_us'] = 20
        metrics['preemption_count'] = 1
        with self.assertRaisesRegex(ValueError, 'accounting'):
            oracle.verify_oracle(shapes, work, metrics)

    def test_fingerprint_preserves_engine_identity(self):
        config = {'serving': {'cluster': {'replicas': 1, 'model': 'm'}, 'engine': {'cap': 16384},
                              'router': {'policy': 'jsq'}}}
        loaded = deepcopy(config)
        loaded['serving']['cluster']['replicas'] = 8
        loaded['serving']['router'] = {'policy': 'request-split'}
        self.assertEqual(oracle.engine_fingerprint(config), oracle.engine_fingerprint(loaded))
        loaded['serving']['engine']['cap'] = 8192
        self.assertNotEqual(oracle.engine_fingerprint(config), oracle.engine_fingerprint(loaded))

    def test_audit_selects_request_extremes_with_shared_session(self):
        refs = {ident: {'runtime_session_id': 'f/s', 'step': step, 'shape': f'{prompt}/2'}
                for step, (ident, prompt) in enumerate((('a', 100), ('b', 200)))}
        shapes = {f'{prompt}/2': {'input_tokens': prompt, 'output_tokens': 2, 'service_us': service, 'preparation_us': 2}
                  for prompt, service in ((100, 100), (200, 50))}
        manifest = {'requests': refs, 'shapes': shapes}
        work = {'requests': [{'request_id': ident, 'trace_id': 'f', 'session_id': 's', 'step': step,
                              'prompt_tokens': prompt, 'output_tokens': 2, 'arrival_us': 0, 'completion_us': 10,
                              'ttft_us': 5} for step, (ident, prompt) in enumerate((('a', 100), ('b', 200)))]}
        metrics = {'preemption_count': 0, 'requests': [{'requestID': ident, 'handled_by': 'instance_0',
                    'scheduling_delay_ms': .002, 'gateway_queue_delay_ms': .002} for ident in refs]}
        def event(ident, kind, at, count, batch=1):
            return {'kind': kind, 'at_us': at, 'request_id': ident, 'session_id': 'f/s',
                    'service_us': shapes[refs[ident]['shape']]['service_us'], 'arrival_us': 0,
                    'preparation_instance': 'instance_0' if ident == 'a' else 'instance_1',
                    'preparation_service_us': 2, 'preparation_wait_us': 0, 'prepared_us': 2,
                    'ready_count': count, 'instance': 'instance_0', 'largest_first': True,
                    'capacity': {'batch_size': batch, 'scheduled_tokens': 4, 'used_kv_blocks': 1,
                                 'promised_kv_blocks': 2, 'total_kv_blocks': 10}}
        events = [event('a', 'prepare', 0, 0), event('b', 'prepare', 0, 0),
                  event('a', 'ready', 2, 1), event('b', 'ready', 2, 2),
                  event('a', 'claim', 2, 2), event('b', 'claim', 2, 1, 2),
                  event('a', 'complete', 10, 0), event('b', 'complete', 10, 0)]
        audit = {'format_version': 1, 'policy': 'request-split', 'events': events, 'router_latency_us': 0,
                 'renderers': [{'instance': f'instance_{i}', 'api_servers': 1, 'workers': 1,
                                'rendered': int(i < 2), 'wait_us': 0} for i in range(8)],
                 'reservations': {f'instance_{i}': {'admission_refusals': 0, 'peak_promised_blocks': 2,
                                 'final_promised_blocks': 0, 'final_used_kv_blocks': 0, 'total_kv_blocks': 10,
                                 'preemptions': 0, 'allocation_failures': 0} for i in range(8)}}
        self.assertEqual(common.verify_audit(audit, manifest, work, metrics)['claims'], 2)
        diagnostics = request_rows({'id': 'r', 'router': 'request-split', 'arrival_seed': 42}, work, metrics, manifest, audit)
        self.assertEqual(diagnostics[0]['additional_admission_wait_us'], 0)
        self.assertEqual(diagnostics[1]['post_first_token_us'], 5)
        bad = deepcopy(audit)
        bad['events'][4] = event('b', 'claim', 2, 2)
        with self.assertRaisesRegex(ValueError, 'priority'):
            common.verify_audit(bad, manifest, work, metrics)
        bad = deepcopy(audit)
        bad['reservations']['instance_0']['final_promised_blocks'] = 1
        with self.assertRaisesRegex(ValueError, 'leaked'):
            common.verify_audit(bad, manifest, work, metrics)

    def test_paired_tables_include_elapsed_and_slowdown(self):
        rows = []
        for policy, elapsed in (('jsq', 20), ('request-split', 10)):
            run = {'id': policy, 'router': policy, 'arrival_seed': 42}
            objects = [{'elapsed_us': elapsed, 'slowdown': elapsed/5}]
            rows.extend(per_run_statistics(run, objects, objects))
        paired = paired_statistics(rows)
        self.assertEqual({r['metric'] for r in paired}, {'elapsed_us', 'slowdown'})
        self.assertEqual({r['unit'] for r in paired}, {'session', 'family'})
        self.assertTrue(all(r['percent_change'] == -50 for r in paired))


if __name__ == '__main__':
    unittest.main()
