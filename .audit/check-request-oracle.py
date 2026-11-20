from pathlib import Path
import json, hashlib, statistics, math
study_path=Path('research/outputs/mega-request-split-4s').resolve()
read=lambda p:json.loads(p.read_text())
study=read(study_path/'study.json'); manifest=read(Path(study['service_manifest'])); raw=study_path/'oracle/all-shapes/run'
work=read(raw/'workload.json'); metrics=read(raw/'metrics.json'); old=Path(study['runs'][0]['old_output']); original=read(old/'workload.json')
assert work['complete'] and work['accounting_match']
assert len(manifest['requests'])==58741 and len(manifest['shapes'])==56134
actual=[json.loads(d) for d in (raw/'scenario.yaml').read_text().split('---')]
expected=[json.loads(d) for d in (old/'scenario.yaml').read_text().split('---')]
assert expected[0]['cluster']['nodes']==expected[1]['pools'][0]['nodes']==8
expected[0]['cluster']['nodes']=expected[1]['pools'][0]['nodes']=1
assert actual==expected
command=read(raw/'commands.json')[2]; assert command[command.index('--num-instances')+1]=='1'
assert metrics['in_flight_requests_max']==1 and len(metrics['server_series']['engines'])==1
rows=sorted(work['requests'],key=lambda r:r['step']); native={r['requestID']:r for r in metrics['requests']}
shapes={(r['prompt_tokens'],r['output_tokens']) for r in original['requests']}
assert len(rows)==len(shapes)==len(native)
for i,row in enumerate(rows):
 p,o=row['prompt_tokens'],row['output_tokens']; shape=manifest['shapes'][f'{p}/{o}']; metric=native[row['request_id']]
 assert (p,o) in shapes and row['step']==i and shape['oracle_request_id']==row['request_id']
 assert row['arrival_us']==(rows[i-1]['completion_us'] if i else 0)
 assert row['delay_us']==0 and row['state']=='completed'
 assert row['prerequisites']==([dict(request_id=rows[i-1]['request_id'],on='completion')] if i else [])
 assert shape['service_us']==row['completion_us']-row['arrival_us']==row['e2e_us']
 assert math.isclose(metric['e2e_ms']*1000,shape['service_us'],abs_tol=1e-5)
 assert shape['preparation_us']==round(metric['scheduling_delay_ms']*1000)
 assert metric.get('gateway_queue_delay_ms',0)==0
for row in original['requests']:
 ref=manifest['requests'][row['request_id']]
 assert ref['runtime_session_id']==row['trace_id']+'/'+row['session_id'] and ref['step']==row['step']
 assert ref['shape']==f"{row['prompt_tokens']}/{row['output_tokens']}"
series=metrics['server_series']['engines'][0]
for field,expected in [('prompt_tokens_local_compute',sum(p for p,o in shapes)),('generation_tokens',sum(o for p,o in shapes)),('prompt_tokens_local_cache_hit',0),('prompt_tokens_external_kv_transfer',0)]:assert sum(series[field])==expected
assert metrics['total_input_tokens']==sum(p for p,o in shapes) and metrics['total_output_tokens']==sum(o for p,o in shapes)
assert metrics['completed_requests']==len(shapes)
assert all(metrics[k]==0 for k in ['still_queued','still_running','dropped_unservable','length_capped_requests','timed_out_requests','cancelled_requests','context_overflow_requests','failed_requests','preemption_count'])
assert series['kv_cache_usage_last'][-1]==0
proof=read(study_path/'oracle/proof.json'); assert proof['exact_parity'] and len(proof['probe_shapes'])==9
forward=read(study_path/'oracle/proof-forward/shapes.json')
for path in [study_path/'oracle/proof-reverse/shapes.json',*sorted((study_path/'oracle').glob('proof-single-*/shapes.json'))]:
 for key,r in read(path).items(): assert r['service_us']==forward[key]['service_us']
output=dict(checked_shapes=len(shapes),checked_request_mappings=len(original['requests']),prompt_tokens=metrics['total_input_tokens'],output_tokens=metrics['total_output_tokens'],maximum_in_flight=metrics['in_flight_requests_max'],complete=True,same_engine_scenario_except_one_replica=True,standalone_and_order_parity=True,binary_sha256=study['binary_sha256'],manifest_sha256=hashlib.sha256(Path(study['service_manifest']).read_bytes()).hexdigest())
(study_path/'independent-oracle-audit.json').write_text(json.dumps(output,indent=2)+'\n');print(json.dumps(output,indent=2))
