import os
os.environ["GIT_OPTIONAL_LOCKS"] = "0"
from pathlib import Path
import json,subprocess,concurrent.futures
root=Path('/home/akira/inference-sim'); base=root/'research/outputs/mega-session-split-4s/runs/loaded-cacheoff-jsq-fcfs-4s-seed42'; target=root/'.audit/request-oracle-probe'; target.mkdir(exist_ok=True)
shapes=[(1,1),(543,189),(16383,2),(16384,2),(16385,2),(32769,100),(109756,288),(2000,1000)]
docs=[json.loads(x) for x in (base/'scenario.yaml').read_text().split('---')]; docs[0]['cluster']['nodes']=1; docs[1]['pools'][0]['nodes']=1
(target/'scenario.yaml').write_text('\n---\n'.join(json.dumps(d) for d in docs))
basecmd=json.loads((base/'commands.json').read_text())[2]
def run(name,sequence):
 out=target/name; out.mkdir(exist_ok=True)
 requests=[dict(req_id=i,parent_ids=[i-1] if i else [],type='n',t=0,api_time=0,model='m',**{'in':p,'out':o,'hash_ids':[]}) for i,(p,o) in enumerate(sequence)]
 (out/'input.jsonl').write_text(json.dumps(dict(id='probe',models=['m'],block_size=64,hash_id_scope='local',requests=requests))+'\n')
 cmd=basecmd.copy(); replacements={'--semi-closed-input':out/'input.jsonl','--semi-closed-report':out/'workload.json','--metrics-path':out/'metrics.json','--scenarios':target,'--num-instances':1,'--research-router-decisions':out/'router.json'}
 for flag,value in replacements.items():cmd[cmd.index(flag)+1]=str(value)
 with (out/'log').open('w') as log:subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
 work=json.loads((out/'workload.json').read_text()); metric=json.loads((out/'metrics.json').read_text())
 assert work['complete'] and work['accounting_match']; assert metric['completed_requests']==len(sequence)
 ordered=sorted(work['requests'],key=lambda r:r['step'])
 for i,r in enumerate(ordered):
  if i:assert r['arrival_us']==ordered[i-1]['completion_us']
  assert r['delay_us']==0
 return name,{f"{r['prompt_tokens']}/{r['output_tokens']}":r['completion_us']-r['arrival_us'] for r in ordered}
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
 results=dict(pool.map(lambda item:run(*item), [('chain',shapes),('reverse',list(reversed(shapes))),*[(f'single-{i}',[s]) for i,s in enumerate(shapes)]]))
for name,result in results.items():
 for shape,elapsed in result.items():assert elapsed==results['chain'][shape],(name,shape,elapsed,results['chain'][shape])
(target/'proof.json').write_text(json.dumps(dict(shapes=shapes,results=results,exact_parity=True),indent=2)+'\n')
print(json.dumps(results['chain'],indent=2)); print('10 native CLI replays; exact chain/reverse/clean-singleton parity')
