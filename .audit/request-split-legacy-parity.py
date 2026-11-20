import os
os.environ["GIT_OPTIONAL_LOCKS"] = "0"
from pathlib import Path
import argparse,json,subprocess,hashlib
parser=argparse.ArgumentParser(); parser.add_argument('binary',type=Path); args=parser.parse_args()
root=Path('/home/akira/inference-sim'); old=root/'research/outputs/mega-session-split-4s/runs/loaded-cacheoff-session-split-fcfs-4s-seed42'; out=root/'.audit/request-split-legacy-parity'; out.mkdir(exist_ok=True)
cmd=json.loads((old/'commands.json').read_text())[2]; cmd[0]=str(args.binary.resolve())
for flag,name in [('--semi-closed-report','workload.json'),('--metrics-path','metrics.json'),('--research-router-decisions','router-decisions.json')]:cmd[cmd.index(flag)+1]=str(out/name)
with (out/'log').open('w') as log:subprocess.run(cmd,stdout=log,stderr=subprocess.STDOUT,check=True)
checks={}
for name in ['workload.json','metrics.json','router-decisions.json']:
 a=json.loads((out/name).read_text()); b=json.loads((old/name).read_text())
 if name=='metrics.json':
  a.get('catalog',{}).pop('path',None); b.get('catalog',{}).pop('path',None)
 assert a==b,name
 checks[name]=dict(equal=True,byte_identical=(out/name).read_bytes()==(old/name).read_bytes())
proof=dict(binary_sha256=hashlib.sha256(args.binary.read_bytes()).hexdigest(),legacy_output=str(old),checks=checks)
(out/'proof.json').write_text(json.dumps(proof,indent=2)+'\n'); print(json.dumps(proof,indent=2))
