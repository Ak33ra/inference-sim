"""Copy the reviewed SPLIT delta while protecting preexisting root edits."""

import argparse
import difflib
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess


ROOT = Path('/home/akira/inference-sim')
WORK = Path('/scratch/akira/request-split-implementation')


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    baseline = json.loads((ROOT / '.audit/request-split-baseline.json').read_text())
    names = subprocess.check_output(['git', 'ls-files', '-co', '--exclude-standard', '-z'],
        cwd=WORK, env={**os.environ, 'GIT_OPTIONAL_LOCKS': '0'}).decode().split('\0')
    changes, diff = [], []
    for name in sorted(set(names) | set(baseline)):
        if not name or name.startswith('.audit/'):
            continue
        source, target = WORK / name, ROOT / name
        new, old = digest(source), baseline.get(name)
        if new == old:
            continue
        assert name.startswith(('sim/cluster/', 'cmd/', 'research/request_split/')) or name == 'research/scripts/tests/test_request_split.py', name
        assert new is not None, f'unexpected deletion: {name}'
        assert digest(target) in (old, new), f'root changed concurrently: {name}'
        before = target.read_text().splitlines(True) if target.exists() else []
        diff.extend(difflib.unified_diff(before, source.read_text().splitlines(True),
            fromfile='before/' + name, tofile='after/' + name))
        changes.append(dict(path=name, before=old, after=new))
    for name in baseline:
        if name.startswith(('research/semiclosed/', 'research/policy_study/', 'research/session_split/', 'research/session_reservation/')):
            assert digest(WORK / name) == baseline[name], f'frozen helper changed: {name}'
    (ROOT / '.audit/request-split-integration.diff').write_text(''.join(diff))
    (ROOT / '.audit/request-split-changes.json').write_text(json.dumps(changes, indent=2) + '\n')
    print('\n'.join(row['path'] for row in changes))
    print(f'{len(changes)} files; apply={args.apply}')
    if args.apply:
        for row in changes:
            target = ROOT / row['path']
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(WORK / row['path'], target)
            assert digest(target) == row['after']


if __name__ == '__main__':
    main()
