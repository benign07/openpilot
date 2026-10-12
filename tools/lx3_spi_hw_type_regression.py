"""Same actual Panda constructor test against exact old and new production code."""
from pathlib import Path
import subprocess,json
root=Path(__file__).resolve().parents[1];out=root/'arm-evidence';results={}
for label,executable in [('before','arm-before-hw-type'),('after','selfdrive/pandad/tests/test_spi_hw_type')]:
  results[label]={}
  for length in [-1,0,1]:
    run=subprocess.run([str(root/executable),str(length)],capture_output=True,text=True,timeout=10)
    results[label][str(length)]={'returncode':run.returncode,'output':run.stdout+run.stderr}
    (out/f'hw-type-{label}-{length}.log').write_text(run.stdout+run.stderr)
assert all(r['returncode']==0 for r in results['after'].values())
assert results['before']['1']['returncode']==0
assert results['before']['-1']['returncode']!=0 and results['before']['0']['returncode']!=0
(out/'hw-type-validation.json').write_text(json.dumps(results,indent=2))
print('PASS exact old fails negative/short hardware-type read; new rejects both and preserves normal construction')
