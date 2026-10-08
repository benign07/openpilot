"""Export actual pure snapshot builders for the unchanged installed Android app.

Fake service inputs; this is not a complete installed overlay or HTTP/IPC test.
The pinned legacy builder may omit freshness metadata; the client must not guess.
"""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
BASELINE = '75b5d824bd13f1a5d4225f36edfd769d4b52677b'
destination = Path(sys.argv[1]); destination.mkdir(parents=True, exist_ok=True)
with tempfile.TemporaryDirectory() as temp:
  old = Path(temp)
  for name in ('snapshot.py', 'contract.py', 'normalize.py', 'services.py'):
    data = subprocess.check_output(['git', 'show', f'{BASELINE}:selfdrive/carrot/server/live_runtime/{name}'], cwd=ROOT)
    (old / name).write_bytes(data)
  for label, base in [('rollback', old), ('modern', ROOT/'openpilot/selfdrive/carrot/server/live_runtime')]:
    alias = '_hud_fixture_' + label
    package = ModuleType(alias); package.__path__ = [str(base)]; sys.modules[alias] = package
    spec = importlib.util.spec_from_file_location(alias+'.snapshot', base/'snapshot.py')
    module = importlib.util.module_from_spec(spec); sys.modules[spec.name] = module; spec.loader.exec_module(module)
    class SM:
      data = {'carState': object(), 'longitudinalPlan': object()}
      alive = {'carState': True, 'longitudinalPlan': True}
      valid = {'carState': True, 'longitudinalPlan': True}
      logMonoTime = {name: time.monotonic_ns() for name in data}
      values = {'carState': SimpleNamespace(vEgo=12., canValid=True, canTimeout=False,
                                            cruiseState=SimpleNamespace(enabled=True, speed=15.)),
                'longitudinalPlan': SimpleNamespace(myDrivingMode=3)}
      def __getitem__(self, name): return self.values[name]
    payload = module.build_live_payload(sm=SM(), repo_flavor='carrot')
    (destination / ('hud_'+label+'.json')).write_text(
      json.dumps({'ok': True, 'snapshotAgeMs': 10, **payload}), encoding='utf-8')
    print(label, 'schema', payload['meta']['schemaVersion'],
          'freshness', 'serviceAgeMs' in payload['runtime'])
