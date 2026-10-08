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
from unittest.mock import patch
from types import ModuleType, SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
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

# Real public status builder, with only the boot-id source substituted on PC.
from openpilot.selfdrive.carrot.hud_update import core, service
with tempfile.TemporaryDirectory() as temp:
  folder = Path(temp)
  core.save(folder / 'baseline-migration.json', {'phase': 'prepared', 'preserved_sequence': 41})
  core.save(folder / 'config.json', {'phone_ip': '127.0.0.1', 'token': 'test-only', 'public_key': 'test-only'})
  with patch.object(service, 'Path', return_value=SimpleNamespace(read_text=lambda: 'fixture-boot')):
    status = service.UpdateService({}, state_root=folder).public()
  assert status['phase'] == 'migration_blocked' and status['latest'] is None
  (destination / 'hud_migration_blocked.json').write_text(json.dumps(status), encoding='utf8')
