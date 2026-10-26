"""Seven-file signature/scope and failure rollback tests; no device or service IO."""
from pathlib import Path
import base64
import importlib.util
import json
import os
import shutil
import time

import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('audit_installer_test', ROOT / 'tools/install_lx3_audit_20261026.py')
deploy = importlib.util.module_from_spec(spec)
spec.loader.exec_module(deploy)


@pytest.fixture
def package(tmp_path, monkeypatch):
  folder, root, community, params = (tmp_path / n for n in ('package', 'root', 'community', 'params'))
  for path in (folder, root, community, params):
    path.mkdir()
  for name, source in (('haptic_core.py', 'install_original_lx3_haptics.py'),
                       ('feature_core.py', 'install_original_lx3_feature_parity.py')):
    shutil.copyfile(ROOT / 'tools' / source, folder / name)
  old, new = b'VALUE = 1\n', b'VALUE = 2\n'
  rows = [{'path': n, 'before': deploy.sha(old), 'sha256': deploy.sha(new),
           'bytes': len(new), 'data': base64.b64encode(new).decode()} for n in deploy.ORDER]
  owner = b'{"schema":1,"values":{"unchanged":10}}\n'
  for name in deploy.JSON_NAMES[:2]:
    (community / name).write_bytes(owner)
  key = Ed25519PrivateKey.generate()
  public = key.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
  monkeypatch.setattr(deploy, 'PUBLIC_SHA', deploy.sha(public))
  config = tmp_path / 'public.json'
  config.write_text(json.dumps({'public_key': base64.b64encode(public).decode()}))
  protected = {n: 'a' * 64 for n in deploy.REQUIRED_PROTECTED}
  protected.update(deploy.FIXED_PROTECTED)
  manifest = {'schema': 1, 'release_id': deploy.RID, 'target_serial': deploy.SERIAL,
              'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV', 'source_commit': '1' * 40,
              'installer_sha256': deploy.sha(Path(deploy.__file__).read_bytes()),
              'support_sha256': {'haptic_core.py': deploy.CORE_SHA, 'feature_core.py': deploy.FEATURE_SHA},
              'files': rows, 'protected': protected,
              'preserved_json': {n: deploy.sha(owner) for n in deploy.JSON_NAMES[:2]}}

  def sign():
    payload = json.dumps(manifest).encode()
    (folder / 'package.signed.json').write_text(json.dumps({
      'payload': base64.b64encode(payload).decode(), 'signature': base64.b64encode(key.sign(payload)).decode()}))
  sign()
  return folder, root, community, params, config, manifest, sign, old, new


@pytest.mark.parametrize('change', ['serial', 'vehicle', 'path', 'duplicate', 'missing', 'native',
                                   'support', 'payload', 'signature', 'installer', 'prefs', 'commit'])
def test_signed_scope_rejects_tampering_before_source_writes(package, change):
  folder, root, _, _, config, manifest, sign, _, _ = package
  deploy.verify(folder, root, config)
  if change == 'serial': manifest['target_serial'] = 'other'
  elif change == 'vehicle': manifest['car_fingerprint'] = 'KIA_EV9'
  elif change == 'path': manifest['files'][0]['path'] = '../outside.py'
  elif change == 'duplicate': manifest['files'][0] = manifest['files'][1]
  elif change == 'missing': manifest['protected'].pop('selfdrive/car/cruise.py')
  elif change == 'native': manifest['protected']['selfdrive/pandad/pandad'] = '0' * 64
  elif change == 'support': (folder / 'feature_core.py').write_bytes(b'# altered\n')
  elif change == 'payload': manifest['files'][0]['data'] = base64.b64encode(b'VALUE = 3\n').decode()
  elif change == 'installer': manifest['installer_sha256'] = '0' * 64
  elif change == 'prefs': manifest['preserved_json'].pop('driving_preferences.json')
  elif change == 'commit': manifest['source_commit'] = 'uncommitted'
  sign()
  if change == 'signature':
    envelope = json.loads((folder / 'package.signed.json').read_text())
    envelope['signature'] = base64.b64encode(b'\0' * 64).decode()
    (folder / 'package.signed.json').write_text(json.dumps(envelope))
  with pytest.raises(Exception):
    deploy.verify(folder, root, config)
  assert not list(root.iterdir())


def staged(package):
  folder, root, community, params, config, manifest, _, old, new = package
  _, files, bundle, core, feature = deploy.verify(folder, root, config)
  # Temporary source restoration fixture, not a substitute for device-native checks.
  core.PROTECTED = {}
  (root / 'prebuilt').write_bytes(b'')
  record = {'phase': 'staged', 'release_id': deploy.RID, 'source_commit': manifest['source_commit'],
            'bundle_sha256': bundle, 'files': {}, 'preferences': {}, 'protected_settings': {},
            'preserved_json': {n: deploy.digest_or_none(community / n) for n in deploy.JSON_NAMES}}
  for name, row in files.items():
    for base, data in ((root, old), (folder / 'before', old), (folder / 'new', new)):
      path = base / name
      path.parent.mkdir(parents=True, exist_ok=True)
      path.write_bytes(data)
    record['files'][name] = {**{k: row[k] for k in ('before', 'sha256', 'bytes')}, 'mode': 0o644}
  (folder / 'transaction.json').write_text(json.dumps(record))
  (community / 'hud_updates').mkdir()
  (community / 'hud_updates/state.json').write_text('{"phase":"complete"}')
  return folder, root, community, params, manifest, files, bundle, core, feature, record


@pytest.mark.skipif(os.name == 'nt', reason='Qualified shared atomic helper uses Linux directory fsync')
@pytest.mark.parametrize('prefix', [0, 1, 3, 6, 7])
def test_every_partial_prefix_restores_before_layer_and_owner_json(package, prefix):
  folder, root, community, _, _, files, _, core, _, record = staged(package)
  owner_before = {n: deploy.digest_or_none(community / n) for n in deploy.JSON_NAMES}
  for name in deploy.ORDER[:prefix]:
    (root / name).write_bytes((folder / 'new' / name).read_bytes())
  core.restore(root, folder, record, restore_preferences=False)
  assert all(deploy.sha((root / n).read_bytes()) == row['before'] for n, row in files.items())
  assert {n: deploy.digest_or_none(community / n) for n in deploy.JSON_NAMES} == owner_before


@pytest.mark.parametrize('damage', ['foreign_source', 'backup', 'owner_json'])
def test_preinstall_rejects_foreign_state_before_service_stop(package, monkeypatch, damage):
  folder, root, community, params, manifest, files, bundle, core, feature, _ = staged(package)
  if damage == 'foreign_source': (root / deploy.ORDER[3]).write_bytes(b'# foreign\n')
  elif damage == 'backup': (folder / 'before' / deploy.ORDER[3]).write_bytes(b'# corrupt\n')
  else: (community / deploy.JSON_NAMES[0]).write_bytes(b'{"changed":true}')
  calls = []
  monkeypatch.setattr(deploy.subprocess, 'run', lambda *a, **k: calls.append(a))
  with pytest.raises(Exception):
    deploy.transaction('install', root, folder, manifest, files, bundle, core, feature, community, params)
  assert calls == []


@pytest.mark.parametrize('phase', ['stopping', 'installing', 'recovery_required'])
def test_interrupted_transaction_allows_explicit_rollback_but_still_requires_fresh_park(package, monkeypatch, phase):
  folder, root, community, params, manifest, files, bundle, core, feature, record = staged(package)
  record['phase'] = phase
  (folder / 'transaction.json').write_text(json.dumps(record))
  if phase == 'installing':
    (root / deploy.ORDER[0]).write_bytes((folder / 'new' / deploy.ORDER[0]).read_bytes())
  def gate():
    raise RuntimeError('fresh parked gate reached')
  monkeypatch.setattr(core, 'parked', gate)
  monkeypatch.setattr(deploy.subprocess, 'run', lambda *a, **k: pytest.fail('No service action before parked gate'))
  with pytest.raises(RuntimeError, match='fresh parked gate reached'):
    deploy.transaction('rollback', root, folder, manifest, files, bundle, core, feature, community, params)


@pytest.mark.skipif(os.name == 'nt', reason='Qualified shared atomic helper uses Linux directory fsync')
def test_mid_install_failure_restores_all_sources_before_restart(package, monkeypatch):
  folder, root, community, params, manifest, files, bundle, core, feature, _ = staged(package)
  calls = []
  monkeypatch.setattr(core, 'parked', lambda: {'mono': time.monotonic(), 'continuous_seconds': 10})
  monkeypatch.setattr(deploy, 'writers_stopped', lambda *a: None)
  monkeypatch.setattr(deploy.subprocess, 'run', lambda cmd, **k: calls.append(cmd))
  monkeypatch.setattr(deploy.subprocess, 'Popen', lambda *a, **k: pytest.fail('A failed install must not reboot'))
  atomic, once = core.atomic, [False]

  def fault(path, data, mode=0o600):
    if path == root / deploy.ORDER[3] and not once[0]:
      once[0] = True
      raise OSError('injected write failure')
    atomic(path, data, mode)
  monkeypatch.setattr(core, 'atomic', fault)
  with pytest.raises(OSError, match='injected'):
    deploy.transaction('install', root, folder, manifest, files, bundle, core, feature, community, params)
  assert all(deploy.sha((root / n).read_bytes()) == row['before'] for n, row in files.items())
  assert json.loads((folder / 'transaction.json').read_text())['phase'] == 'restored_after_failure'
  assert calls == [['sudo', '-n', 'systemctl', 'stop', 'comma.service'], ['sudo', '-n', 'systemctl', 'start', 'comma.service']]
  deploy.verify_json(manifest['preserved_json'], community)


def test_prepare_uses_installed_before_and_preserves_all_unmodified_hashes(package, monkeypatch):
  spec = importlib.util.spec_from_file_location('audit_prepare_test', ROOT / 'tools/prepare_lx3_audit_20261026.py')
  prepare = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(prepare)
  folder, _, _, _, _, manifest, _, _, new = package
  source = {name: new for name in deploy.ORDER}
  source.update({prepare.INSTALLER: Path(prepare.deploy.__file__).read_bytes(),
                 'tools/install_original_lx3_haptics.py': (folder / 'haptic_core.py').read_bytes(),
                 'tools/install_original_lx3_feature_parity.py': (folder / 'feature_core.py').read_bytes()})
  monkeypatch.setattr(prepare, 'committed', lambda commit, name: source[name])
  before = {'hashes': {r['path']: r['before'] for r in manifest['files']},
            'prefs': {n: {'exists': True, 'sha256': h} for n, h in manifest['preserved_json'].items()}}
  installed = {'hashes': {**manifest['protected'], **before['hashes']}}
  actual, _ = prepare.compose('1' * 40, before, installed, b'Verification\n')
  assert actual['protected'] == manifest['protected']
  assert actual['preserved_json'] == manifest['preserved_json']
  assert {r['path']: r['before'] for r in actual['files']} == before['hashes']
  installed['hashes'][deploy.ORDER[0]] = '0' * 64
  with pytest.raises(ValueError, match='disagrees'):
    prepare.compose('1' * 40, before, installed, b'Verification\n')
