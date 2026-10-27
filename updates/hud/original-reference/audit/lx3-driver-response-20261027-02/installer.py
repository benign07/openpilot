"""Signed sixteen-file audit layer for original LX3; not shared HUD source OTA.

Reuses the hash-pinned parked transaction helpers. No native, firmware, Params,
owner JSON or updater writes. Installation and rollback are explicit phases.
"""
from pathlib import Path, PurePosixPath
import argparse
import base64
import hashlib
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

RID = 'lx3-driver-response-20261027-02'
SERIAL = '80acfe79'
PUBLIC_SHA = '56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
CORE_SHA = 'd08e4062811ec1099cd3afc9b84bda2174f712a72f3ef5db0a0a6dc0d7ba6a03'
FEATURE_SHA = '692a3d911f0793572f14529503ba4af6a848d425292bc44939b36de1bbe9ecbd'
ORDER = ['opendbc_repo/opendbc/car/hyundai/manual_steering.py', 'selfdrive/carrot/manual_steering.py', 'selfdrive/controls/lib/approach_comfort.py', 'selfdrive/controls/lib/lane_planner_2.py', 'selfdrive/controls/lib/lateral_planner.py', 'selfdrive/controls/lib/longitudinal_planner.py', 'selfdrive/carrot/carrot_controls.py', 'opendbc_repo/opendbc/car/hyundai/carcontroller.py', 'selfdrive/carrot/can_diagnostics/automatic.py', 'selfdrive/carrot/can_diagnostics/automatic_runtime.py', 'selfdrive/carrot/server/services/params.py', 'selfdrive/carrot/server/services/settings.py', 'selfdrive/carrot/server/features/params.py', 'selfdrive/carrot/server/features/system.py', 'selfdrive/ui/soundd.py', 'selfdrive/ui/ui_state.py']
FIXED_PROTECTED = {
  'selfdrive/pandad/pandad': '4b1f395024b2fb570f000491013981e6c1838d8553ab92ac282cf8d8533dfb0c',
  'selfdrive/pandad/spi.cc': '712dac60a855caea46cd1cd5799367db243b00f4f4fdfb108c01047e95ecf4b4',
  'common/params_pyx.so': '5ddd33516911f918e714bb94712b1e5932267b24f1e83a45d225453a668b6e48',
  'common/params_keys.h': '5f68a38eb84937fd5a4c8565c0d86e87966ea7c8682bc92ddff3750149a08ec0',
  'panda/board/obj/panda_h7.bin.signed': '075aba179ae30aad23235779e77f946eb0d24d56edb3c0553467e0e7928fbdb6',
  'opendbc_repo/opendbc/car/car.capnp': 'a77c1109baf93af369637f9a28451907afc54c235262944297166d9b45363be9',
  'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py': '19419019764695a94ca87cdf10a3beeec9fe0e5f79ab10e5bedbe3cc470d673c',
}
REQUIRED_PROTECTED = set(FIXED_PROTECTED) | {
  'selfdrive/car/cruise.py', 'selfdrive/car/lx3_authority.py',
  'selfdrive/car/card.py', 'selfdrive/controls/controlsd.py',
  'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py',
  'selfdrive/carrot/server/services/setting_safety.py',
}
JSON_NAMES = ('driving_preferences.json', 'cruise_button_preferences.json', 'notification_preferences.json', 'driving_response_preferences.json')
PARAM_KEYS = (
  'DisableDM', 'HardwareC3xLite', 'CalibrationParams', 'LiveTorqueParameters',
  'DongleId', 'CarSelected3', 'RecordRoadCam', 'CanfdDebug', 'PandaSignatures',
  'OpenpilotEnabledToggle', 'AlwaysLateral', 'EnableRadarTracks', 'EnableCornerRadar',
  'UseLaneLineSpeed', 'UseLaneLineCurveSpeed', 'PathOffset', 'AdjustLaneOffset',
  'C3xLiteHapticNotifications', 'C3xLiteHapticEngage', 'C3xLiteHapticTraffic',
  'C3xLiteHapticLaneChange', 'C3xLiteHapticStopping', 'C3xLiteHapticNavigation',
  'C3xLiteHapticReverse', 'C3xLiteHapticPattern',
)
HEX = re.compile(r'[a-f0-9]{64}')
sha = lambda raw: hashlib.sha256(raw).hexdigest()


def require(ok, message):
  if not ok:
    raise ValueError(message)


def load(name, path):
  spec = importlib.util.spec_from_file_location(name, path)
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


def checked(root, name):
  part = PurePosixPath(name)
  require(str(part) == name and not part.is_absolute() and '..' not in part.parts and '\\' not in name, 'Unsafe path')
  path = root / name
  for parent in (path, *path.parents):
    if parent == root:
      break
    require(not parent.is_symlink(), 'Symlink in deployment path: ' + name)
  require(path.resolve().is_relative_to(root.resolve()), 'Path escapes source root')
  return path


def digest_or_none(path):
  require(not path.is_symlink(), 'Symlink in preserved setting: ' + str(path))
  require(not path.exists() or path.is_file(), 'Non-file preserved setting')
  return sha(path.read_bytes()) if path.is_file() else None


def verify(folder, root, config=Path('/data/community/hud_updates/config.json')):
  require(sys.flags.optimize == 0, 'Optimized Python would disable the pinned transaction checks')
  public = base64.b64decode(json.loads(config.read_text())['public_key'], validate=True)
  require(sha(public) == PUBLIC_SHA, 'Wrong signing public key')
  raw = (folder / 'package.signed.json').read_bytes()
  require(0 < len(raw) <= 8 * 1024**2, 'Invalid package size')
  envelope = json.loads(raw)
  payload = base64.b64decode(envelope['payload'], validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'], validate=True), payload)
  manifest = json.loads(payload)
  require(manifest['schema'] == 1 and manifest['release_id'] == RID and manifest['target_serial'] == SERIAL, 'Wrong release or device')
  require(manifest['car_fingerprint'] == 'HYUNDAI_PALISADE_LX3_HEV', 'Wrong vehicle')
  require(re.fullmatch(r'[a-f0-9]{40}', manifest['source_commit']) is not None, 'Unpinned source commit')
  require(manifest['installer_sha256'] == sha(Path(__file__).read_bytes()), 'Installer hash mismatch')
  expected_support = {'haptic_core.py': CORE_SHA, 'feature_core.py': FEATURE_SHA}
  require(manifest['support_sha256'] == expected_support, 'Unsupported transaction helpers')
  for name, digest in expected_support.items():
    require(sha(checked(folder, name).read_bytes()) == digest, 'Helper hash mismatch: ' + name)
  protected = manifest['protected']
  require(REQUIRED_PROTECTED <= protected.keys() and not set(ORDER) & protected.keys(), 'Incomplete protected scope')
  require(all(protected.get(n) == h for n, h in FIXED_PROTECTED.items()), 'Protected native/schema mismatch')
  for name, digest in protected.items():
    checked(root, name)
    require(isinstance(digest, str) and HEX.fullmatch(digest), 'Invalid protected hash')
  rows = manifest['files']
  require(len(rows) == len(ORDER) and {v['path'] for v in rows} == set(ORDER), 'Wrong sixteen-file scope')
  files = {v['path']: v for v in rows}
  for name, row in files.items():
    checked(root, name)
    require(HEX.fullmatch(row['before']) and HEX.fullmatch(row['sha256']), 'Invalid file hash')
    data = base64.b64decode(row['data'], validate=True)
    require(type(row['bytes']) is int and 0 < len(data) == row['bytes'] <= 1024**2 and sha(data) == row['sha256'], 'Payload checksum/size mismatch')
    compile(data, name, 'exec')
  prefs = manifest['preserved_json']
  require(set(JSON_NAMES[:2]) <= prefs.keys() <= set(JSON_NAMES), 'Wrong owner JSON scope')
  require(all(h is None or isinstance(h, str) and HEX.fullmatch(h) for h in prefs.values()), 'Invalid owner JSON hash')
  core = load('audit_transaction_core', folder / 'haptic_core.py')
  core.ORDER, core.PROTECTED = list(ORDER), dict(protected)
  feature = load('audit_namespace_core', folder / 'feature_core.py')
  return manifest, files, sha(raw), core, feature


def validate_record(manifest, files, record, bundle):
  require(record['bundle_sha256'] == bundle and record['release_id'] == RID, 'Wrong transaction')
  require(record['source_commit'] == manifest['source_commit'] and set(record['files']) == set(ORDER), 'Wrong transaction scope')
  for name, row in record['files'].items():
    require(all(row[k] == files[name][k] for k in ('before', 'sha256', 'bytes')), 'Transaction file mismatch')
    require(type(row['mode']) is int and 0 <= row['mode'] <= 0o777, 'Invalid original mode')
  require(set(record['preserved_json']) == set(JSON_NAMES), 'Incomplete JSON snapshot')
  require(all(record['preserved_json'][n] == h for n, h in manifest['preserved_json'].items()), 'Owner JSON snapshot differs')


def verify_json(expected, community):
  for name, digest in expected.items():
    require(name in JSON_NAMES and digest_or_none(community / name) == digest, 'Owner JSON changed: ' + name)


def verify_params(record, params_dir, field='protected_settings'):
  for name, digest in record[field].items():
    require(digest_or_none(params_dir / name) == digest, 'Protected Params changed: ' + name)


def writers_stopped(core, feature, proc=Path('/proc')):
  core.stopped()
  require(feature.learned_writers_stopped(proc), 'Learned parameter writers still running')
  names = ('plannerd', 'carrot_server', 'soundd', 'controlsd', 'card')
  for path in proc.iterdir():
    if not path.name.isdigit() or int(path.name) == os.getpid():
      continue
    try:
      comm = (path / 'comm').read_text().strip()
      args = (path / 'cmdline').read_bytes().split(b'\0')
      ui_running = comm=='ui' or any(a in (b'./ui',b'selfdrive.ui.ui',b'openpilot.selfdrive.ui.ui')
        or a.endswith(b'/selfdrive/ui/ui.py') or a==b'selfdrive/ui/ui.py' for a in args)
      require(not ui_running and comm not in names and not any(any(n.encode() in a for n in names) for a in args),
              'Source consumer still running: ' + path.name)
    except (FileNotFoundError, ProcessLookupError):
      pass


def smoke(root, folder, core, feature):
  with tempfile.TemporaryDirectory(prefix='lx3-audit-smoke-') as temp:
    parent = Path(temp)
    view = parent / 'openpilot'
    feature.make_smoke_view(root, folder / 'new', view, ORDER)
    program = '''import importlib,json,sys
from pathlib import Path
from openpilot.common.params import Params
root=Path(sys.argv[1]).resolve()
assert len(Params().all_keys())==333
names=json.loads(sys.argv[2])
for name in names:
 if name=='selfdrive/ui/ui_state.py':
  # Full UI import creates global hardware/IPC instances. Only compile it in
  # staging; separate UI method tests and postboot rendering remain required.
  compile((root/name).read_bytes(),name,'exec')
  continue
 module=('opendbc.'+name.split('opendbc_repo/opendbc/',1)[1][:-3].replace('/','.')) if name.startswith('opendbc_repo/opendbc/') else 'openpilot.'+name[:-3].replace('/','.')
 m=importlib.import_module(module)
 assert Path(m.__file__).resolve().is_relative_to(root),(name,m.__file__)
print('PASS staged response imports; UI compile only; native333 registry; no actuator instance')
'''
    result = subprocess.run([sys.executable, '-B', '-c', program, str(view), json.dumps(ORDER)], cwd=parent,
                            env=feature.smoke_environment(parent, view), capture_output=True, text=True, timeout=60)
    core.atomic(folder / 'smoke.stdout', result.stdout.encode())
    core.atomic(folder / 'smoke.stderr', result.stderr.encode())
    require(result.returncode == 0, 'Staged import failed: ' + result.stderr[-2500:])


def stage(root, folder, manifest, files, bundle, core, feature, community, params_dir):
  record_path = folder / 'transaction.json'
  require(not record_path.exists(), 'Existing transaction must not be overwritten')
  core.before(root, files)
  verify_json(manifest['preserved_json'], community)
  record = {'phase': 'staging', 'release_id': RID, 'source_commit': manifest['source_commit'],
            'bundle_sha256': bundle, 'boot_before': Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
            'files': {}, 'preferences': {}, 'preserved_json': {n: digest_or_none(community / n) for n in JSON_NAMES},
            'protected_settings': {n: digest_or_none(params_dir / n) for n in PARAM_KEYS if (params_dir / n).is_file()}}
  core.save(record_path, record)
  for name in ORDER:
    path = checked(root, name)
    mode = path.stat().st_mode & 0o777
    core.atomic(folder / 'before' / name, path.read_bytes(), mode)
    core.atomic(folder / 'new' / name, base64.b64decode(files[name]['data'], validate=True), mode)
    record['files'][name] = {**{k: files[name][k] for k in ('before', 'sha256', 'bytes')}, 'mode': mode}
  smoke(root, folder, core, feature)
  verify_json(record['preserved_json'], community)
  core.before(root, files)
  record['phase'] = 'staged'
  core.save(record_path, record)
  return record


def transaction(phase, root, folder, manifest, files, bundle, core, feature, community, params_dir):
  record_path = folder / 'transaction.json'
  record = json.loads(record_path.read_text())
  validate_record(manifest, files, record, bundle)
  if phase == 'install':
    require(record['phase'] == 'staged', 'Not staged')
    core.before(root, files)
    require(all(sha((folder / 'new' / n).read_bytes()) == files[n]['sha256'] for n in ORDER), 'Staged source changed')
    core.validate_restore(root, folder, record)
    verify_json(record['preserved_json'], community)
  else:
    require(record['phase'] in ('stopping', 'installing', 'complete', 'installed_pending_reboot_verification', 'recovery_required'), 'Wrong rollback phase')
    core.validate_restore(root, folder, record)
  gate = core.parked()  # A prior preflight file never replaces this fresh continuous P/zero gate.
  previous = record['phase']
  record.update(phase='stopping', gate_before_stop=gate)
  core.save(record_path, record)
  changed = False
  try:
    subprocess.run(['sudo', '-n', 'systemctl', 'stop', 'comma.service'], check=True, timeout=10)
    writers_stopped(core, feature)
    require(json.loads((community / 'hud_updates/state.json').read_text())['phase'] == 'complete', 'Concurrent HUD transaction')
    if phase == 'install':
      feature.freeze_stopped_settings(params_dir, record, True)
      verify_json(record['preserved_json'], community)
    else:
      # A source-only rollback must preserve the owner's current choices, not
      # require recreating the staging-time bytes. Keep the manifest-bound
      # original fields intact for record validation and interrupted retries.
      record['rollback_preserved_json'] = {n: digest_or_none(community / n) for n in JSON_NAMES}
      record['rollback_protected_settings'] = {n: digest_or_none(params_dir / n) for n in PARAM_KEYS}
      core.save(record_path, record)
    require(time.monotonic() - gate['mono'] < 15, 'Expired stopped transaction window')
    if phase == 'install':
      core.before(root, files)
      core.validate_restore(root, folder, record)
      record['phase'] = 'installing'
      core.save(record_path, record)
      changed = True
      for name in ORDER:
        core.atomic(checked(root, name), (folder / 'new' / name).read_bytes(), record['files'][name]['mode'])
      require(all(sha(checked(root, n).read_bytes()) == files[n]['sha256'] for n in ORDER), 'Installed source mismatch')
      require(all(sha(checked(root, n).read_bytes()) == h for n, h in core.PROTECTED.items()), 'Protected source changed')
      verify_json(record['preserved_json'], community)
      verify_params(record, params_dir)
      require(time.monotonic() - gate['mono'] < 15, 'Installation exceeded stopped transaction window')
      record['phase'] = 'installed_pending_reboot_verification'
    else:
      core.validate_restore(root, folder, record)
      changed = True
      core.restore(root, folder, record, restore_preferences=False)
      core.before(root, files)
      verify_json(record['rollback_preserved_json'], community)
      verify_params(record, params_dir, field='rollback_protected_settings')
      require(time.monotonic() - gate['mono'] < 15, 'Rollback exceeded stopped transaction window')
      record['phase'] = 'restored_before_audit_layer'
    core.save(record_path, record)
  except BaseException:
    if changed and phase == 'install':
      try:
        core.restore(root, folder, record, restore_preferences=False)
        core.before(root, files)
        verify_json(record['preserved_json'], community)
        verify_params(record, params_dir)
        record['phase'] = 'restored_after_failure'
        core.save(record_path, record)
      except BaseException:
        record['phase'] = 'recovery_required'
        core.save(record_path, record)
        raise
    elif changed:
      record['phase'] = 'recovery_required'
      core.save(record_path, record)
      raise
    else:
      record['phase'] = previous
      core.save(record_path, record)
    subprocess.run(['sudo', '-n', 'systemctl', 'start', 'comma.service'], check=True, timeout=10)
    raise
  subprocess.Popen(['sudo', '-n', 'sh', '-c', 'sleep 3; reboot'], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, start_new_session=True)
  return record


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('phase', choices=['stage', 'install', 'rollback'])
  parser.add_argument('--folder', type=Path, required=True)
  args = parser.parse_args()
  root, folder, community, params_dir = Path('/data/openpilot'), args.folder, Path('/data/community'), Path('/data/params/d')
  require(root.resolve() == root and folder == Path('/data/lx3_separate_installs') / RID and folder.resolve() == folder, 'Wrong deployment location')
  require(params_dir.resolve().is_relative_to(Path('/data/params')), 'Params path escapes target')
  from openpilot.system.hardware import HARDWARE
  from openpilot.common.params import Params
  from cereal import car
  require(HARDWARE.get_serial() == SERIAL and __import__('platform').machine() == 'aarch64' and sys.version_info[:2] == (3, 12), 'Wrong hardware/runtime')
  with car.CarParams.from_bytes(Params().get('CarParams')) as cp:
    require(cp.carFingerprint == 'HYUNDAI_PALISADE_LX3_HEV' and not cp.passive and not cp.dashcamOnly
            and any(s.safetyParam == 2238 for s in cp.safetyConfigs), 'Wrong vehicle safety profile')
  import fcntl
  lock = (folder / 'installer.lock').open('a+b')
  fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
  require(json.loads((community / 'hud_updates/state.json').read_text())['phase'] == 'complete', 'HUD transaction incomplete')
  require((root / 'cereal/car.capnp').is_symlink() and os.readlink(root / 'cereal/car.capnp') == '../opendbc_repo/opendbc/car/car.capnp', 'Schema alias changed')
  manifest, files, bundle, core, feature = verify(folder, root)
  if args.phase == 'stage':
    record = stage(root, folder, manifest, files, bundle, core, feature, community, params_dir)
  else:
    record = transaction(args.phase, root, folder, manifest, files, bundle, core, feature, community, params_dir)
  print('RESULT_JSON=' + json.dumps({'release_id': RID, 'phase': record['phase'],
                                    'preferences_changed': False, 'native_changed': False, 'road_qualified': False}))


if __name__ == '__main__':
  main()
