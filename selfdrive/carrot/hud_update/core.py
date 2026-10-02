"""Portable release validation and recoverable boot-time file transaction.

No shell commands, Params writes, git resets, or running-process file changes.
Firmware, native binaries, DBC/schema and the updater itself are out of scope.
"""
import base64
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import time

ROOT = Path('/data/openpilot')
STATE_ROOT = Path('/data/community/hud_updates')
CONFIG = STATE_ROOT / 'config.json'
CHANNEL = 'https://raw.githubusercontent.com/benign07/openpilot/hud-device-updates-20260928/updates/hud/latest.json'
RAW = 'https://raw.githubusercontent.com/benign07/openpilot/'
LIMIT = 8 * 1024 * 1024
HEX = re.compile(r'^[a-f0-9]{64}$')
RELEASE = re.compile(r'^[a-z0-9][a-z0-9-]{0,63}$')


def sha(data):
  return hashlib.sha256(data).hexdigest()


def canonical(value):
  return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def atomic(path, data, mode=0o600):
  path = Path(path)
  path.parent.mkdir(parents=True, exist_ok=True)
  temp = path.with_name(path.name + '.hud-tmp')
  with temp.open('wb') as stream:
    stream.write(data)
    stream.flush()
    os.fsync(stream.fileno())
  os.chmod(temp, mode)
  os.replace(temp, path)
  if os.name == 'posix':
    fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def save(path, value):
  atomic(path, canonical(value))


def load(path, default=None):
  try: return json.loads(Path(path).read_text(encoding='utf-8'))
  except FileNotFoundError: return default


def checked_path(root, name):
  p = PurePosixPath(name)
  if not isinstance(name, str) or str(p) != name or p.is_absolute() or '..' in p.parts or '\\' in name:
    raise ValueError('Invalid release path')
  allowed = (name.startswith('selfdrive/carrot/') or name.startswith('selfdrive/controls/lib/') or
             name.startswith('opendbc_repo/opendbc/car/hyundai/') or
             name in ('selfdrive/selfdrived/selfdrived.py', 'selfdrive/controls/controlsd.py'))
  if not allowed or p.suffix not in ('.py', '.js', '.css', '.html', '.json') or 'hud_update' in name:
    raise ValueError('This file needs a separate device deployment: ' + name)
  # Symlinked files/parents cannot redirect a release outside the selected tree.
  target = root / name
  for parent in (target, *target.parents):
    if parent == root: break
    if parent.is_symlink(): raise ValueError('Symlink in release path')
  target.resolve().relative_to(root.resolve())
  return target


def validate_index(index):
  if index.get('schema') != 1 or not RELEASE.fullmatch(index.get('release_id', '')):
    raise ValueError('Unsupported release index')
  if type(index.get('sequence')) is not int or index['sequence'] < 1:
    raise ValueError('Invalid release sequence')
  if not re.fullmatch(r'[a-f0-9]{40}', index.get('bundle_commit', '')):
    raise ValueError('Release must pin an immutable Git commit')
  if not HEX.fullmatch(index.get('bundle_sha256', '')) or type(index.get('bundle_bytes')) is not int or not 0 < index['bundle_bytes'] <= LIMIT:
    raise ValueError('Invalid release size/hash')
  if not isinstance(index.get('notes'), list) or not 1 <= len(index['notes']) <= 30 or any(not isinstance(x, str) or len(x) > 1000 for x in index['notes']):
    raise ValueError('Invalid change notes')
  return index


def bundle_url(index):
  validate_index(index)
  return RAW + index['bundle_commit'] + '/updates/hud/bundles/' + index['release_id'] + '.json'


def verify_bundle(raw, index, public_key, current_sequence=0):
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
  validate_index(index)
  if len(raw) != index['bundle_bytes'] or sha(raw) != index['bundle_sha256']:
    raise ValueError('Release download checksum mismatch')
  envelope = json.loads(raw)
  payload = base64.b64decode(envelope['payload'], validate=True)
  Ed25519PublicKey.from_public_bytes(base64.b64decode(public_key, validate=True)).verify(
    base64.b64decode(envelope['signature'], validate=True), payload)
  release = json.loads(payload)
  if release.get('schema') != 1 or release.get('sequence') != index['sequence'] or release.get('release_id') != index['release_id'] or release.get('notes') != index['notes']:
    raise ValueError('Signed release and displayed version disagree')
  if type(release['sequence']) is not int or release['sequence'] <= current_sequence:
    raise ValueError('Release already installed or older than installed version')
  if release.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV':
    raise ValueError('Wrong vehicle release')
  if not re.fullmatch(r'[a-f0-9]{40}', release.get('source_commit', '')):
    raise ValueError('Missing source commit')
  files = release.get('files')
  if not isinstance(files, list) or not 1 <= len(files) <= 64:
    raise ValueError('Invalid release file count')
  names, total = set(), 0
  for row in files:
    name = row['path']
    checked_path(Path('/unused-release-root'), name)
    if name in names or not HEX.fullmatch(row.get('before', '')) or not HEX.fullmatch(row.get('sha256', '')):
      raise ValueError('Invalid/duplicate release file')
    names.add(name)
    data = base64.b64decode(row['data'], validate=True)
    total += len(data)
    if total > 4 * 1024**2 or type(row.get('bytes')) is not int or len(data) != row['bytes'] or sha(data) != row['sha256']:
      raise ValueError('Invalid file checksum/size')
    if name.endswith('.py'): compile(data, name, 'exec')
    if name.endswith('.json'): json.loads(data)
  return release


def stage(root, state_root, release):
  """Prepare immutable originals and replacements; production files stay untouched."""
  directory = state_root / 'releases' / release['release_id']
  if directory.exists():
    existing = load(directory / 'release.json')
    if existing != release: raise ValueError('Existing release staging differs')
  if shutil.disk_usage(state_root).free < 128 * 1024**2 + sum(x['bytes'] for x in release['files']) * 2:
    raise ValueError('Not enough storage; original files preserved')
  for row in release['files']:
    target = checked_path(root, row['path'])
    if not target.is_file() or sha(target.read_bytes()) != row['before']:
      raise ValueError('Device source differs; PC review required: ' + row['path'])
  directory.mkdir(parents=True, exist_ok=True)
  for row in release['files']:
    source = checked_path(root, row['path'])
    original = source.read_bytes()
    if sha(original) != row['before']: raise ValueError('Source changed during staging')
    atomic(directory / 'original' / row['path'], original)
    atomic(directory / 'new' / row['path'], base64.b64decode(row['data'], validate=True))
  save(directory / 'release.json', release)
  return directory


def validate_staged(root, state_root, release):
  directory = state_root / 'releases' / release['release_id']
  for row in release['files']:
    target = checked_path(root, row['path'])
    if sha(target.read_bytes()) != row['before']:
      raise ValueError('Device changed before reboot: ' + row['path'])
    if sha((directory / 'original' / row['path']).read_bytes()) != row['before'] or sha((directory / 'new' / row['path']).read_bytes()) != row['sha256']:
      raise ValueError('Staged content damaged')


def rollback(root, state_root, state):
  release = state['release']
  directory = state_root / 'releases' / release['release_id']
  # Validate all backups before the first restoration. A damaged backup stops launch.
  for row in release['files']:
    if sha((directory / 'original' / row['path']).read_bytes()) != row['before']:
      raise ValueError('Backup damaged; manual recovery required')
    checked_path(root, row['path'])
  state['phase'] = 'rolling_back'; save(state_root / 'state.json', state)
  for row in release['files']:
    target = checked_path(root, row['path'])
    if sha(target.read_bytes()) not in (row['before'], row['sha256']):
      raise ValueError('Unrelated source changes; manual recovery required')
    atomic(target, (directory / 'original' / row['path']).read_bytes(), target.stat().st_mode & 0o777)
  state.update(phase='rolled_back', message='설치 중단 감지 · 이전 파일 복구 완료', finished_at=time.time())
  save(state_root / 'installed.json', state.get('previous_installed', {}))
  save(state_root / 'state.json', state)


def apply_at_boot(root=ROOT, state_root=STATE_ROOT, boot_id=None, now=None, write=atomic):
  """Called by launch script only, before any manager/driving process starts."""
  state = load(state_root / 'state.json', {})
  if state.get('phase') in ('applying', 'rolling_back'):
    rollback(root, state_root, state)
    return 'rolled_back'
  if state.get('phase') != 'armed': return 'unchanged'
  now = time.time() if now is None else now
  boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip() if boot_id is None else boot_id
  # Delayed unrelated power cycles must not consume an old parked authorization.
  if boot_id == state.get('armed_boot') or not 0 <= now - state.get('armed_at', 0) <= 120:
    # Do not automatically re-arm an expired request and loop through reboots.
    state.update(phase='failed', message='재시작 지연 · 기존 파일 유지. P 정차 후 업데이트를 다시 예약하세요.')
    save(state_root / 'state.json', state)
    return 'deferred'
  release = state['release']
  directory = state_root / 'releases' / release['release_id']
  try:
    validate_staged(root, state_root, release)
  except Exception:
    state.update(phase='failed', message='설치 전 파일 검증 실패 · 기존 코드 유지')
    save(state_root / 'state.json', state)
    return 'rejected'
  state['phase'] = 'applying'; save(state_root / 'state.json', state)
  try:
    for row in release['files']:
      target = checked_path(root, row['path'])
      write(target, (directory / 'new' / row['path']).read_bytes(), target.stat().st_mode & 0o777)
    for row in release['files']:
      if sha(checked_path(root, row['path']).read_bytes()) != row['sha256']:
        raise ValueError('Installed file checksum mismatch')
  except BaseException:
    rollback(root, state_root, state)
    raise
  save(state_root / 'installed.json', {k: release[k] for k in ('release_id', 'sequence', 'notes', 'source_commit')})
  state.update(phase='verifying', applied_boot=boot_id, applied_at=now, message='새 버전 파일 적용 · 기기 시작 확인 중')
  save(state_root / 'state.json', state)
  return 'applied'
