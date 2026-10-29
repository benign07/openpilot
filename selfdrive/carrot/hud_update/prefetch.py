"""Bounded signed-envelope cache. Never stages files or authorizes installation."""
import json
from . import core


def read_metadata(path):
  try:
    with path.open('rb') as stream:raw=stream.read(16385)
    if len(raw)>16384:return None
    return json.loads(raw)
  except (OSError,ValueError,UnicodeError):return None


def wifi_connected(sm, now):
  try:
    return bool(sm.alive['deviceState'] and sm.valid['deviceState'] and
                0 <= now-sm.logMonoTime['deviceState']/1e9 < 3. and
                str(sm['deviceState'].networkType) == 'wifi')
  except (AttributeError, KeyError, TypeError, ValueError):
    return False


def source_hashes(root, release):
  return {row['path']:core.sha(target.read_bytes()) if target.is_file() else None
          for row in release['files'] for target in (core.checked_path(root,row['path']),)}


def source_context(root, release):
  return core.sha(core.canonical(source_hashes(root,release)))


class SourceMismatch(ValueError):
  def __init__(self,path,context):
    super().__init__('기존 파일이 게시 기준과 다릅니다: '+path)
    self.context=context


def check_sources(root, release):
  hashes=source_hashes(root,release)
  for row in release['files']:
    if hashes[row['path']] != row['before']:
      raise SourceMismatch(row['path'],core.sha(core.canonical(hashes)))


class BundleCache:
  def __init__(self, folder):
    self.folder = folder/'prefetch'

  def load(self, index, public_key, sequence):
    try:
      envelope = self.folder/(index['bundle_sha256']+'.json')
      if not core.HEX.fullmatch(index['bundle_sha256']) or envelope.stat().st_size > core.LIMIT:
        return None
      raw = envelope.read_bytes()
      release = core.verify_bundle(raw, index, public_key, sequence)
      return raw, release
    except Exception:
      return None

  def save(self, index, raw):
    # Content addressed; the pinned caller index is always verified against
    # this signed envelope. No separately replaceable cache index can tear.
    digest = index['bundle_sha256']
    if len(raw) > core.LIMIT or not core.HEX.fullmatch(digest) or core.sha(raw) != digest:
      raise ValueError('캐시 크기 제한 초과')
    core.atomic(self.folder/(digest+'.json'), raw)
    # Called only under the service lock for its still-selected request.
    # Delete only cache blob names; never release backups/profile/user files.
    for path in self.folder.glob('*.json'):
      if path.stem != digest and core.HEX.fullmatch(path.stem): path.unlink()


def validate_block(value):
  if not isinstance(value,dict):return None
  expected={'schema','bundle_sha256','sequence','kind','source_context','message'}
  if set(value)!=expected:return None
  if type(value['schema']) is not int or value['schema']!=1:return None
  if type(value['sequence']) is not int or value['sequence']<1:return None
  if not isinstance(value['bundle_sha256'],str) or not core.HEX.fullmatch(value['bundle_sha256']):return None
  if value['kind'] not in ('source','validation'):return None
  context=value['source_context']
  if value['kind']=='source':
    if not isinstance(context,str) or not core.HEX.fullmatch(context):return None
  elif context is not None:return None
  if not isinstance(value['message'],str) or not 1<=len(value['message'])<=240:return None
  return dict(value)
