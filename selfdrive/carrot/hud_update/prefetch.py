"""Bounded signed-envelope cache. Never stages files or authorizes installation."""
from . import core


def wifi_connected(sm, now):
  try:
    return bool(sm.alive['deviceState'] and sm.valid['deviceState'] and
                0 <= now-sm.logMonoTime['deviceState']/1e9 < 3. and
                str(sm['deviceState'].networkType) == 'wifi')
  except (AttributeError, KeyError, TypeError, ValueError):
    return False


def check_sources(root, release):
  for row in release['files']:
    target = core.checked_path(root, row['path'])
    if not target.is_file() or core.sha(target.read_bytes()) != row['before']:
      raise ValueError('기존 파일이 게시 기준과 다릅니다: '+row['path'])


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
