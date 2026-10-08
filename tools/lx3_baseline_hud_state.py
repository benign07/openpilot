"""Full-baseline installer helper, never invoked by source-only phone updates.

Caller has stopped the old updater and verified the complete native/source bundle.
Archive obsolete layout transactions while retaining pairing and anti-replay floor.
This is updater metadata only, not a firmware or complete vehicle rollback image.
"""
from pathlib import Path
import os
import re
import time
import uuid

from openpilot.selfdrive.carrot.hud_update import core

TERMINAL = {'idle', 'complete', 'failed', 'rolled_back', 'health_warning', 'cancelled'}
ENTRY_NAMES = ('installed.json', 'state.json', 'latest.json', 'history.json', 'releases')
JOURNAL = 'baseline-migration.json'


def sync_dir(path):
  if os.name == 'posix':
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try: os.fsync(fd)
    finally: os.close(fd)


def restore_baseline(state_root: Path):
  """Installer recovery, with all update processes stopped, before root rollback.

  A durable journal also covers SIGTERM/power loss. Retry restoration before
  starting either updater; a partial migration must never restart at floor zero.
  """
  state_root = Path(state_root)
  journal = core.load(state_root / JOURNAL, {})
  if journal.get('phase') == 'restored': return False
  if journal.get('phase') not in ('prepared', 'complete', 'restoring'):
    raise ValueError('No recoverable baseline migration')
  name = journal.get('archive_name', '')
  if not re.fullmatch(r'[0-9]+-[a-f0-9]{32}', name):
    raise ValueError('Invalid migration archive identity')
  archive = state_root / 'baseline_archive' / name
  entries = journal.get('archived_entries')
  floor = journal.get('preserved_sequence')
  if (not isinstance(entries, list) or len(entries) != len(set(entries)) or
      not set(entries) <= set(ENTRY_NAMES) or type(floor) is not int or floor < 0):
    raise ValueError('Invalid migration journal')
  if archive.is_symlink() or archive.parent.is_symlink():
    raise ValueError('Unexpected migration archive symlink')
  current_sequence = core.load(state_root / 'installed.json', {}).get('sequence', 0)
  if type(current_sequence) is not int or current_sequence < 0:
    raise ValueError('Unknown live update sequence floor')
  floor = max(floor, current_sequence)
  journal['preserved_sequence'] = floor
  journal['phase'] = 'restoring'
  core.save(state_root / JOURNAL, journal)
  for name in ENTRY_NAMES:
    old, live = archive / name, state_root / name
    if old.is_symlink() or live.is_symlink():
      raise ValueError('Unexpected updater entry symlink')
    if old.exists():
      if live.exists():
        if name not in ('installed.json', 'state.json') or not live.is_file():
          raise ValueError('New updater data must be archived before rollback')
        live.unlink()
      old.replace(live)
      sync_dir(archive); sync_dir(state_root)
    elif name not in entries and live.exists():
      if name not in ('installed.json', 'state.json') or not live.is_file():
        raise ValueError('Unexpected data in interrupted baseline')
      live.unlink(); sync_dir(state_root)
  installed = core.load(state_root / 'installed.json', {})
  if 'installed.json' in entries and not installed:
    raise ValueError('Original installed state is missing or unreadable')
  if installed:
    installed['sequence'] = max(installed.get('sequence', 0), floor)
    core.save(state_root / 'installed.json', installed)
  journal['phase'] = 'restored'
  core.save(state_root / JOURNAL, journal)
  return True


def initialize_baseline(state_root: Path, source_commit: str, native_inventory_sha256: str):
  if not re.fullmatch(r'[a-f0-9]{40}', source_commit) or not core.HEX.fullmatch(native_inventory_sha256):
    raise ValueError('Missing verified baseline identity')
  state_root = Path(state_root)
  journal = core.load(state_root / JOURNAL, {})
  if (state_root / JOURNAL).exists() and journal.get('phase') not in ('prepared', 'restoring', 'complete', 'restored'):
    raise ValueError('Unknown baseline migration state')
  if journal.get('phase') in ('prepared', 'restoring'):
    raise ValueError('Recover the interrupted baseline migration before retrying')
  state = core.load(state_root / 'state.json', {})
  if state.get('phase', 'idle') not in TERMINAL:
    raise ValueError('Finish or cancel the previous update before replacing its baseline')
  installed = core.load(state_root / 'installed.json', {})
  sequence = installed.get('sequence', 0)
  if type(sequence) is not int or sequence < 0:
    raise ValueError('Unknown update sequence floor')
  if (installed.get('source_commit') == source_commit and installed.get('native_protocol') == 5 and
      installed.get('native_inventory_sha256') == native_inventory_sha256):
    return {'changed': False, 'installed': installed}
  state_root.mkdir(parents=True, exist_ok=True)
  entries = [state_root / name for name in ENTRY_NAMES if (state_root / name).exists()]
  if any(path.is_symlink() for path in entries):
    raise ValueError('Unexpected symlink in updater state')
  archive = state_root / 'baseline_archive' / (str(time.time_ns()) + '-' + uuid.uuid4().hex)
  archive.mkdir(parents=True, exist_ok=False)
  sync_dir(archive.parent); sync_dir(state_root)
  # This is durable before installed.json or the sequence floor can move.
  journal = {'phase': 'prepared', 'source_commit': source_commit,
             'archive_name': archive.name, 'preserved_sequence': sequence,
             'archived_entries': [path.name for path in entries]}
  core.save(state_root / JOURNAL, journal)
  moved = []
  try:
    for path in entries:
      path.replace(archive / path.name)
      moved.append(path.name)
      sync_dir(archive); sync_dir(state_root)
    installed = {'release_id': 'baseline-modern-' + source_commit[:8], 'sequence': sequence,
                 'source_commit': source_commit, 'notes': ['LX3 modern development baseline'],
                 'native_protocol': 5, 'native_inventory_sha256': native_inventory_sha256,
                 'installation_kind': 'full_development_baseline'}
    core.save(state_root / 'installed.json', installed)
    core.save(state_root / 'state.json', {'phase': 'idle', 'message': '새 기준 버전 · 호환 업데이트 확인 대기'})
    core.save(archive / 'migration.json', {'source_commit': source_commit, 'preserved_sequence': sequence,
                                         'archived_entries': moved})
    journal['phase'] = 'complete'
    core.save(state_root / JOURNAL, journal)
  except Exception:
    restore_baseline(state_root)
    raise
  return {'changed': True, 'archive': str(archive), 'installed': installed}
