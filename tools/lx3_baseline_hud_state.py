"""Full-baseline installer helper, never invoked by source-only phone updates.

Caller has stopped the old updater and verified the complete native/source bundle.
Archive obsolete layout transactions while retaining pairing and anti-replay floor.
This is updater metadata only, not a firmware or complete vehicle rollback image.
"""
from pathlib import Path
import re
import time
import uuid

from openpilot.selfdrive.carrot.hud_update import core

TERMINAL = {'idle', 'complete', 'failed', 'rolled_back', 'health_warning'}
ENTRY_NAMES = ('installed.json', 'state.json', 'latest.json', 'history.json', 'releases')


def initialize_baseline(state_root: Path, source_commit: str, native_inventory_sha256: str):
  if not re.fullmatch(r'[a-f0-9]{40}', source_commit) or not core.HEX.fullmatch(native_inventory_sha256):
    raise ValueError('Missing verified baseline identity')
  state_root = Path(state_root)
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
  moved = []
  try:
    for path in entries:
      path.replace(archive / path.name)
      moved.append(path.name)
    installed = {'release_id': 'baseline-modern-' + source_commit[:8], 'sequence': sequence,
                 'source_commit': source_commit, 'notes': ['LX3 modern development baseline'],
                 'native_protocol': 5, 'native_inventory_sha256': native_inventory_sha256,
                 'installation_kind': 'full_development_baseline'}
    core.save(state_root / 'installed.json', installed)
    core.save(state_root / 'state.json', {'phase': 'idle', 'message': '새 기준 버전 · 호환 업데이트 확인 대기'})
    core.save(archive / 'migration.json', {'source_commit': source_commit, 'preserved_sequence': sequence,
                                         'archived_entries': moved})
  except Exception:
    for name in ('installed.json', 'state.json'):
      (state_root / name).unlink(missing_ok=True)
    for name in moved:
      (archive / name).replace(state_root / name)
    raise
  return {'changed': True, 'archive': str(archive), 'installed': installed}
