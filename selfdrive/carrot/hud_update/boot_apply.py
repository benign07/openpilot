"""Invoked before launch's manager startup; failures stop launch for recovery."""
import sys
import time
from pathlib import Path

root = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(root))
from selfdrive.carrot.hud_update import core


def wait_for_synchronized_clock(marker=Path('/run/systemd/timesync/synchronized'), timeout=90,
                                monotonic=time.monotonic, sleep=time.sleep):
  # AGNOS can boot with its image-build date before NTP corrects wall time.
  # The marker lives in /run and therefore belongs to this boot, not an old one.
  deadline = monotonic() + timeout
  while not marker.is_file():
    remaining = deadline - monotonic()
    if remaining <= 0: return False
    sleep(min(1, remaining))
  return True


def apply_with_clock(root=core.ROOT, state_root=core.STATE_ROOT):
  state = core.load(state_root / 'state.json', {})
  if state.get('phase') == 'armed' and not wait_for_synchronized_clock():
    state.update(phase='failed', message='부팅 시각 동기화 실패 · 기존 파일 유지. 연결 확인 후 다시 예약하세요.')
    core.save(state_root / 'state.json', state)
    return 'clock_unavailable'
  # Incomplete file transactions are recovered immediately, even without NTP.
  return core.apply_at_boot(root=root, state_root=state_root)


if __name__ == '__main__':
  if core.CONFIG.is_file():
    print('HUD update boot transaction:', apply_with_clock(root=root), flush=True)
