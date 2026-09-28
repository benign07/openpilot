"""Invoked before launch's manager startup; failures stop launch for recovery."""
import sys
from pathlib import Path

if __name__ == '__main__':
  root = Path(__file__).resolve().parents[3]
  sys.path.insert(0, str(root))
  from selfdrive.carrot.hud_update.core import CONFIG, apply_at_boot
  if CONFIG.is_file():
    print('HUD update boot transaction:', apply_at_boot(root=root), flush=True)
