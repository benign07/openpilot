"""Read-only preflight for retaining LX3 drive evidence; never delete records."""
import argparse
import json
from pathlib import Path
import shutil


def check(root='/data', staging_bytes=0):
  capacity = shutil.disk_usage(root)
  # Keep stock loggerd thresholds; also allow the planned staging footprint.
  required = max(5 * 1024**3, (capacity.total * 30 + 99) // 100) + staging_bytes
  return {'ready': capacity.free >= required, 'free_bytes': capacity.free,
          'total_bytes': capacity.total, 'required_free_bytes': required,
          'needed_bytes': max(0, required - capacity.free),
          'automatic_delete': False,
          'action': 'preserve rollback on PC and remove only verified obsolete staging; do not lower loggerd thresholds'}


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--data-root', type=Path, default=Path('/data'))
  parser.add_argument('--staging-bytes', type=int, default=0)
  args = parser.parse_args()
  if args.staging_bytes < 0:
    parser.error('staging bytes must not be negative')
  result = check(args.data_root, args.staging_bytes)
  print(json.dumps(result))
  raise SystemExit(0 if result['ready'] else 2)
