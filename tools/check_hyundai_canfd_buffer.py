"""Build/run actual CAN-FD buffer policy offline with mock STM definitions."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
  root = Path(__file__).resolve().parents[1]
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--compiler', nargs='+', default=['cc'])
  parser.add_argument('--safety-root', type=Path, default=root / 'opendbc_repo/opendbc/safety')
  parser.add_argument('--output', type=Path, required=True)
  parser.add_argument('--mode', choices=['all', 'refill', 'admission', 'grant'], default='all')
  parser.add_argument('--sanitize', action='store_true')
  args = parser.parse_args()
  args.output.mkdir(parents=True, exist_ok=True)
  source = root / 'opendbc_repo/opendbc/safety/tests/test_hyundai_canfd_buffer.c'
  exe = args.output / ('buffer-test.exe' if os.name == 'nt' else 'buffer-test')
  command = [*args.compiler, '-std=gnu11', '-Wall', '-Wextra', '-Werror', '-Wno-pointer-to-int-cast',
             '-I' + str(args.safety_root / 'board'), '-I' + str(args.safety_root),
             str(source), '-lm', '-o', str(exe)]
  # Upstream added this mock-board function; do not alter any policy for the comparison.
  if 'void putui(' in (args.safety_root / 'board/fake_stm.h').read_text():
    command.append('-DBUFFER_FAKE_STM_HAS_PUTUI')
  if args.sanitize:
    command += ['-fsanitize=undefined', '-fno-sanitize-recover=undefined', '-g']
  build = subprocess.run(command, capture_output=True)
  (args.output / 'build.log').write_bytes(build.stdout + build.stderr)
  run = subprocess.run([str(exe.resolve()), args.mode], capture_output=True) if build.returncode == 0 else None
  (args.output / 'test.log').write_bytes(run.stdout + run.stderr if run else b'NOT RUN: compile failed\n')
  files = [source, Path(__file__), *sorted(args.safety_root.rglob('*.h'))]
  evidence = {'scope': 'Offline production-header buffer regression, not complete firmware or vehicle qualification',
              'build_returncode': build.returncode, 'test_returncode': run.returncode if run else None,
              'mode': args.mode, 'command': command,
              'files': [{'path': str(p.resolve()), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in files]}
  (args.output / 'result.json').write_text(json.dumps(evidence, indent=2), encoding='utf-8')
  sys.stdout.buffer.write((build.stdout + build.stderr) if build.returncode else (run.stdout + run.stderr))
  return build.returncode or run.returncode


if __name__ == '__main__':
  raise SystemExit(main())
