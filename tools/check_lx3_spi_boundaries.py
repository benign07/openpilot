"""Compile production SPI paths with inert host I/O and MCU hardware fixtures."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def run(arguments, log):
  result = subprocess.run(arguments, capture_output=True, text=True, encoding='utf-8', timeout=90)
  log.write_text(result.stdout + result.stderr, encoding='utf-8')
  if result.returncode:
    raise RuntimeError(f'{log.name}: {result.stdout}{result.stderr}')


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--cc', default='gcc')
  parser.add_argument('--cxx', default='g++')
  parser.add_argument('--output', type=Path, required=True)
  args = parser.parse_args()
  out = args.output.resolve()
  out.mkdir(parents=True, exist_ok=True)
  fixtures = ROOT / 'selfdrive/pandad/tests/spi_host_fixtures'
  source = ROOT / 'selfdrive/pandad/spi.cc'
  exe = out / 'spi-host'
  run([args.cxx, '-std=c++17', '-O2', '-g', '-pthread', '-Wall', '-Wextra',
       '-fsanitize=undefined', '-fno-sanitize-recover=all',
       '-I' + str(fixtures), '-I' + str(ROOT),
       '-DSPI_PRODUCTION_SOURCE="' + source.as_posix() + '"', '-DEXPECT_SPI_DIAGNOSTICS',
       str(ROOT / 'selfdrive/pandad/tests/spi_host_probe.cc'), '-o', str(exe)], out / 'host-compile.log')
  cases = ('good', 'oversized', 'request', 'recovery', 'nack', 'header_nack', 'checksum',
           'header_stuck', 'control_data_stuck', 'can_header_stuck', 'mixed_nack', 'disconnect_backpressure')
  for case in (*cases, 'boundary_ok', 'boundary_fail'):
    run([str(exe), case], out / ('host-' + case + '.log'))
  # Use the SAME fixture against the exact previously installed production driver.
  baseline = '32ff4a1101fc522ce43d44e0989cd52862abc203'
  old_source = out / 'exact-before-spi.cc'
  old_source.write_bytes(subprocess.check_output(['git', 'show', baseline + ':selfdrive/pandad/spi.cc'], cwd=ROOT))
  old_exe = out / 'spi-host-before'
  run([args.cxx, '-std=c++17', '-O2', '-g', '-pthread', '-Wall', '-Wextra',
       '-fsanitize=undefined', '-fno-sanitize-recover=all',
       '-I' + str(fixtures), '-I' + str(ROOT),
       '-DSPI_PRODUCTION_SOURCE="' + old_source.as_posix() + '"', '-DEXPECT_SPI_DIAGNOSTICS',
       str(ROOT / 'selfdrive/pandad/tests/spi_host_probe.cc'), '-o', str(old_exe)], out / 'before-compile.log')
  expected_bad = {'header_stuck', 'control_data_stuck', 'can_header_stuck'}
  before = {}
  for case in cases:
    result = subprocess.run([str(old_exe), case], capture_output=True, text=True, timeout=90)
    output = result.stdout + result.stderr
    (out / ('before-' + case + '.log')).write_text(output, encoding='utf-8')
    before[case] = result.returncode
    if case in expected_bad:
      assert result.returncode == 42 and 'unbounded SPI recovery/retry' in output, (case, output)
    else:
      assert result.returncode == 0, (case, output)
  (out / 'nack-before-after.json').write_text(json.dumps({
    'baseline': baseline, 'before_source_sha256': hashlib.sha256(old_source.read_bytes()).hexdigest(),
    'after_source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
    'same_probe_sha256': hashlib.sha256((ROOT / 'selfdrive/pandad/tests/spi_host_probe.cc').read_bytes()).hexdigest(),
    'before_returncodes': before, 'after_passed': [*cases, 'boundary_ok', 'boundary_fail'],
    'scope': 'Actual host production driver with inert IO and simulated timing; UBSAN; no hardware.'}, indent=2), encoding='utf-8')
  for name in ('counter.c', 'bounds.c', 'crc.h'):
    shutil.copyfile(ROOT / 'panda/tests/spi_host_fixtures' / name, out / name)
  for name in ('spi.h', 'spi_declarations.h', 'spi_diagnostics.h'):
    shutil.copyfile(ROOT / 'panda/board/drivers' / name, out / name)
  exe = out / 'spi-counter'
  run([args.cc, '-std=c11', '-O2', '-Wall', '-Wextra', '-fsanitize=undefined',
       '-fno-sanitize-recover=all', '-I' + str(out), str(out / 'counter.c'), '-o', str(exe)], out / 'counter-compile.log')
  run([str(exe)], out / 'counter.log')
  exe = out / 'spi-bounds'
  run([args.cc, '-std=c11', '-O2', '-Wall', '-Wextra', '-fsanitize=undefined',
       '-fno-sanitize-recover=all', '-I' + str(out), str(out / 'bounds.c'), '-o', str(exe)], out / 'bounds-compile.log')
  for case in ('tx', 'rx'):
    run([str(exe), case], out / ('bounds-' + case + '.log'))
  print('PASS actual host SPI 14 scenarios; exact baseline 3 specific unbounded failures; MCU cause counters and DMA/response bounds; no hardware connection')


if __name__ == '__main__':
  main()
