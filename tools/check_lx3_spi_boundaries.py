"""Compile production SPI paths with inert host I/O and MCU hardware fixtures."""
import argparse
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
  run([args.cxx, '-std=c++17', '-O1', '-g', '-pthread', '-Wall', '-Wextra',
       '-fsanitize=undefined', '-fno-sanitize-recover=all',
       '-I' + str(fixtures), '-I' + str(ROOT),
       '-DSPI_PRODUCTION_SOURCE="' + source.as_posix() + '"', '-DEXPECT_SPI_DIAGNOSTICS',
       str(ROOT / 'selfdrive/pandad/tests/spi_host_probe.cc'), '-o', str(exe)], out / 'host-compile.log')
  for case in ('good', 'oversized', 'request', 'recovery', 'nack', 'header_nack', 'checksum'):
    run([str(exe), case], out / ('host-' + case + '.log'))
  for name in ('counter.c', 'crc.h'):
    shutil.copyfile(ROOT / 'panda/tests/spi_host_fixtures' / name, out / name)
  for name in ('spi.h', 'spi_declarations.h'):
    shutil.copyfile(ROOT / 'panda/board/drivers' / name, out / name)
  exe = out / 'spi-counter'
  run([args.cc, '-std=c11', '-O1', '-Wall', '-Wextra', '-fsanitize=undefined',
       '-fno-sanitize-recover=all', '-I' + str(out), str(out / 'counter.c'), '-o', str(exe)], out / 'counter-compile.log')
  run([str(exe)], out / 'counter.log')
  print('PASS actual host SPI 7 scenarios and MCU VERSION/CRC/saturation fixture; no hardware connection')


if __name__ == '__main__':
  main()
