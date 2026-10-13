"""Same-directory ARM object comparison against the installed native sources."""
from pathlib import Path
import hashlib
import json
import subprocess

ROOT = Path(__file__).resolve().parents[1]
BASELINE = '32ff4a1101fc522ce43d44e0989cd52862abc203'
CHANGED = ['selfdrive/pandad/spi.cc', 'selfdrive/pandad/spi_diagnostics.h']
OUT = ROOT / 'arm-evidence'
sha = lambda data: hashlib.sha256(data).hexdigest()


def objects():
  return {p.relative_to(ROOT).as_posix(): sha(p.read_bytes()) for p in ROOT.rglob('*')
          if p.suffix in ('.o', '.os') and 'arm-sdk' not in p.parts}


def build(name):
  with (OUT / name).open('wb') as log:
    subprocess.run(['/tmp/lx3-build/bin/scons', '-f', 'tools/lx3_spi_arm.SConstruct', '-j2'], cwd=ROOT,
                   stdout=log, stderr=subprocess.STDOUT, check=True, timeout=1200)


def main():
  OUT.mkdir(exist_ok=True)
  paths = ['selfdrive/pandad', 'panda', 'common', 'cereal', 'system/hardware', 'msgq_repo', 'third_party/json11']
  changed = subprocess.check_output(['git', 'diff', '--name-only', BASELINE, 'HEAD', '--', *paths], cwd=ROOT).decode().splitlines()
  assert all(p in CHANGED or p.startswith('selfdrive/pandad/tests/') for p in changed), changed
  def clean(built=False):
    status = subprocess.check_output(['git', 'status', '--porcelain', '--untracked-files=all', '--', *paths], cwd=ROOT).decode().splitlines()
    generated = {'selfdrive/pandad/tests/test_spi_hw_type', 'selfdrive/pandad/tests/test_spi_mcu_status'}
    for line in status:
      assert built and line[:3] == '?? ' and line[3:] in generated, status
      assert 'AArch64' in subprocess.check_output(['readelf', '-h', line[3:]], cwd=ROOT).decode()
  clean()
  saved = {p: (ROOT / p).read_bytes() for p in CHANGED}
  try:
    for p in CHANGED:
      (ROOT / p).write_bytes(subprocess.check_output(['git', 'show', BASELINE + ':' + p], cwd=ROOT))
    build('native-before-build.log')
    before = objects()
    binary_before = sha((ROOT / 'selfdrive/pandad/pandad').read_bytes())
  finally:
    for p, data in saved.items():
      (ROOT / p).write_bytes(data)
  clean(built=True)
  build('build.log')
  after = objects()
  assert before.keys() == after.keys() and before
  differences = [p for p in before if before[p] != after[p]]
  assert differences == ['selfdrive/pandad/spi.o'], differences
  record = {'source_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT).decode().strip(),
            'baseline': BASELINE, 'native_source_differences': changed, 'objects_before': before,
            'objects_after': after, 'changed_objects': differences, 'all_non_SPI_objects_identical': True,
            'rebuilt_baseline_binary_sha256': binary_before,
            'verified_installed_binary_sha256': '56850ffaff33b34a92636852364fe0403f2778d15e99c882cd552baced06cf13',
            'rebuilt_baseline_equals_installed_binary': binary_before == '56850ffaff33b34a92636852364fe0403f2778d15e99c882cd552baced06cf13',
            'candidate_binary_sha256': sha((ROOT / 'selfdrive/pandad/pandad').read_bytes()),
            'scope': 'Same runner/path/compiler/options. Baseline sources, not reproduction of old installed binary/toolchain.'}
  (OUT / 'native-object-comparison.json').write_text(json.dumps(record, indent=2))
  print('PASS same-directory ARM build: only spi.o differs;', len(after) - 1, 'other objects byte-identical')


if __name__ == '__main__':
  main()
