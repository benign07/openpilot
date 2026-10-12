"""Verify SDK bytes and record architecture, dependencies and source provenance."""
from pathlib import Path
import argparse,hashlib,json,subprocess,tarfile,re

ROOT = Path(__file__).resolve().parents[1]
sha = lambda data: hashlib.sha256(data).hexdigest()


def run(*args):
  return subprocess.check_output(args, cwd=ROOT, text=True)


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('action', choices=('sdk', 'record'))
  args = parser.parse_args()
  if args.action == 'sdk':
    package = ROOT / 'tools/lx3_spi_arm_sdk_20261012.tar.gz'
    expected = (ROOT / 'tools/lx3_spi_arm_sdk_20261012.sha256').read_text().split()[0]
    assert sha(package.read_bytes()) == expected
    dest = ROOT / 'arm-sdk'; dest.mkdir(exist_ok=True)
    with tarfile.open(package) as tf:
      for member in tf:
        rel = Path(member.name)
        assert not rel.is_absolute() and '..' not in rel.parts and member.isfile()
        assert rel.parts[0] in ('capnproto', 'zeromq', 'licenses', 'manifest.json')
        tf.extract(member, dest, filter='data')
    manifest = json.loads((dest / 'manifest.json').read_text())
    for name, row in manifest['files'].items():
      assert sha((dest / name).read_bytes()) == row['sha256']
    print('PASS sealed original ARM SDK headers/static libraries/tools', len(manifest['files']))
    return
  out = ROOT / 'arm-evidence'; out.mkdir(exist_ok=True)
  binary = ROOT / 'selfdrive/pandad/pandad'
  header = run('readelf', '-h', str(binary)); assert 'AArch64' in header
  dynamic = run('readelf', '-d', str(binary)); assert 'RPATH' not in dynamic and 'RUNPATH' not in dynamic
  dependencies = run('ldd', str(binary)); assert 'not found' not in dependencies
  assert all(lib not in dependencies for lib in ['libcapnp.so', 'libkj.so', 'libzmq.so'])
  needed = [line.split('[')[1].split(']')[0] for line in dynamic.splitlines() if '(NEEDED)' in line]
  assert set(needed) <= {'libstdc++.so.6', 'libm.so.6', 'libgcc_s.so.1', 'libc.so.6', 'libpthread.so.0', 'libdl.so.2', 'ld-linux-aarch64.so.1'}
  versions = run('readelf', '--version-info', str(binary))
  baseline = json.loads((ROOT / 'tools/lx3_spi_arm_baseline.json').read_text())
  tags = set(re.findall(r'GLIBC(?:XX)?_[0-9]+(?:\.[0-9]+)+|CXXABI_[0-9]+(?:\.[0-9]+)+', versions))
  assert tags <= set(baseline['original_required_tags']), 'new binary requires newer ABI than the verified original'
  for name, digest in baseline['paired_sources'].items():
    assert sha((ROOT / name).read_bytes()) == digest, name
  # Prove the fork's actual hardware selector, not just the manifest label.
  probe = out / 'hardware-probe.cc'
  probe.write_text('#include <type_traits>\n#include "system/hardware/hw.h"\n'
                   'static_assert(std::is_same_v<Hardware, HardwareTici>);\n'
                   'int main(){return Hardware::PC() ? 1 : 0;}\n')
  run('clang++', '-std=c++17', '-D__TICI__', '-mcpu=cortex-a57', '-I.',
      '-Iarm-sdk/capnproto/install/include', str(probe), '-o', str(out / 'hardware-probe'))
  run(str(out / 'hardware-probe'))
  schemas = {p: sha((ROOT / p).read_bytes()) for p in ['cereal/car.capnp', 'cereal/log.capnp', 'cereal/custom.capnp', 'cereal/deprecated.capnp', 'cereal/services.py']}
  sources = {}
  for directory in ['selfdrive/pandad', 'common', 'cereal/messaging', 'msgq_repo/msgq', 'system/hardware', 'third_party/json11']:
    for p in (ROOT / directory).rglob('*'):
      if p.suffix in ('.cc', '.h', '.cpp') and 'tests' not in p.parts:
        sources[p.relative_to(ROOT).as_posix()] = sha(p.read_bytes())
  manifest = {'source_commit':run('git', 'rev-parse', 'HEAD').strip(),
              'machine':'AArch64', 'hardware_define':'__TICI__', 'cpu':'cortex-a57',
              'binary_sha256':sha(binary.read_bytes()), 'binary_bytes':binary.stat().st_size,
              'sdk_sha256':(ROOT / 'tools/lx3_spi_arm_sdk_20261012.sha256').read_text().split()[0],
              'dynamic_dependencies':needed, 'schemas':schemas, 'sources':sources,
              'compiler':run('clang++','--version').splitlines()[0],
              'ABI_requirements_subset_of_original':True, 'actual_HardwareTici_selector_verified':True,
              'vehicle_installed':False, 'vehicle_qualified':False}
  for name,text in [('elf-header.txt',header),('dynamic.txt',dynamic),('ldd.txt',dependencies),('symbol-versions.txt',versions)]:
    (out / name).write_text(text)
  (out / 'manifest.json').write_text(json.dumps(manifest,indent=2))
  print(json.dumps({k:manifest[k] for k in ['machine','binary_sha256','binary_bytes','dynamic_dependencies']}))


if __name__ == '__main__': main()
