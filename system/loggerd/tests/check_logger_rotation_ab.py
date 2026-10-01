"""PC CI only: compile exact pre-fix logger source and require a specific stall.

Restore and rebuild the candidate even on failure. Support libraries are current,
not an original full device image. No vehicle or CAN I/O.
"""
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

from openpilot.common.basedir import BASEDIR
from openpilot.system.hardware import PC
from openpilot.system.loggerd.tests.logger_rotation_case import run_case

assert PC and os.environ.get('GITHUB_ACTIONS') == 'true', 'Run only in isolated GitHub PC CI'
root = Path(BASEDIR).resolve()
assert root == Path(os.environ['GITHUB_WORKSPACE']).resolve()
source = root / 'system/loggerd/loggerd.cc'
binary = source.with_suffix('')
baseline = '999df40fec9833a5e1dedca49929536c835aad81'
relative = source.relative_to(root).as_posix()
report = {'baseline_logger_source_commit': baseline, 'cases': [], 'passed': False,
          'scope': 'Actual old loggerd source compiled with unchanged current logger support; isolated synthetic VisionIPC, not vehicle runtime.'}


def build_logger(name):
  build = subprocess.run(['uv', 'run', '--no-sync', 'scons', '--minimal', '-j2', relative[:-3]],
                         cwd=root, capture_output=True, text=True, timeout=300)
  (root / name).write_text(build.stdout + build.stderr)
  assert build.returncode == 0, f'Logger build failed; inspect {name}'


try:
  subprocess.run(['git', 'fetch', '--no-tags', '--depth=1', 'origin', baseline], cwd=root, check=True)
  old_source = subprocess.check_output(['git', 'show', f'{baseline}:{relative}'], cwd=root)
  changed = subprocess.check_output(['git', 'diff', '--name-only', baseline, 'HEAD', '--', 'system/loggerd'],
                                    cwd=root, text=True).splitlines()
  assert {name for name in changed if '/tests/' not in name} == {relative}, changed
  assert b'camera_streams_known' not in old_source
  candidate_source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
  candidate_binary_hash = hashlib.sha256(binary.read_bytes()).hexdigest()
  report.update(baseline_logger_source_sha256=hashlib.sha256(old_source).hexdigest(),
                candidate_logger_source_sha256=candidate_source_hash,
                candidate_logger_binary_sha256=candidate_binary_hash)
  for include_driver, record_front in ((False, True), (False, False), (True, True), (True, False)):
    report['cases'].append(run_case('candidate', include_driver, record_front))
  with tempfile.TemporaryDirectory(prefix='lx3-logger-ab-') as directory:
    backup = Path(directory)
    shutil.copy2(source, backup / 'loggerd.cc')
    shutil.copy2(binary, backup / 'loggerd')
    try:
      source.write_bytes(old_source)
      build_logger('lx3-logger-baseline-build.log')
      report['baseline_logger_binary_sha256'] = hashlib.sha256(binary.read_bytes()).hexdigest()
      assert report['baseline_logger_binary_sha256'] != candidate_binary_hash
      for include_driver, record_front in ((False, True), (False, False), (True, True), (True, False)):
        report['cases'].append(run_case('baseline', include_driver, record_front, expect_stall=not include_driver))
    finally:
      shutil.copy2(backup / 'loggerd.cc', source)
      try:
        # Rebuild restores old-source object/signature state as well as the ELF.
        build_logger('lx3-logger-candidate-restored-build.log')
        assert hashlib.sha256(source.read_bytes()).hexdigest() == candidate_source_hash
        assert hashlib.sha256(binary.read_bytes()).hexdigest() == candidate_binary_hash
        report['candidate_source_binary_objects_restored'] = True
      except BaseException:
        # Preserve candidate ELF even if restoration compilation itself failed;
        # that failure remains fatal and is never a passing A/B report.
        shutil.copy2(backup / 'loggerd', binary)
        report['candidate_source_binary_objects_restored'] = False
        raise
  report['passed'] = True
  print(json.dumps({'logger_rotation_ab_passed': True, 'cases': len(report['cases'])}), flush=True)
except BaseException as error:
  report['error'] = f'{type(error).__name__}: {str(error)[:500]}'
  raise
finally:
  (root / 'lx3-logger-rotation-ab.json').write_text(json.dumps(report, indent=2))
