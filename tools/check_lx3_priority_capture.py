"""Reproduce exact installed source loss with the same real quota regression."""
from pathlib import Path
import argparse,hashlib,importlib.util,json,subprocess,tempfile
from selfdrive.carrot.can_diagnostics.test_priority_capsule import full_store_regression,urgent_retention_regression
from selfdrive.carrot.can_diagnostics import automatic

ROOT=Path(__file__).resolve().parents[1]


def main():
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('--out',type=Path,required=True);a=p.parse_args()
  a.out.mkdir(parents=True,exist_ok=True)
  before='b0ae42d568cf546ff57e8b1db6ee202e259e6be0'
  raw=subprocess.check_output(['git','show',before+':selfdrive/carrot/can_diagnostics/automatic.py'],cwd=ROOT)
  assert hashlib.sha256(raw).hexdigest()=='8aab199e7bc2d2de9ef82d3c32e71266022963522c9e0bdc5442e85888627227'
  path=a.out/'installed-before-automatic.py';path.write_bytes(raw)
  spec=importlib.util.spec_from_file_location('exact_installed_before_automatic',path)
  module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
  before_failed=False
  with tempfile.TemporaryDirectory() as tmp:
    try:full_store_regression(module,tmp)
    except AssertionError as error:
      assert str(error)=='ordinary quota suppressed first-fault evidence';before_failed=True
  assert before_failed,'installed-source counterexample disappeared'
  urgent_before_failed=False
  with tempfile.TemporaryDirectory() as tmp:
    try:urgent_retention_regression(module,tmp)
    except AssertionError as error:
      assert str(error)=='30-second throttle suppresses urgent retirement';urgent_before_failed=True
  assert urgent_before_failed,'installed retention throttle counterexample disappeared'
  with tempfile.TemporaryDirectory() as tmp:urgent_retention_regression(automatic,tmp)
  with tempfile.TemporaryDirectory() as tmp:after=full_store_regression(automatic,tmp)
  record={'before_commit':before,'before_sha256':hashlib.sha256(raw).hexdigest(),
    'after_sha256':hashlib.sha256(Path(automatic.__file__).read_bytes()).hexdigest(),
    'same_test':'full_store_regression','before_loses_first_fault':True,'after_priority_records':len(after),
    'same_urgent_retention_test_before_fails_after_passes':True,
    'no_vehicle_or_CAN_publisher':True}
  (a.out/'result.json').write_text(json.dumps(record,indent=2))
  print('PASS exact installed source FAIL -> candidate PASS, same real full-store regression; passive only')


if __name__=='__main__':main()
