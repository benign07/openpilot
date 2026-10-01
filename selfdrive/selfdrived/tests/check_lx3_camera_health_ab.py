"""PC CI: compare exact old CarState source with current startup camera handling.

Shared current imports and schema, not the complete original device image or its
recorded card-consumer timing. Does not publish CAN, controls, ACKs or heartbeats.
"""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import types

from openpilot.common.basedir import BASEDIR
from openpilot.system.hardware import PC
from openpilot.selfdrive.selfdrived.tests.test_lx3_runtime_configuration import TestLx3RuntimeConfiguration
from opendbc.car import Bus
from opendbc.car.hyundai.carstate import CarState
from opendbc.car.hyundai.hyundaicanfd import hkg_can_fd_checksum

assert PC and os.environ.get('GITHUB_ACTIONS') == 'true', 'Run only in isolated GitHub PC CI'
root = Path(BASEDIR).resolve()
assert root == Path(os.environ['GITHUB_WORKSPACE']).resolve()
baseline = '1d3367bb5d1c58a62af055989b586fd47ca94658'
relative = 'opendbc_repo/opendbc/car/hyundai/carstate.py'
report = {'baseline_carstate_source_commit': baseline, 'passed': False, 'cases': [],
          'scope': 'Actual old CarState source with shared current imports/schema; a startup mechanism comparison, not the exact noon ControlsReady/consumer timeline or OEM warning proof.'}


def run_case(label, state_type, old_module=None):
  fixture = TestLx3RuntimeConfiguration()
  fixture.setUp()
  try:
    if old_module is not None:
      # The old module has its own Params binding; keep it in the same isolated
      # fixture directory as the actual current interface/DBC configuration.
      old_module.Params = lambda: fixture.params
    fixture.params.put_bool('ControlsReady', False)
    state = state_type(fixture.configuration())
    parsers = state.get_can_parsers_canfd(state.CP)
    ns = 1_000_000_000
    physical_counter = 254
    message_counter = 254

    def frame(address, size, counter):
      data = bytearray(size)
      data[2] = counter
      data[:2] = hkg_can_fd_checksum(address, None, data).to_bytes(2, 'little')
      return bytes(data)

    def tick():
      nonlocal ns, physical_counter, message_counter
      ns += 40_000_000
      physical_counter = (physical_counter + 2) & 0xff
      message_counter = (message_counter + 1) & 0xff
      parsers[Bus.pt].update([[ns, [(0x10B, frame(0x10B, 16, physical_counter), 0),
                                  (0xEA, frame(0xEA, 24, message_counter), 0)]]])
      parsers[Bus.cam].update([[ns, [(0x162, frame(0x162, 32, message_counter), 2)]]])
      return state.update(parsers)

    before = None
    for _ in range(3):
      before = tick()
      assert state.controls_ready_count == 0
    assert state.lx3_button_intent.input.ready
    assert bool(before.steerFaultTemporary) == (label == 'baseline')
    assert (state.ccnc_0x162 is None) == (label == 'baseline')
    fixture.params.put_bool('ControlsReady', True)
    first_healthy_count = None
    for expected_count in range(1, 124):
      result = tick()
      assert state.controls_ready_count == expected_count
      assert state.lx3_button_intent.input.ready
      expected_fault = label == 'baseline' and expected_count <= 122
      assert bool(result.steerFaultTemporary) == expected_fault, (label, expected_count, result.steerFaultTemporary)
      if not result.steerFaultTemporary and first_healthy_count is None:
        first_healthy_count = expected_count
    assert first_healthy_count == (123 if label == 'baseline' else 1)
    assert state.ccnc_0x162 is not None
    result = {'label': label, 'controls_not_ready_ticks': 3,
              'physical_input_ready_before_controls': True,
              'temporary_fault_before_controls': bool(before.steerFaultTemporary),
              'first_healthy_controls_ready_count': first_healthy_count,
              'old_cache_assignment_count': 122 if label == 'baseline' else None,
              'case_passed': True}
    print(json.dumps(result), flush=True)
    return result
  finally:
    fixture.doCleanups()


try:
  subprocess.run(['git', 'fetch', '--no-tags', '--depth=1', 'origin', baseline], cwd=root, check=True)
  old_source = subprocess.check_output(['git', 'show', f'{baseline}:{relative}'], cwd=root)
  report['baseline_carstate_source_sha256'] = hashlib.sha256(old_source).hexdigest()
  report['candidate_carstate_source_sha256'] = hashlib.sha256((root / relative).read_bytes()).hexdigest()
  module = types.ModuleType('lx3_original_carstate_ab')
  module.__file__ = str(root / relative)
  exec(compile(old_source, f'{baseline}:{relative}', 'exec'), module.__dict__)
  report['cases'].append(run_case('candidate', CarState))
  report['cases'].append(run_case('baseline', module.CarState, module))
  report['passed'] = True
  print(json.dumps({'camera_health_source_ab_passed': True, 'cases': len(report['cases'])}), flush=True)
except BaseException as error:
  report['error'] = f'{type(error).__name__}: {str(error)[:500]}'
  raise
finally:
  (root / 'lx3-camera-health-source-ab.json').write_text(json.dumps(report, indent=2))
