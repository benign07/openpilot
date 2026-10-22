"""Actual timer AST -> actual CCNC formatter/DBC/CRC. No live CAN or motor test."""
import ast
import importlib.util
from pathlib import Path
import sys
import types
from types import SimpleNamespace as NS

import pytest

HERE = Path(__file__).parent
ROOT = HERE.parents[2]
CONTROLLER = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
spec = importlib.util.spec_from_file_location('haptic_display_helpers', HERE / 'test_surround_display.py')
shared = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = shared
spec.loader.exec_module(shared)
codec = shared.codec
CCNC = shared.display_scope()['create_ccnc_messages']


def load_tick():
  tree = ast.parse(CONTROLLER.read_text(encoding='utf8'))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'CarController')
  update = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'update')
  start = next(i for i, n in enumerate(update.body) if isinstance(n, ast.Assign) and
               any(isinstance(t, ast.Name) and t.id == 'speed_decel_active' for t in n.targets))
  end = next(i for i, n in enumerate(update.body[start:], start) if isinstance(n, ast.If) and
             ast.unparse(n.test) == 'self.frame >= self.speedCameraHapticEndFrame')
  fn = ast.parse('def tick(self, hud_control, left_lane_warning=0, right_lane_warning=0):\n pass').body[0]
  fn.body = update.body[start:end + 1] + ast.parse('return left_lane_warning, right_lane_warning').body
  intervals = next(n.value for n in tree.body if isinstance(n, ast.Assign) and
                   any(isinstance(t, ast.Name) and t.id == 'vibrate_intervals' for t in n.targets))
  scope = {'DT_CTRL': .01, 'vibrate_intervals': ast.literal_eval(intervals)}
  exec(compile(ast.fix_missing_locations(ast.Module(body=[fn], type_ignores=[])), str(CONTROLLER), 'exec'), scope)
  return scope['tick']


def state(mode=1):
  return NS(frame=0, activeCarrot=0, speedCameraHapticEndFrame=0, hapticFeedbackWhenSpeedCamera=mode)


def formatter(packer, frame, warning=(0, 0), blinkers=(False, False), source_vibrate=0, notification=False):
  cs = shared.minimal_cs()
  cs.out.leftBlinker, cs.out.rightBlinker = blinkers
  cs.ccnc_0x162.update(VIBRATE=source_vibrate, FAULT_LSS=1, FAULT_DAS=1, FAULT_LFA=1)
  return CCNC(
    NS(carFingerprint=shared.LX3, flags=1, safetyConfigs=[NS(safetyParam=2048)]), packer, NS(ECAN=0, CAM=2), frame,
    NS(enabled=True, latActive=True), cs, NS(leadDistance=0.), 0.,
    *warning, 0, False, 0, notification_vibrate=notification)


@pytest.mark.parametrize('phase', range(5))
@pytest.mark.parametrize('mode', range(4))
def test_camera_pattern_survives_all_twenty_hz_send_phases(codec, phase, mode):
  packer, decode, checksum = codec
  tick = load_tick(); controller = state(mode)
  entry = 20 + phase
  sent = []
  for frame in range(entry + 900):
    controller.frame = frame
    warning = tick(controller, NS(activeCarrot=3 if frame >= entry else 0))
    expected_pulse = mode > 0 and entry <= frame < entry + 800 and any(
      a <= (frame - entry) * .01 < b for a, b in [(0., .5), (1., 1.5), (5., 5.5), (6., 6.5), (7.5, 8.)])
    assert warning == ((mode, mode) if expected_pulse else (0, 0))  # Preserve legacy CAN numeric modes too.
    if frame % 5:
      continue
    packets = formatter(packer, frame, warning)
    packet = next(p for p in packets if p[0] == 0x162)
    assert packet[2] == 0 and len(packet[1]) == 32
    assert int.from_bytes(packet[1][:2], 'little') == checksum(0x162, None, bytearray(packet[1]))
    if decode('CCNC_0x162', packet[1])['VIBRATE']:
      sent.append(frame)
  expected = [] if mode == 0 else [f for f in range(entry, entry + 800) if f % 5 == 0 and
    any(a <= (f - entry) * .01 < b for a, b in [(0., .5), (1., 1.5), (5., 5.5), (6., 6.5), (7.5, 8.)])]
  assert sent == expected
  assert len(sent) == (0 if mode == 0 else 50)


def test_frozen_baseline_has_no_camera_warning_pulses():
  scope = {'DT_CTRL': .01, 'vibrate_intervals': [(0., .5), (1., 1.5), (5., 5.5), (6., 6.5), (7.5, 8.)]}
  frozen = HERE / 'fixtures/haptic_camera_timer_cadd97de.py'
  exec(compile(frozen.read_text(encoding='utf8'), str(frozen), 'exec'), scope)
  c = state(); pulses = []
  for frame in range(1000):
    c.frame = frame
    if any(scope['tick'](c, NS(activeCarrot=3 if frame >= 20 else 0))):
      pulses.append(frame)
  assert pulses == []


@pytest.mark.parametrize('exit_state', [0, 2, 4])
@pytest.mark.parametrize('leave', [1, 10, 70, 120, 770, 850])
def test_camera_exit_cancels_and_reentry_restarts(leave, exit_state):
  tick = load_tick(); c = state()
  for frame in range(leave + 20):
    c.frame = frame
    assert tick(c, NS(activeCarrot=3 if frame < leave else exit_state)) == ((1, 1) if frame < min(leave, 50) else
      (1, 1) if frame < leave and any(a <= frame < b for a, b in [(100, 150), (500, 550), (600, 650), (750, 800)]) else (0, 0))
  c.frame += 1
  assert tick(c, NS(activeCarrot=3)) == (1, 1)


def test_mode_changes_do_not_retrigger_or_extend_timer():
  tick = load_tick(); c = state(0)
  assert tick(c, NS(activeCarrot=3)) == (0, 0)
  c.frame = 25; c.hapticFeedbackWhenSpeedCamera = 1
  assert tick(c, NS(activeCarrot=3)) == (1, 1)
  assert c.speedCameraHapticEndFrame == 800
  c.frame = 26; c.hapticFeedbackWhenSpeedCamera = 0
  assert tick(c, NS(activeCarrot=3)) == (0, 0)
  c.frame = 100; c.hapticFeedbackWhenSpeedCamera = 1
  assert tick(c, NS(activeCarrot=3)) == (1, 1)
  c.frame = 800
  assert tick(c, NS(activeCarrot=3)) == (0, 0)
  c.frame = 900
  assert tick(c, NS(activeCarrot=3)) == (0, 0)


@pytest.mark.parametrize('warning,blinkers,source,expected', [
  ((1, 0), (False, False), 0, 1), ((1, 0), (True, False), 0, 0),
  ((0, 1), (False, True), 0, 0), ((1, 1), (True, False), 0, 1),
  ((0, 0), (False, False), 1, 1), ((0, 0), (False, False), 0, 0)])
def test_existing_ldw_and_source_vibration_preserved(codec, warning, blinkers, source, expected):
  packer, decode, _ = codec
  packet = next(p for p in formatter(packer, 100, warning, blinkers, source) if p[0] == 0x162)
  assert decode('CCNC_0x162', packet[1])['VIBRATE'] == expected


def test_timer_preserves_preexisting_lane_departure_warning():
  tick = load_tick(); c = state(0)
  assert tick(c, NS(activeCarrot=0), 2, 0) == (2, 0)
  c.frame = 20
  assert tick(c, NS(activeCarrot=3), 2, 0) == (2, 0)


def test_haptic_changes_only_vibrate_and_checksum_not_faults_or_steering(codec):
  packer, decode, _ = codec
  old = formatter(packer, 100)
  packer.counters.clear()
  new = formatter(packer, 100, (1, 1))
  assert len(old) == len(new) == 2
  for before, after in zip(old, new, strict=True):
    assert before[0] == after[0] and before[2] == after[2]
    name = packer.dbc.addr_to_msg[before[0]].name
    a, b = decode(name, before[1]), decode(name, after[1])
    changed = {k for k in a if a[k] != b[k]}
    assert changed == ({'CHECKSUM', 'VIBRATE'} if before[0] == 0x162 else set())
    if before[0] == 0x162:
      assert all(b[k] == 1 for k in ['FAULT_LSS', 'FAULT_DAS', 'FAULT_LFA'])
      assert bytes(x ^ y for x, y in zip(before[1][2:], after[1][2:], strict=True)) == bytes(4) + b'\x10' + bytes(25)


def test_existing_routine_notification_remains_independent_of_camera_mode(codec, monkeypatch):
  module = types.ModuleType('opendbc.car.hyundai.lx3_buttons')
  module.uses_lx3_authority = shared.load(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_buttons.py',
    {'uses_lx3_authority'}, {'LX3_AUTHORITY_FLAG': 2048})['uses_lx3_authority']
  monkeypatch.setitem(sys.modules, module.__name__, module)
  cls = shared.load(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py',
                    {'HapticNotifications'})['HapticNotifications']
  tick = load_tick(); c = state(0); routine = cls()
  packer, decode, _ = codec
  packets_on = 0
  for frame in range(100, 125):
    c.frame = frame
    warning = tick(c, NS(activeCarrot=0))
    notify = routine.update('engage', eligible=True, frame=frame)
    if frame % 5 == 0:
      packet = next(p for p in formatter(packer, frame, warning, notification=notify) if p[0] == 0x162)
      packets_on += decode('CCNC_0x162', packet[1])['VIBRATE']
  assert packets_on == 4


def test_thirty_second_hold_has_only_one_pattern_and_preserves_ldw_between_pulses():
  tick = load_tick(); c = state()
  pulses = []
  for frame in range(3000):
    c.frame = frame
    result = tick(c, NS(activeCarrot=3), 2, 0)
    camera_pulse = frame < 800 and any(a <= frame < b for a, b in [(0, 50), (100, 150), (500, 550), (600, 650), (750, 800)])
    assert result == ((1, 1) if camera_pulse else (2, 0))
    if result == (1, 1):
      pulses.append(frame)
  assert len(pulses) == 250 and pulses[-1] == 799


@pytest.mark.parametrize('blinkers,expected', [((False, False), 1), ((True, False), 1), ((False, True), 1), ((True, True), 0)])
def test_camera_sets_both_warning_sides_with_existing_blinker_semantics(codec, blinkers, expected):
  packer, decode, _ = codec
  packet = next(p for p in formatter(packer, 100, (1, 1), blinkers) if p[0] == 0x162)
  assert decode('CCNC_0x162', packet[1])['VIBRATE'] == expected


def test_warning_values_have_no_steering_message_consumer_and_keep_send_gates():
  tree = ast.parse(CONTROLLER.read_text(encoding='utf8'))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'CarController')
  update = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'update')
  calls = [n for n in ast.walk(update) if isinstance(n, ast.Call) and
           any(isinstance(a, ast.Name) and a.id in ('left_lane_warning', 'right_lane_warning') for a in n.args)]
  assert {ast.unparse(n.func) for n in calls} == {'hyundaicanfd.create_ccnc_messages', 'hyundaican.create_lkas11'}
  allowed_nodes = {node for call in calls for node in ast.walk(call)}
  assert all(node in allowed_nodes for node in ast.walk(update) if isinstance(node, ast.Name) and
             isinstance(node.ctx, ast.Load) and node.id in ('left_lane_warning', 'right_lane_warning'))
  call = next(n for n in calls if ast.unparse(n.func) == 'hyundaicanfd.create_ccnc_messages')
  parents = {child: node for node in ast.walk(update) for child in ast.iter_child_nodes(node)}
  ancestor = call; gates = []
  while ancestor in parents:
    ancestor = parents[ancestor]
    if isinstance(ancestor, ast.If):
      gates.append(ast.unparse(ancestor.test))
  assert 'self.CP.openpilotLongitudinalControl' in gates
  factory = next(n for n in ast.parse(shared.HYUNDAI.read_text(encoding='utf8')).body
                 if isinstance(n, ast.FunctionDef) and n.name == 'create_ccnc_messages')
  assert any(isinstance(n, ast.If) and ast.unparse(n.test) == 'frame % 5 == 0' for n in ast.walk(factory))
  assert any(isinstance(n, ast.If) and ast.unparse(n.test) == 'CS.ccnc_0x162 is not None' for n in ast.walk(factory))
