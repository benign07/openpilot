"""Production sound/notification paths with inert audio/GPIO, never device I/O."""
import ast
import copy
from enum import IntEnum
import importlib.util
import json
import logging
from pathlib import Path
import re
import sys
from types import SimpleNamespace as NS

import numpy as np
import pytest

ROOT = Path(__file__).resolve().parents[3]
HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location('independent_notification_helpers', HERE / 'test_surround_display.py')
shared = importlib.util.module_from_spec(spec); sys.modules[spec.name] = shared; spec.loader.exec_module(shared)
enum_text = re.search(r'enum AudibleAlert\s*\{([^}]*)\}', (ROOT / 'cereal/car.capnp').read_text(encoding='utf8'), re.S).group(1)
Alert = IntEnum('AudibleAlert', {name: int(value) for name, value in re.findall(r'(\w+)\s+@(\d+)', enum_text)})


def output_classes(params, clock, writes):
  scope = {'AudibleAlert': Alert, 'Params': lambda: params, 'time': clock, 'logging': logging}
  tree = ast.parse((ROOT / 'selfdrive/controls/beep.py').read_text(encoding='utf8'))
  nodes = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'Beepd' or
           isinstance(n, ast.Assign) and any(isinstance(t, ast.Name) and t.id in ('OPTIONAL_ALERTS', 'REPEAT_ALERTS') for t in n.targets)]
  exec(compile(ast.Module(body=nodes, type_ignores=[]), '<production beep>', 'exec'), scope)
  beep = scope['Beepd']; beep.enable_gpio = lambda self: None
  beep._write_gpio = lambda self, on: writes.append(on)
  scope.update(np=np, retry=lambda **kwargs: lambda fn: fn, sound_list={a: ('unused', 1, 1.) for a in Alert},
               check_selfdrive_timeout_alert=lambda sm: False)
  sound = shared.load(ROOT / 'selfdrive/ui/soundd.py', {'Soundd'}, scope)['Soundd']
  return beep, sound


def setup(mute, hardware=0):
  p = NS(mute=mute, hardware=hardware, haptic=False)
  p.get_bool = lambda key: p.mute if key == 'C3xLiteBuzzerMute' else p.haptic
  p.get_int = lambda key: p.hardware if key == 'HardwareC3xLite' else 100
  clock = NS(value=0.); clock.monotonic = lambda: clock.value
  writes = []; beep_cls, sound_cls = output_classes(p, clock, writes)
  b = beep_cls()
  s = sound_cls.__new__(sound_cls)
  s.params = p; s.routine_mute = False; s.mute_checked = float('-inf')
  s.current_alert = Alert.none; s.current_sound_frame = 0; s.current_volume = 1.
  s.loaded_sounds = {a: np.ones(100, dtype=np.float32) for a in Alert}
  s.selfdrive_timeout_alert = False; s.carrot_count_down = 0
  class SM(dict): pass
  sm = SM(selfdriveState=NS(alertSound=NS(raw=Alert.engage)), carrotMan=NS(leftSec=0))
  sm.updated = {'selfdriveState': True}
  return p, clock, writes, b, s, sm


@pytest.mark.parametrize('hardware', [0, 1])
@pytest.mark.parametrize('mute,haptic', [(False, False), (False, True), (True, False), (True, True)])
@pytest.mark.parametrize('backend', ['gpio', 'speaker'])
def test_sound_and_motor_request_switches_are_independent(hardware, mute, haptic, backend):
  p, clock, writes, b, s, sm = setup(mute, hardware); p.haptic = haptic
  if backend == 'gpio':
    b.get_audible_alert(sm, lambda unused: False); b.tick(0.)
    audible = bool(writes[-1])
  else:
    s.get_audible_alert(sm)
    audible = bool(np.any(s.get_sound_data(10)))
  assert audible is (not mute)
  assert sm['selfdriveState'].alertSound.raw == Alert.engage
  # Execute the real parameter-read expression without a controller/vehicle.
  tree = ast.parse((ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py').read_text(encoding='utf8'))
  assignment = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign) and isinstance(n.value, ast.Call) and
                    any(ast.unparse(t) == 'self.haptic_notifications_enabled' for t in n.targets))
  c = NS(); exec(compile(ast.Module(body=[assignment], type_ignores=[]), '<production haptic switch>', 'exec'), {'self': c, 'params': p})
  assert c.haptic_notifications_enabled is haptic
  cls = shared.load(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py', {'HapticNotifications'})['HapticNotifications']
  assert cls().update('engage', eligible=c.haptic_notifications_enabled, frame=100) is haptic


def test_live_speaker_mute_stops_optional_and_protected_warning_still_plays():
  p, clock, _, _, s, sm = setup(False)
  s.get_audible_alert(sm); assert np.any(s.get_sound_data(10))
  p.mute = True; clock.value = .5; sm.updated['selfdriveState'] = False
  s.get_audible_alert(sm); assert not np.any(s.get_sound_data(10))
  sm.updated['selfdriveState'] = True; sm['selfdriveState'].alertSound.raw = Alert.warningImmediate
  s.get_audible_alert(sm); assert np.any(s.get_sound_data(10))


@pytest.mark.parametrize('name', ['disengage', 'refuse', 'prompt', 'promptRepeat', 'promptDistracted',
                               'warningImmediate', 'warningSoft', 'bsdWarning', 'audio1'])
def test_both_sound_backends_preserve_existing_protected_warnings(name):
  _, _, writes, b, s, sm = setup(True)
  sm['selfdriveState'].alertSound.raw = getattr(Alert, name)
  b.get_audible_alert(sm, lambda unused: False); b.tick(0.)
  s.get_audible_alert(sm)
  assert writes[-1] and np.any(s.get_sound_data(10))


def test_menu_has_two_independent_options_and_preserves_hardware_parent():
  data = json.loads((ROOT / 'selfdrive/carrot_settings.json').read_text(encoding='utf8'))
  names = ('C3xLiteBuzzerMute', 'C3xLiteHapticNotifications')
  before = {p['name']: {k: p[k] for k in ('min', 'max', 'default')} for p in data['params'] if p['name'] in names}
  inject = shared.load(ROOT / 'selfdrive/carrot/server/services/settings.py', {'inject_driving_preferences'},
                       {'DRIVING_PREFERENCES': {},'CRUISE_BUTTON_PREFERENCES': {}})['inject_driving_preferences']
  inject(data)
  def leaves(nodes):
    for node in nodes:
      yield from node.get('params', [])
      yield from leaves(node.get('groups', []))
  occurrences = list(leaves(data['menu']))
  assert all(occurrences.count(n) == 1 for n in (*names, 'HardwareC3xLite'))
  assert data['menu'][0]['groups'][0]['groups'][0]['id'] == 'notification_methods'
  haptic_names = [p['name'] for p in data['params'] if p['name'].startswith('C3xLiteHaptic')]
  assert set(haptic_names) <= set(data['menu'][0]['groups'][0]['groups'][0]['params'])
  assert all(occurrences.count(n) == 1 for n in haptic_names)
  after = {p['name']: {k: p[k] for k in ('min', 'max', 'default')} for p in data['params'] if p['name'] in names}
  assert after == before
  snapshot = copy.deepcopy(data); inject(data); assert data == snapshot


def test_muted_optional_cannot_preempt_a_protected_first_play():
  _, _, _, _, s, sm = setup(True)
  sm['selfdriveState'].alertSound.raw = Alert.warningImmediate
  s.get_audible_alert(sm); s.get_sound_data(10)
  sm['selfdriveState'].alertSound.raw = Alert.engage
  s.get_audible_alert(sm)
  assert s.current_alert == Alert.warningImmediate
  assert np.any(s.get_sound_data(100))
  s.get_audible_alert(sm)
  assert s.current_alert == Alert.none


def test_mute_refresh_bound_and_unmute_ongoing_request():
  p, clock, _, _, s, sm = setup(False)
  s.get_audible_alert(sm); s.get_sound_data(10)
  p.mute = True; clock.value = .49; sm.updated['selfdriveState'] = False
  s.get_audible_alert(sm); assert np.any(s.get_sound_data(10))
  clock.value = .5
  s.get_audible_alert(sm); assert not np.any(s.get_sound_data(10))
  p.mute = False; clock.value = 1.; sm.updated['selfdriveState'] = True
  s.get_audible_alert(sm); assert np.any(s.get_sound_data(10))


def test_countdown_and_timeout_remain_audible_with_routine_mute():
  _, _, _, _, s, sm = setup(True)
  sm['carrotMan'].leftSec = 2
  s.get_audible_alert(sm)
  assert s.current_alert == Alert.none and s.carrot_count_down == 0
  sm['selfdriveState'].alertSound.raw = Alert.none
  s.get_audible_alert(sm)
  assert s.current_alert == Alert.audio2 and np.any(s.get_sound_data(10))
  sm.updated['selfdriveState'] = False
  s.get_audible_alert.__func__.__globals__['check_selfdrive_timeout_alert'] = lambda unused: True
  s.get_audible_alert(sm)
  assert s.current_alert == Alert.warningImmediate and np.any(s.get_sound_data(10))


def test_haptic_request_retains_profile_can_control_and_blinker_gates():
  tree = ast.parse((ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py').read_text(encoding='utf8'))
  call = next(n for n in ast.walk(tree) if isinstance(n, ast.Call) and ast.unparse(n.func) == 'self.haptic_notifications.update')
  condition = next(k.value for k in call.keywords if k.arg == 'eligible')
  compiled = compile(ast.Expression(condition), '<production notification eligibility>', 'eval')
  for change in [None, 'profile', 'setting', 'can', 'timeout', 'inactive', 'left', 'right']:
    cs = NS(out=NS(canValid=True, canTimeout=False, leftBlinker=False, rightBlinker=False))
    cc = NS(latActive=True, longActive=False); c = NS(haptic_notifications_enabled=True); guard = True
    if change == 'profile': guard = False
    elif change == 'setting': c.haptic_notifications_enabled = False
    elif change == 'can': cs.out.canValid = False
    elif change == 'timeout': cs.out.canTimeout = True
    elif change == 'inactive': cc.latActive = False
    elif change == 'left': cs.out.leftBlinker = True
    elif change == 'right': cs.out.rightBlinker = True
    assert eval(compiled, {'self': c, 'CS': cs, 'CC': cc, 'lx3_guard': guard}) is (change is None)


def test_menu_preserves_all_existing_parameter_identities_and_favorite_names():
  data = json.loads((ROOT / 'selfdrive/carrot_settings.json').read_text(encoding='utf8'))
  identity = lambda d: {p['name']: tuple(p.get(k) for k in ('min', 'max', 'default', 'unit')) for p in d['params']}
  before = identity(data); favorites = ['HardwareC3xLite', 'C3xLiteBuzzerMute', 'C3xLiteHapticNotifications']
  inject = shared.load(ROOT / 'selfdrive/carrot/server/services/settings.py', {'inject_driving_preferences'},
                       {'DRIVING_PREFERENCES': {},'CRUISE_BUTTON_PREFERENCES': {}})['inject_driving_preferences']
  inject(data)
  assert {k:v for k,v in identity(data).items() if k in before} == before
  assert set(identity(data))-set(before) == {'NotificationBorderPulse'}
  assert all(name in identity(data) for name in favorites)
