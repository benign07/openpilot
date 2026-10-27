"""Execute production UI status logic without graphics/native IPC or a vehicle."""
import ast
from enum import Enum
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

SOURCE = Path(__file__).resolve().parents[1] / 'ui_state.py'


class UIStatus(Enum):
  DISENGAGED = 'disengaged'
  ENGAGED = 'engaged'
  OVERRIDE = 'override'


def harness():
  tree = ast.parse(SOURCE.read_text(encoding='utf8'))
  original = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'UIState')
  methods = [n for n in original.body if isinstance(n, ast.FunctionDef) and n.name in ('_update_status', 'engaged')]
  cls = ast.ClassDef(name='StatusHarness', bases=[], keywords=[], body=methods, decorator_list=[])
  state = NS(preEnabled='preEnabled', overriding='overriding')
  scope = {'UIStatus': UIStatus, 'log': NS(SelfdriveState=NS(OpenpilotState=state)),
           'time': NS(monotonic=lambda: 100.)}
  exec(compile(ast.fix_missing_locations(ast.Module(body=[cls], type_ignores=[])), str(SOURCE), 'exec'), scope)
  h = scope['StatusHarness']()
  class SM(dict):
    pass
  h.sm = SM(carControl=NS(latActive=False), selfdriveState=NS(enabled=False, state='disabled'),carParams=NS(carFingerprint='KIA_EV9'))
  h.sm.updated = {'selfdriveState': False, 'carControl': True}
  h.sm.alive = {'carControl': True}
  h.sm.valid = {'carControl': True}
  h.sm.logMonoTime = {'carControl': 100_000_000_000}
  h.sm.frame = 10
  h.started = h._started_prev = True
  h.lat_active = h._engaged_prev = False
  h.status = UIStatus.DISENGAGED
  h._engaged_transition_callbacks = []
  h._offroad_transition_callbacks = []
  return h


def test_lateral_only_updates_without_a_new_longitudinal_state_message():
  h = harness()
  h.sm['carControl'].latActive = True
  h._update_status()
  assert h.lat_active
  assert h.status == UIStatus.DISENGAGED  # Lower SCC border stays disengaged.
  assert not h.engaged  # Do not redefine the control-facing property to paint a border.


@pytest.mark.parametrize('condition', ['stale', 'future', 'invalid', 'not_alive', 'offroad'])
def test_old_lateral_green_clears_without_waiting_for_selfdrive_state(condition):
  h = harness()
  h.lat_active = h.sm['carControl'].latActive = True
  if condition == 'stale': h.sm.logMonoTime['carControl'] = 99_500_000_000
  elif condition == 'future': h.sm.logMonoTime['carControl'] = 100_100_000_000
  elif condition == 'invalid': h.sm.valid['carControl'] = False
  elif condition == 'not_alive': h.sm.alive['carControl'] = False
  else: h.started = False
  h._update_status()
  assert not h.lat_active


def test_actual_lateral_off_clears_even_with_cruise_enabled():
  h = harness()
  h.lat_active = True
  h.sm.updated['selfdriveState'] = True
  h.sm['selfdriveState'].enabled = True
  h.sm['selfdriveState'].state = 'enabled'
  h._update_status()
  assert not h.lat_active and h.engaged
  assert h.status == UIStatus.ENGAGED


@pytest.mark.parametrize('state', ['preEnabled', 'overriding'])
def test_longitudinal_override_priority_is_unchanged(state):
  h = harness()
  h.sm.updated['selfdriveState'] = True
  h.sm['selfdriveState'].state = state
  h.sm['carControl'].latActive = True
  h._update_status()
  assert h.lat_active and h.status == UIStatus.OVERRIDE


def test_button_intent_or_retained_permission_does_not_mean_active_steering():
  h = harness()
  h.sm['carState'] = NS(latEnabled=True, lx3Authority=NS(allowed=1))
  h._update_status()
  assert not h.lat_active


@pytest.mark.parametrize('torque,limited,expected',[(25.,False,True),(0.,False,False),(.9,False,False),(250.,True,False),(float('nan'),False,False)])
def test_lx3_host_request_only_goes_green_with_nonzero_prepared_unlimited_output(torque,limited,expected):
  h=harness();h.sm['carControl'].latActive=True
  h.sm['carParams'].carFingerprint='HYUNDAI_PALISADE_LX3_HEV'
  h.sm['carOutput']=NS(actuatorsOutput=NS(torqueOutputCan=torque,lx3AngleLimited=limited))
  h.sm.alive['carOutput']=h.sm.valid['carOutput']=True;h.sm.logMonoTime['carOutput']=100_000_000_000
  h._update_status();assert h.lat_active is expected
  h.sm.logMonoTime['carOutput']=99_500_000_000
  h._update_status();assert not h.lat_active
