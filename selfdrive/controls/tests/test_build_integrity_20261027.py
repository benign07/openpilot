"""Compare published29f0 semantics, not a second implementation of this build.

No device I/O or ECU ACK claim. Native MPC comparisons run separately in CI.
"""
import ast
import copy
from functools import lru_cache
import importlib.util
import json
import math
from pathlib import Path
import subprocess
import pytest

ROOT=Path(__file__).resolve().parents[3]
BEFORE='29f0d86f2533cb80945ab3c85d67386f49c311cb'


@lru_cache(None)
def original(name):
  return subprocess.check_output(['git','show',BEFORE+':'+name],cwd=ROOT).decode('utf8')


def harness(name):
  spec=importlib.util.spec_from_file_location('integrity_'+name,Path(__file__).with_name(name+'.py'))
  module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


def original_class(name):
  raw=original('selfdrive/controls/lib/lane_planner_2.py')
  node=next(n for n in ast.parse(raw).body if isinstance(n,ast.ClassDef) and n.name==name)
  scope={'math':math,'DT_MDL':.05}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'published29f0_'+name,'exec'),scope)
  return scope[name]()


def test_settings_preserve_published_contract_except_explicit_model_mode_help():
  from openpilot.selfdrive.carrot.server.services.settings import inject_driving_preferences
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES,CRUISE_BUTTON_PREFERENCES,RESPONSE_PREFERENCES
  raw=original('selfdrive/carrot/server/services/settings.py')
  node=next(n for n in ast.parse(raw).body if isinstance(n,ast.FunctionDef) and n.name=='inject_driving_preferences')
  scope=dict(DRIVING_PREFERENCES=DRIVING_PREFERENCES,CRUISE_BUTTON_PREFERENCES=CRUISE_BUTTON_PREFERENCES,RESPONSE_PREFERENCES=RESPONSE_PREFERENCES)
  exec(compile(ast.Module(body=[node],type_ignores=[]),'published29f0_settings','exec'),scope)
  fixture=json.loads((ROOT/'selfdrive/carrot_settings.json').read_text(encoding='utf8'))
  a=scope['inject_driving_preferences'](copy.deepcopy(fixture));b=inject_driving_preferences(copy.deepcopy(fixture))
  old={p['name']:p for p in a['params']}
  normalized=copy.deepcopy(b)
  changed={'DriverPositionHold','YellowLineBiasCm','NearbyVehicleBiasCm'}
  for option in normalized['params']:
    if option['name'] in changed:
      for field in ('descr','edescr','cdescr'):
        assert '모델 모드' in option[field] and '30km/h부터' not in option[field]
        option[field]=old[option['name']][field]
  assert json.dumps(a,sort_keys=True,ensure_ascii=False).encode()==json.dumps(normalized,sort_keys=True,ensure_ascii=False).encode()
  assert inject_driving_preferences(copy.deepcopy(b))==b


@pytest.mark.parametrize('shadow',[0,1])
def test_shared_corridor_preserves_margin_outputs_and_reasons(shadow):
  t=harness('test_lateral_margin');old=original_class('LateralMargin');new=t.engine()
  settings={'YellowLineBiasCm':5,'NearbyVehicleBiasCm':5,'LateralMarginShadow':shadow}
  for i in range(600):
    r=t.radar(left=120<=i<240,right=300<=i<420)
    changes={}
    if 200<=i<205:changes['probabilities']=[.79,.95]
    if 440<=i<460:changes['blinkers']=True
    if 480<=i<490:changes['valid']=False
    if 540<=i<550:changes['corridor']=[(-1.4,1.4,0.,0.)]*3
    assert t.apply(old,r,i,settings,**changes)==t.apply(new,r,i,settings,**changes)
    assert (old.reason,old.target,old.value,old.evidence)==(new.reason,new.target,new.value,new.evidence)


def test_shared_corridor_preserves_qualified_three_plus_five_hold():
  t=harness('test_driver_position_hold');old=original_class('DriverPositionHold');new=t.engine()
  assert t.capture(old)==t.capture(new)
  for i in range(161):
    stamp=10.3+i*.05
    assert t.step(old,stamp)==t.step(new,stamp)
    assert old.reason==new.reason


def test_withdrawal_narrowing_is_a_deliberate_stricter_change_not_equality_claim():
  t=harness('test_driver_position_hold');old=original_class('DriverPositionHold');new=t.engine()
  t.capture(old);t.capture(new)
  changed=dict(probabilities=[.7,.95],corridor=[(-1.4,1.4,0.,0.)]*3)
  assert t.step(old,10.3,**changed)>0.
  assert t.step(new,10.3,**changed)==0.


def test_redundant_yield_latch_cleanup_preserves_unambiguous_driver_traces():
  t=harness('test_lx3_driver_recovery');before=t.Driver();after=t.Driver()
  before.program=t.program(source=original('opendbc_repo/opendbc/car/hyundai/carcontroller.py'))
  for force,pressed,active in [(v,p,a) for v,p in ((0.,False),(100.,False),(-200.,False),(300.,True),(100.,False))
                                for a in (True,False,True) for _ in range(100)]:
    assert before.step(force,pressed=pressed,active=active)==after.step(force,pressed=pressed,active=active)


def test_signed_noise_fix_reproduces_published_failure_before_and_avoids_it_after():
  t=harness('test_lx3_driver_recovery');before=t.Driver();after=t.Driver()
  before.program=t.program(source=original('opendbc_repo/opendbc/car/hyundai/carcontroller.py'))
  for _ in range(100):before.step(0.);after.step(0.)
  for i in range(1000):
    force=230.*(-1 if i%2 else 1)
    old=before.step(force);new=after.step(force)
  assert old==25. and new==250.


def test_unqualified_state_machine_is_absent_from_runtime_but_normal_decision_is_recorded():
  for name in ('approach_comfort','longitudinal_planner'):
    text=(ROOT/('selfdrive/controls/lib/'+name+'.py')).read_text(encoding='utf8')
    assert 'DepartureThresholdReview' not in text and 'departure_review' not in text
  tree=ast.parse((ROOT/'selfdrive/controls/lib/longitudinal_planner.py').read_text(encoding='utf8'))
  keys={k.value for n in ast.walk(tree) if isinstance(n,ast.Dict) for k in n.keys if isinstance(k,ast.Constant)}
  assert {'planned_accel','fcw','planner_mode','stop_decision','v_at_action','v_at_action_plus_1s','mpc_should_stop'}<=keys
