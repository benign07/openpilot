"""Bounded native-path position hold, independent from torque handback timing."""
import ast
import math
from pathlib import Path
import pytest


def engine():
  path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/lane_planner_2.py'
  nodes=[n for n in ast.parse(path.read_text(encoding='utf8')).body if isinstance(n,(ast.ClassDef,ast.FunctionDef)) and n.name in ('DriverPositionHold','path_offset_limits')]
  scope={'math':math};exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
  return scope['DriverPositionHold']()


def step(h,t,**changes):
  args=dict(enabled=True,now_ns=round(t*1e9),valid=True,lane_mode=True,lat_active=True,
    pressed=False,blinkers=False,maneuver=False,observed_position=.08,
    probabilities=[.95,.95],stds=[.05,.05],corridor=[(-1.8,1.8,0.,0.)]*3,
    legacy_offset=0.,occupied=[False,False])
  args.update(changes);return h.update(**args)


def capture(h,position=.08):
  for i in range(10):step(h,9.+i*.05,observed_position=0.)
  for i in range(15):assert step(h,9.5+i*.05,pressed=True,observed_position=position*min(1.,i/4))==0.
  return step(h,10.25,observed_position=position)


def test_three_second_hold_then_five_second_smooth_return_not_point_two_torque_timer():
  h=engine();assert capture(h)==pytest.approx(.08)
  values=[]
  for i in range(1,161):
    age=i*.05;value=step(h,10.25+age);values.append(value)
    if age<=3.:assert value==pytest.approx(.08)
    if age==5.5:assert value==pytest.approx(.04)
  assert values[-1]==0. and h.reason=='completed'
  assert all(b<=a for a,b in zip(values,values[1:]))
  assert max(a-b for a,b in zip(values,values[1:]))<.002


@pytest.mark.parametrize('position',[-.4,-.08,.08,.4])
def test_captured_position_respects_sign_and_ten_centimetre_additional_limit(position):
  h=engine();assert capture(h,position)==pytest.approx(max(-.1,min(.1,position)))


@pytest.mark.parametrize('changes',[
  {'enabled':False},{'lane_mode':False},{'lat_active':False},
  {'blinkers':True},{'maneuver':True},{'occupied':[False,True]},
])
def test_invalid_or_new_occupancy_abandons_old_bias_without_later_resurrection(changes):
  h=engine();capture(h)
  assert step(h,10.3,**changes)==0.
  for i in range(50):assert step(h,10.35+i*.05)==0.


def test_light_contact_without_native_press_or_real_displacement_does_not_capture():
  h=engine()
  for i in range(100):assert step(h,10.+i*.05)==0.
  for i in range(5):step(h,15.+i*.05,pressed=True,observed_position=.08)
  assert step(h,15.25)==0. and h.reason=='no_confirmed_displacement'


def test_lane_relative_target_is_not_added_to_a_changing_existing_bias():
  h=engine();capture(h)
  assert step(h,10.3,legacy_offset=.02)==pytest.approx(.06)
  assert .02+h.value==pytest.approx(.08)


def test_stale_gap_or_backwards_clock_discards_capture():
  for stamp in (9.,11.):
    h=engine();capture(h)
    assert step(h,stamp)==pytest.approx(.0775) and h.reason=='input_gap'
    assert h.released_ns is None


def test_curved_projected_footprint_and_total_legacy_bound_are_not_relaxed():
  h=engine();capture(h)
  assert 0.<=step(h,10.3,corridor=[(-1.8,1.8,0.,.15)]*3)<.002
  h=engine();capture(h)
  assert step(h,10.3,legacy_offset=-.4)==pytest.approx(.1)
  assert h.reason=='holding_3s'


def test_actual_planner_hold_requires_explicit_native_lane_path_and_preserves_setting():
  import importlib.util
  path=Path(__file__).with_name('test_lateral_margin_mode_audit.py')
  spec=importlib.util.spec_from_file_location('position_hold_planner_harness',path)
  harness=importlib.util.module_from_spec(spec);spec.loader.exec_module(harness)
  h=harness.Harness(harness.OFF,lane_speed=50)
  h.response_settings['DriverPositionHold']=1
  h.run(40,centre=0.)
  assert h.planner.useLaneLineSpeedApply==50 and h.planner.lanelines_active
  for i in range(15):h.step(centre=-.08*min(1.,i/4),pressed=True)
  h.step(centre=-.08)
  assert h.planner.driver_position_applied==pytest.approx(.08)
  for i in range(160):h.step(centre=-.08)
  assert h.planner.driver_position_applied==0.
  assert h.params.values['UseLaneLineSpeed']==50


@pytest.mark.parametrize('changes',[{'valid':False},{'probabilities':[.7,.95]},
  {'stds':[.2,.05]},{'observed_position':float('nan')}])
def test_routine_quality_failure_withdraws_gradually_without_reviving_capture(changes):
  h=engine();capture(h)
  assert step(h,10.3,**changes)==pytest.approx(.0775)
  values=[step(h,10.35+i*.05) for i in range(40)]
  assert values[-1]==0. and all(b<=a for a,b in zip(values,values[1:]))


def test_quality_withdrawal_also_enforces_new_narrow_or_curved_corridor_bounds():
  h=engine();capture(h)
  assert step(h,10.3,probabilities=[.7,.95],corridor=[(-1.4,1.4,0.,0.)]*3)==0.
  h=engine();capture(h)
  assert 0.<=step(h,10.3,probabilities=[.7,.95],corridor=[(-1.8,1.8,0.,.15)]*3)<.002


def test_existing_margin_withdrawal_does_not_abort_the_hold_at_the_additional_bound():
  h=engine()
  for i in range(10):step(h,9.+i*.05,observed_position=-.1,legacy_offset=-.1)
  for i in range(15):step(h,9.5+i*.05,pressed=True,observed_position=-.15 if i>4 else -.1,legacy_offset=-.1)
  # 5cm is exactly the minimum confirmation displacement.
  first=step(h,10.25,observed_position=-.15,legacy_offset=-.1)
  assert first==pytest.approx(-.05)
  values=[step(h,10.3+i*.05,legacy_offset=-.1+min(.1,(i+1)*.0025)) for i in range(120)]
  assert min(values)>=-.1 and h.reason=='returning_5s' and not any(v==0. for v in values)


def test_stationary_noisy_lane_estimates_without_position_change_do_not_create_hold():
  import random
  rng=random.Random(271027)
  for std in (.05,.1,.15):
    for episode in range(100):
      h=engine()
      for i in range(40):
        step(h,10.+i*.05,pressed=10<=i<39,observed_position=rng.gauss(0.,std),stds=[std,std])
      assert h.released_ns is None,(std,episode,h.value)
