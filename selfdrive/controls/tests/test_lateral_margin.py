"""Production preference class, without native Params/MPC import requirements."""
import ast
import math
from pathlib import Path
from types import SimpleNamespace as NS

import pytest


def engine():
  path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/lane_planner_2.py'
  node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.ClassDef) and n.name=='LateralMargin')
  scope={'math':math,'DT_MDL':.05}
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
  return scope['LateralMargin']()


def radar(left=False,right=False):
  def car(sign):return NS(status=True,radar=True,radarTrackId=1 if sign==1 else 2,
    dRel=10.,dPath=3.2*sign,yRel=3.2*sign,vLead=12.,vRel=2.,vLat=0.)
  return NS(leadsLeft=[car(1)] if left else [],leadsRight=[car(-1)] if right else [],leadsCutIn=[])


def apply(h,r,i,settings=None,**changes):
  t=1_000_000_000+i*50_000_000
  args=dict(now_ns=t,radar_ns=t,valid=True,lane_mode=True,lat_active=True,blinkers=False,
    steering_pressed=False,maneuver=False,speed=12.,curvature=0.,left_code=21,right_code=10,
    left_occupied=False,right_occupied=False,probabilities=[.95,.95],stds=[.05,.05],
    corridor=[(-1.7,1.7,0.,0.)]*3,legacy_offset=0.)
  args.update(changes)
  return h.update(settings or {'YellowLineBiasCm':5,'NearbyVehicleBiasCm':5,'LateralMarginShadow':0},r,**args)


@pytest.mark.parametrize('left,right,expected',[(False,False,-.05),(True,False,.05),(False,True,-.05),(True,True,0.)])
def test_left_positive_radar_is_opposite_right_positive_path(left,right,expected):
  h=engine();r=radar(left,right)
  values=[apply(h,r,i) for i in range(150)]
  assert values[-1]==pytest.approx(expected)
  assert max(abs(b-a) for a,b in zip(values,values[1:]))<=.05*.05+1e-12


@pytest.mark.parametrize('code,accepted',[(20,True),(21,True),(24,True),(25,True),(22,False),(23,False),(26,False),(27,False),(28,False),(11,False)])
def test_yellow_dashed_solid_double_not_undecided_edge_or_barrier(code,accepted):
  h=engine();r=radar()
  values=[apply(h,r,i,left_code=code) for i in range(150)]
  assert (values[-1]<0) is accepted


@pytest.mark.parametrize('changes',[
  {'left_code':11,'right_code':21},{'right_code':21},{'lane_mode':False},
  {'lat_active':False},{'valid':False},{'blinkers':True},{'maneuver':True},
  {'steering_pressed':True},{'left_occupied':True},{'probabilities':[.79,.95]},
  {'stds':[.16,.05]},{'curvature':-.0003},{'curvature':.002},
  {'corridor':[(-1.4,1.4,0.,0.)]*3},{'legacy_offset':.1},
  {'corridor':[(-1.7,1.7,-.35,0.)]*3},
])
def test_disable_and_corridor_counterexamples(changes):
  h=engine();r=radar()
  assert not any(apply(h,r,i,**changes) for i in range(150))


def test_unknown_geometry_or_oncoming_left_cancels_yellow():
  for changes in ({'dPath':-3.2},{'vLead':-12.},{'dRel':float('nan')}):
    h=engine();r=radar(True)
    for key,value in changes.items():setattr(r.leadsLeft[0],key,value)
    assert not any(apply(h,r,i) for i in range(150))


def test_stationary_right_and_oem_occupancy_alone_do_not_trigger_a_shift():
  h=engine();r=radar(False,True);r.leadsRight[0].vLead=0.
  settings={'YellowLineBiasCm':0,'NearbyVehicleBiasCm':5,'LateralMarginShadow':0}
  assert not any(apply(h,r,i,settings=settings) for i in range(150))
  h=engine()
  assert not any(apply(h,radar(),i,settings=settings,right_occupied=True) for i in range(150))


def test_shadow_never_applies_and_switch_restarts_ramp():
  h=engine();r=radar();s={'YellowLineBiasCm':5,'NearbyVehicleBiasCm':5,'LateralMarginShadow':1}
  assert not any(apply(h,r,i,settings=s) for i in range(150))
  assert h.value==pytest.approx(-.05)
  s['LateralMarginShadow']=0
  assert apply(h,r,150,settings=s)==0.
  assert apply(h,r,151,settings=s)==0.


def test_stale_radar_and_lane_exit_clear_history():
  h=engine();r=radar()
  for i in range(150):apply(h,r,i)
  assert apply(h,r,150,lane_mode=False)==0.
  assert apply(h,r,151)==0.
  for i in range(152,300):apply(h,r,i)
  assert apply(h,r,300,radar_ns=1_000_000_000)==0.


def test_legacy_total_and_final_path_bound_override_filter_history():
  h=engine();r=radar();s={'YellowLineBiasCm':10,'NearbyVehicleBiasCm':0,'LateralMarginShadow':0}
  for i in range(180):apply(h,r,i,settings=s,legacy_offset=-.39)
  assert -.0100000001<=h.value<=0.
  # A newly occupied side vetoes the old filtered term immediately.
  assert apply(h,r,180,settings=s,left_occupied=True)==0.


def test_repeated_packet_cannot_create_a_stable_track():
  h=engine();r=radar(True);s={'YellowLineBiasCm':0,'NearbyVehicleBiasCm':5,'LateralMarginShadow':0}
  for _ in range(100):assert apply(h,r,1,settings=s)==0.


def test_disable_returns_positive_zero_for_noop_caller():
  h=engine()
  value=apply(h,radar(),1,settings={'YellowLineBiasCm':0,'NearbyVehicleBiasCm':0,'LateralMarginShadow':1})
  assert value==0. and math.copysign(1.,value)==1.
