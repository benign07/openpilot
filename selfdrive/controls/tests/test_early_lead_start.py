"""Unqualified shadow review; production stop policy must remain unchanged."""
from types import SimpleNamespace as NS
import ast
from pathlib import Path
import numpy as np
import pytest
from openpilot.selfdrive.controls.lib.approach_comfort import DepartureThresholdReview,ResponsePreferences,DrivingPreferences

path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/drive_helpers.py'
node=next(n for n in ast.parse(path.read_text(encoding='utf8')).body if isinstance(n,ast.FunctionDef) and n.name=='get_accel_from_plan')
scope={'np':np,'DT_MDL':.05}
exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
get_accel_from_plan=scope['get_accel_from_plan']


def call(h,t,**changes):
  lead=NS(status=True,radar=True,radarTrackId=7,modelProb=.95,dRel=6.,vRel=0.,vLead=0.,aLeadK=0.)
  supplied=changes.pop('radar',None)
  if isinstance(supplied,bool):lead.radar=supplied;supplied=None
  for key in list(changes):
    if hasattr(lead,key):setattr(lead,key,changes.pop(key))
  args=dict(default=.5,radar=supplied if supplied is not None else NS(leadOne=lead,leadTwo=NS(status=False)),enabled=True,eligible=True,
    stamp_ns=round(t*1e9),v_ego=0.,standstill=True,starting_speed=.1,stop_distance=5.5,planned_accel=.2)
  args.update(changes);return h.threshold(**args)


def qualify(h):
  for i in range(25):assert call(h,10.+i*.05)==.5
  values=[call(h,11.25+i*.05,dRel=6.+i*.04,vRel=.8,vLead=.8) for i in range(12)]
  assert values[-1]==.25
  return 11.8


def test_confirmed_departure_changes_only_stop_boolean_on_the_same_mpc_trajectory():
  h=DepartureThresholdReview();qualify(h)
  # Exact production interpolation: an already positive plan between .25/.5.
  times=[0.,.5,1.,1.5,2.];speeds=[0.,.15,.3,.45,.6];accels=[.3]*5
  old=get_accel_from_plan(speeds,accels,times,action_t=.55,vEgoStopping=.5)
  new=get_accel_from_plan(speeds,accels,times,action_t=.55,vEgoStopping=.25)
  assert old[1] and not new[1]
  assert old[0]==new[0] and old[2:]==new[2:]


@pytest.mark.parametrize('changes',[{'enabled':False},{'eligible':False},{'status':False},
  {'radar':False},{'modelProb':.8},{'radarTrackId':8},{'vRel':-.2},{'vLead':0.},
  {'aLeadK':-1.},{'dRel':4.5},{'v_ego':.5},{'planned_accel':0.},
  {'dRel':float('nan')}, {'default':.2}])
def test_lost_conditions_withdraw_and_never_force_a_departure(changes):
  h=DepartureThresholdReview();t=qualify(h)
  assert call(h,t+.05,**changes)==changes.get('default',.5)


def test_cached_radar_cannot_confirm_and_negative_plan_cancels_even_on_cached_radar():
  h=DepartureThresholdReview()
  for i in range(100):assert call(h,10.)==.5
  t=qualify(DepartureThresholdReview())
  h=DepartureThresholdReview();qualify(h)
  assert call(h,t,dRel=6.44,vRel=.8,vLead=.8,planned_accel=-.1)==.5
  assert not h.active


def test_stationary_lead_noise_and_one_spike_never_select_lower_threshold():
  h=DepartureThresholdReview()
  for i in range(100):assert call(h,10.+i*.05,dRel=6.+(.4 if i==35 else 0.),vRel=.9 if i==35 else 0.)==.5


def test_stationary_observations_with_negative_acceleration_still_establish_departure_reference():
  h=DepartureThresholdReview()
  for i in range(25):assert call(h,10.+i*.05,planned_accel=-.1)==.5
  values=[call(h,11.25+i*.05,dRel=6.+i*.04,vRel=.8,vLead=.8) for i in range(12)]
  assert values[-1]==.25


def test_new_store_defaults_off_and_does_not_invalidate_rollback_preferences(tmp_path):
  old=tmp_path/'old.json';old.write_text('{"schema":1,"values":{"LaunchFollowStrength":10}}')
  raw=old.read_bytes();new=ResponsePreferences(tmp_path/'response.json')
  assert new.read()=={'DriverPositionHold':0}
  new.set('DriverPositionHold',1)
  with pytest.raises(ValueError,match='unknown'):
    new.set('DepartureThresholdReview',1)
  assert old.read_bytes()==raw and DrivingPreferences(old).read()['LaunchFollowStrength']==10
  assert not list(tmp_path.glob('*.tmp'))


def test_actual_starting_point_one_does_not_drop_latch_before_original_stop_threshold_clears():
  h=DepartureThresholdReview();t=qualify(h)
  assert call(h,t+.05,dRel=6.46,vRel=.28,vLead=.4,v_ego=.12,standstill=False)==.25
  assert call(h,t+.1,dRel=6.48,vRel=.1,vLead=.6,v_ego=.5,standstill=False)==.5


def test_duplicate_secondary_track_is_not_a_new_close_obstacle():
  h=DepartureThresholdReview()
  for i in range(25):call(h,10.+i*.05)
  for i in range(12):
    lead=NS(status=True,radar=True,radarTrackId=7,modelProb=.95,dRel=6.+i*.04,vRel=.8,vLead=.8,aLeadK=0.)
    value=call(h,11.25+i*.05,radar=NS(leadOne=lead,leadTwo=lead))
  assert value==.25


def test_unqualified_candidate_cannot_be_enabled_or_change_actual_planner_stop_threshold():
  from openpilot.selfdrive.controls.lib.approach_comfort import RESPONSE_PREFERENCES
  assert 'EarlyLeadStart' not in RESPONSE_PREFERENCES
  source=path.with_name('longitudinal_planner.py')
  tree=ast.parse(source.read_text(encoding='utf8'))
  calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='get_accel_from_plan']
  assert len(calls)==1
  threshold=next(k.value for k in calls[0].keywords if k.arg=='vEgoStopping')
  assert isinstance(threshold,ast.Name) and threshold.id=='vEgoStopping'
