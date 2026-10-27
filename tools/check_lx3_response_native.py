"""Isolated real longitudinal solver comparison; never a vehicle tool."""
import argparse
import json
import os
import platform
from pathlib import Path
from types import SimpleNamespace as NS
import numpy as np


def main():
  p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True)
  p.add_argument('--isolated-local',action='store_true');args=p.parse_args()
  assert os.environ.get('GITHUB_ACTIONS')=='true' or (args.isolated_local and
    platform.machine().lower() in ('x86_64','amd64') and not Path('/TICI').exists())
  from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import LongitudinalMpc,N,T_IDXS
  from tools.lx3_departure_review import DepartureThresholdReview
  from openpilot.selfdrive.controls.lib.drive_helpers import CONTROL_N,get_accel_from_plan
  from openpilot.selfdrive.modeld.constants import ModelConstants
  c=NS(get_T_FOLLOW=lambda *a:.7,dynamic_t_follow=lambda tf,*a:tf,flex_gap_nominal_tf=.7,
       comfort_brake=2.4,stop_distance=5.5,j_lead_factor=0.,mode='acc',v_cruise=18.,stop_dist=1000.,
       trafficStopDistanceAdjust=0.,autoNaviSpeedDecelRate=1.)
  old,new=LongitudinalMpc(),LongitudinalMpc();helper=DepartureThresholdReview();ts=ModelConstants.T_IDXS[:CONTROL_N]
  differences=[];active=0;maximum_a_difference=0.
  for i in range(160):
    elapsed=max(0.,(i-40)*.05);opening=.4 if i>=40 else 0.
    lead=NS(status=True,radar=True,radarTrackId=7,dRel=6.+opening*elapsed,vLead=opening,
      vRel=opening,aLeadK=0.,aLeadTau=1.5,jLead=0.,modelProb=.99)
    radar=NS(leadOne=lead,leadTwo=NS(**{**vars(lead),'status':False}))
    plans=[]
    for m in (old,new):
      m.set_cur_state(0.,0.);m.set_weights();m.set_accel_limits(-2.,1.3)
      m.update(c,False,radar,18.,*[np.zeros(N+1) for _ in range(4)])
      assert m.solution_status==0 and np.isfinite(m.x_sol).all()
      plans.append((np.interp(ts,T_IDXS,m.v_solution),np.interp(ts,T_IDXS,m.a_solution)))
    np.testing.assert_array_equal(old.params,new.params);np.testing.assert_array_equal(old.yref,new.yref)
    np.testing.assert_array_equal(old.x_sol,new.x_sol);np.testing.assert_array_equal(old.u_sol,new.u_sol)
    threshold=helper.threshold(.5,radar,enabled=True,eligible=True,stamp_ns=1_000_000_000+i*50_000_000,
      v_ego=0.,standstill=True,starting_speed=.1,stop_distance=c.stop_distance,
      planned_accel=float(np.interp(.05,ts,plans[1][1])))
    a=get_accel_from_plan(*plans[0],ts,action_t=.55,vEgoStopping=.5)
    b=get_accel_from_plan(*plans[1],ts,action_t=.55,vEgoStopping=threshold)
    assert a[0]==b[0] and a[2:]==b[2:]
    active+=threshold<.5;maximum_a_difference=max(maximum_a_difference,abs(a[0]-b[0]))
    if a[1]!=b[1]:differences.append(i)
    assert not (b[1] and not a[1])
  assert active>0
  result={'actual_linked_solver_cycles_identical':160,'threshold_candidate_cycles':active,
    'stop_boolean_difference_cycles':differences,'maximum_acceleration_target_difference':maximum_a_difference,
    'physical_obstacles_costs_and_solver_identical':True,
    'candidate_applied_to_control':False,'candidate_qualified':False,
    'scope':'synthetic stationary ego and gently opening lead; not a plant/hold/actuator or road improvement proof'}
  args.output.write_text(json.dumps(result,indent=2));print(json.dumps(result))


if __name__=='__main__':main()
