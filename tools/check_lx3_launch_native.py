"""CI-only before/after launch ceilings with the actual linked Carrot MPC.

Fixed-input solves and a simple ideal kinematic plant are component evidence;
neither measures physical brake release, actuator response or road comfort.
"""
import argparse
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS

import numpy as np


def main():
  assert os.environ.get('GITHUB_ACTIONS') == 'true', 'Isolated CI only; never a vehicle tool'
  parser=argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
  root=Path(__file__).resolve().parents[1]
  fixture=root/'tools/fixtures/lx3_launch_737d718a_before.txt';raw=fixture.read_bytes()
  digest=hashlib.sha256(raw).hexdigest()
  assert digest=='9eabe17f415b2e0108779e99deafdcb81f16f4ee773dc67b45eb6c1d9db83d3c'
  with tempfile.TemporaryDirectory(prefix='lx3-launch-ci-') as directory:
    os.environ['PARAMS_ROOT']=directory
    from cereal import log
    from openpilot.common.params import Params
    from openpilot.selfdrive.carrot.carrot_functions import CarrotPlanner,DrivingMode
    from openpilot.selfdrive.controls.lib.approach_comfort import LaunchFollow,clamp
    from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import LongitudinalMpc,N,T_IDXS
    from openpilot.selfdrive.controls.lib.longitudinal_planner import limit_accel_in_turns
    Params().put_int('MyDrivingMode',3)
    scope={'math':math};scope['clamp']=clamp
    exec(compile(raw,str(fixture),'exec'),scope)
    before_class=scope['LaunchFollow'];personality=log.LongitudinalPersonality.aggressive

    def carrot():
      c=CarrotPlanner();c.myDrivingMode=DrivingMode(3);c.mySafeFactor=1.
      c.tFollowGap1,c.tFollowGap2,c.tFollowGap3,c.tFollowGap4=.7,.8,1.,1.15
      c.personality=personality;c.t_follow_last=c._clip_t_follow(.7)
      c.mode,c.v_cruise,c.stop_dist='acc',18.,1000.
      c.stop_distance=5.5;c.comfort_brake=c.comfortBrake;c.j_lead_factor=0.
      for i,value in enumerate((1.3,1.3,1.1,1.,.9,.75,.6)):
        setattr(c,'cruiseMaxVals'+str(i),value)
      return c

    def radar(v,vl,distance,al):
      lead=NS(status=True,radar=True,radarTrackId=1,dRel=distance,vLead=vl,
              vRel=vl-v,aLeadK=al,aLeadTau=1.5,jLead=0.,modelProb=.99)
      return NS(leadOne=lead,leadTwo=NS(**{**vars(lead),'status':False}),leadsCutIn=[],leadsCenter=[])

    def ceiling(helper,c,r,v,stamp,stopping=False):
      table=float(c.get_carrot_accel(v))
      requested=helper.update(10,r,stamp_ns=stamp,eligible=True,should_stop=stopping,
        v_ego=v,desired_distance=c.stop_distance+v*max(.3,c.t_follow_last),
        table_max=table,approach_active=False)
      assert table<=requested<=max(table,min(1.6,table*1.1))+1e-9
      return table,float(limit_accel_in_turns(v,0.,[-2.,requested],4.)[1])

    def solve(m,c,r,v,a,maximum):
      m.set_weights(jerk_factor=.5)
      m.set_accel_limits(-2.,maximum);m.set_cur_state(v,a)
      m.update(c,False,r,c.v_cruise,*[np.zeros(N+1) for _ in range(4)],personality=personality)
      assert m.solution_status==0 and np.isfinite(m.x_sol).all() and np.isfinite(m.u_sol).all()
      assert (m.v_solution>=-1e-3).all()
      value=float(np.interp(.05,T_IDXS,m.a_solution))
      assert math.isfinite(value)
      return value

    fixed=[]
    for name,cls in (('before',before_class),('after',LaunchFollow)):
      c,m,h=carrot(),LongitudinalMpc(),cls()
      for i in range(30):
        r=radar(0.,0.,8.3,0.);ceiling(h,c,r,0.,1_000_000_000+i*50_000_000,True)
      outputs=[];caps=[];reasons=[]
      for i in range(80):
        v=2.;relative=.6+.6*i*.05
        # Identical independent snapshots, not a kinematic trajectory.
        # Table target+1.4m is above the new1m gate, below the old2m gate.
        distance=c.stop_distance+v*max(.3,c.t_follow_last)+1.4
        r=radar(v,v+relative,distance,.6)
        _,cap=ceiling(h,c,r,v,2_500_000_000+i*50_000_000)
        outputs.append(solve(m,c,r,v,.6,cap));caps.append(cap);reasons.append(h.reason)
      fixed.append({'variant':name,'max_ceiling':max(caps),'final_ceiling':caps[-1],
                    'final_planned_accel':outputs[-1],'plan_last20_mean':float(np.mean(outputs[-20:])),
                    'reasons':sorted(set(reasons)),'solver_status':m.solution_status})
    assert fixed[1]['max_ceiling']>fixed[0]['max_ceiling']+.1,fixed
    assert fixed[1]['plan_last20_mean']>fixed[0]['plan_last20_mean'],fixed

    plant=[]
    for name,cls in (('before',before_class),('after',LaunchFollow)):
      c,m,h=carrot(),LongitudinalMpc(),cls()
      v=vl=a=0.;distance=5.5;peak=distance;minimum=distance;jerk=0.;first_move=None
      caps=[];growth=0
      for i in range(400):
        t=i*.05;al=1.8 if 2.<=t<7.5 else 0.
        r=radar(v,vl,distance,al)
        table,cap=ceiling(h,c,r,v,1_000_000_000+i*50_000_000,stopping=t<2.)
        next_a=solve(m,c,r,v,a,cap)
        next_v=max(0.,v+next_a*.05);next_vl=max(0.,vl+al*.05)
        distance+=((vl+next_vl)-(v+next_v))*.025
        minimum=min(minimum,distance);peak=max(peak,distance)
        assert distance>0.,('ideal_plant_collision',name,t)
        if next_v>.5 and first_move is None:first_move=t
        jerk=max(jerk,abs(next_a-a)/.05);caps.append(cap-table)
        growth+=h.reason=='opening_growth_allowance'
        v,vl,a=next_v,next_vl,next_a
      plant.append({'variant':name,'max_gap_m':peak,'minimum_gap_m':minimum,'final_gap_m':distance,
                    'ego_over_0_5_s':first_move,'maximum_jerk':jerk,'max_allowance':max(caps),
                    'growth_cycles':growth,'solver_status':m.solution_status})
    # Report gap/jerk values, including an adverse/no improvement outcome.
    # A changed ceiling alone is not a closed-loop improvement assertion.
    result={'baseline_commit':'737d718a5609630b4ef81a35c4e44229a25c1ac9',
      'baseline_fixture_sha256':digest,'source_sha256':hashlib.sha256((root/'selfdrive/controls/lib/approach_comfort.py').read_bytes()).hexdigest(),
      'fixed_input_linked_mpc':fixed,'ideal_kinematic_launch':plant,
      'ideal_gap_peak_change_after_minus_before_m':plant[1]['max_gap_m']-plant[0]['max_gap_m'],
      'scope':'Actual linked MPC components with fixed snapshots and an ideal synthetic plant; not full host IPC, physical actuator/hold release, real-road gap or safety qualification.'}
  args.output.write_text(json.dumps(result,indent=2)+'\n',encoding='utf8');print(json.dumps(result))


if __name__=='__main__':main()
