"""Isolated CI: real solver noninterference and PC replay qualification."""
import argparse
import json
import os
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np


def main():
  assert os.environ.get('GITHUB_ACTIONS')=='true', 'PC CI only, never run on a vehicle'
  p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);args=p.parse_args()
  from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import LongitudinalMpc,N,T_IDXS
  from openpilot.selfdrive.controls.lib.approach_comfort import ApproachComfort,ApproachSettings,traffic_ceiling_comparison,LongitudinalEvidence
  from openpilot.selfdrive.controls.lib.drive_helpers import CONTROL_N,get_accel_from_plan
  from openpilot.selfdrive.modeld.constants import ModelConstants
  from tools.replay_lx3_longitudinal_problem import compare
  c=NS(get_T_FOLLOW=lambda *a:.7,dynamic_t_follow=lambda tf,*a:tf,flex_gap_nominal_tf=.7,
       comfort_brake=2.4,stop_distance=5.5,j_lead_factor=0.,mode='acc',v_cruise=18.,stop_dist=1000.,
       trafficStopDistanceAdjust=0.,autoNaviSpeedDecelRate=1.)
  lead=NS(status=True,radar=True,radarTrackId=1,dRel=20.,vLead=1.,vRel=1.,aLeadK=0.,aLeadTau=1.5,jLead=0.,modelProb=.99)
  radar=NS(leadOne=lead,leadTwo=NS(**{**vars(lead),'status':False}))
  plain,captured=LongitudinalMpc(),LongitudinalMpc()
  approach=ApproachComfort();approach.traffic_weight=1.
  pair=traffic_ceiling_comparison(approach,ApproachSettings(True,40,40,40,30),1.3,0.)
  packets=[]
  for cycle in range(20):
    for m,enabled in ((plain,False),(captured,True)):
      m.evidence_requested=enabled
      m.set_cur_state(0.,0.);m.set_weights();m.set_accel_limits(-2.,pair['30']['applied_ceiling'])
      m.update(c,False,radar,18.,*[np.zeros(N+1) for _ in range(4)])
      assert m.solution_status==0
    np.testing.assert_array_equal(plain.params,captured.params)
    np.testing.assert_array_equal(plain.yref,captured.yref)
    np.testing.assert_array_equal(plain.x_sol,captured.x_sol)
    np.testing.assert_array_equal(plain.u_sol,captured.u_sol)
    ts=ModelConstants.T_IDXS[:CONTROL_N]
    target,stop,_,_=get_accel_from_plan(np.interp(ts,T_IDXS,captured.v_solution),np.interp(ts,T_IDXS,captured.a_solution),ts,action_t=.25,vEgoStopping=.5)
    packets.append({'problem':captured.evidence_problem,'comparison':pair,'dt':.05,
                    'output':{'a_target':float(target),'should_stop':bool(stop),'actuator_delay':.2,'v_ego_stopping':.5}})
  initial=compare(packets[0])
  assert initial['status']=='qualified_single_problem', initial
  assert initial['max_horizon_accel_difference']>0.
  nochange={**packets[0],'comparison':{'30':pair['30'],'40':pair['30']}}
  equal=compare(nochange)
  assert equal['status']=='qualified_single_problem' and equal['max_horizon_accel_difference']==0.,equal
  mismatch=json.loads(json.dumps(packets[0]));mismatch['problem']['output_x'][1][2]+=1.
  assert compare(mismatch)['status']=='baseline_mismatch'
  logger=LongitudinalEvidence(True);sizes=[]
  for _ in range(100):
    logger.emit(lambda:packets[0],lambda row:sizes.append(len(json.dumps(row))))
  assert sizes and max(sizes)<32768
  result={'actual_native_cycles_identical':20,'initial_single_problem_pair':initial,'equal_cap_pair':equal,
          'baseline_mismatch_rejected':True,'maximum_frame_bytes':max(sizes),
          'logger_max_emit_us_including_sink':logger.max_emit_us,'logger_budget_rejections':logger.errors,
          'scope':'CI synthetic single-problem validation, no vehicle or subjective quality qualification'}
  args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))


if __name__=='__main__':main()
