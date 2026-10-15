"""Isolated CI: real solver noninterference and PC replay qualification."""
import argparse
import json
import hashlib
import os
import tempfile
import time
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
    packets.append({'problem':captured.evidence_problem,'comparison':pair,'dt':.05,'actual_arm':30,
                    'output':{'a_target':float(target),'should_stop':bool(stop),'actuator_delay':.2,'v_ego_stopping':.5}})
  initial=compare(packets[0])
  assert initial['status']=='baseline_reproduced_single_problem', initial
  assert initial['max_horizon_accel_difference']>0.
  nochange={**packets[0],'comparison':{'30':pair['30'],'40':pair['30']}}
  equal=compare(nochange)
  assert equal['status']=='baseline_reproduced_single_problem' and equal['max_horizon_accel_difference']==0.,equal
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
  # Run the REAL planner builder with the REAL CarrotPlanner fields. Navigation
  # update is held fixed to isolate this passive logger, not to qualify driving.
  with tempfile.TemporaryDirectory(prefix='lx3-evidence-params-') as directory:
    os.environ['PARAMS_ROOT']=directory
    from cereal import car, log
    from openpilot.common.params import Params
    from openpilot.selfdrive.carrot.carrot_functions import CarrotPlanner
    from openpilot.selfdrive.controls.lib import longitudinal_planner as lp
    params=Params();params.put_int('MyDrivingMode',3);params.put_int('ApproachTrafficCalm',30)
    params.put_bool('ApproachComfortEnabled',True)
    cp=car.CarParams.new_message();cp.carFingerprint='HYUNDAI_PALISADE_LX3_HEV';cp.openpilotLongitudinalControl=True
    class SM(dict):
      def all_checks(self,service_list):return True
    sm=SM(carState=car.CarState.new_message(),carControl=car.CarControl.new_message(),
          controlsState=log.ControlsState.new_message(),radarState=log.RadarState.new_message(),
          modelV2=log.ModelDataV2.new_message(),selfdriveState=log.SelfdriveState.new_message())
    sm.valid={k:True for k in sm};sm.alive={k:True for k in sm};sm.logMonoTime={k:time.monotonic_ns() for k in sm}
    sm['carState'].vCruise=20.;sm['carState'].vCluRatio=1.;sm['carState'].standstill=True;sm['carState'].canValid=True
    sm['controlsState'].longControlState='pid';sm['carControl'].longActive=True
    carrot=CarrotPlanner();carrot.update=lambda *args:20.;carrot.mode='acc';carrot.v_cruise=20/3.6;carrot.stop_dist=1000.
    planner=lp.LongitudinalPlanner(cp);rows=[];sent=[]
    old_sink=lp.cloudlog.debug;lp.cloudlog.debug=rows.append
    try:
      planner.update(sm,carrot)
      assert planner.long_evidence_pending is not None,planner.long_evidence.last_error
      # Below0.1 the turn helper aliases its input list. Force the real
      # approach to lower the limit and require the recorded table to stay1.3.
      from openpilot.selfdrive.controls.lib.approach_comfort import ApproachRequest
      original_update=planner.approach.update
      planner.approach.update=lambda *a,**kw:ApproachRequest(0.8)
      original_accel=carrot.get_carrot_accel
      carrot.get_carrot_accel=lambda _:1.3
      try:
        planner.update(sm,carrot)
        bounds=planner.long_evidence_pending['bounds']
        assert bounds['table_max']==1.3 and bounds['after_approach']<1.3,bounds
      finally:
        planner.approach.update=original_update
        carrot.get_carrot_accel=original_accel
      result['standstill_table_max_before_alias_mutation']=True
      original=planner.long_evidence_pending['state']['v_ego']
      sm['carState'].vEgo=99.  # A lazy closure would silently change the evidence.
      planner.publish(sm,NS(send=lambda *args:sent.append(args)),carrot)
      assert sent and rows[-1][LongitudinalEvidence.KEY]['state']['v_ego']==original
      sm['carState'].vEgo=0.
      old_compare=lp.traffic_ceiling_comparison
      lp.traffic_ceiling_comparison=lambda *a: (_ for _ in ()).throw(ValueError('injected diagnostic failure'))
      try:planner.update(sm,carrot)
      finally:lp.traffic_ceiling_comparison=old_compare
      assert planner.long_evidence.errors>0 and np.isfinite(planner.output_a_target)
    finally:lp.cloudlog.debug=old_sink
    result['real_planner_real_carrot_builder']=True
    result['snapshot_not_lazy']=True
    result['diagnostic_failure_did_not_abort_planner']=True
    # Exact OTA10 planner fixture with the current unchanged native solver.
    # New preferences OFF must preserve the real problem and solution bytes.
    fixture=Path(__file__).parent/'fixtures/lx3_longitudinal_ota10_before.txt'
    raw=fixture.read_bytes();scope={'__name__':'lx3_ota10_long_baseline'}
    exec(compile(raw,str(fixture),'exec'),scope)
    baseline=scope['LongitudinalPlanner'](cp);off=lp.LongitudinalPlanner(cp)
    from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences
    off.driving_preferences=DrivingPreferences(Path(directory)/'off.json')
    carrots=[]
    for _ in range(2):
      item=CarrotPlanner();item.update=lambda *args:30.;item.mode='acc';item.v_cruise=30/3.6;item.stop_dist=1000.
      carrots.append(item)
    for i in range(120):
      sm.logMonoTime={k:time.monotonic_ns() for k in sm}
      sm['carState'].vEgo=0. if i<30 else min(8.,(i-30)*.1)
      sm['carState'].standstill=i<30
      for instance,item in zip((baseline,off),carrots):instance.update(sm,item)
      for attr in ('params','yref','x_sol','u_sol'):
        assert getattr(baseline.mpc,attr).tobytes()==getattr(off.mpc,attr).tobytes(),(i,attr)
      assert baseline.output_a_target==off.output_a_target and baseline.output_should_stop==off.output_should_stop
    result['longitudinal_off_baseline_cycles_byte_identical']=120
    result['longitudinal_baseline_source_sha256']=hashlib.sha256(raw).hexdigest()
    # Integration precedence: force an optional capability and inspect the
    # actual planner's turn limiter, feasibility clamp and approach override.
    original_launch=off.launch_follow.update
    off.launch_follow.update=lambda *args,**kw:kw['table_max']*1.2
    carrots[1].get_carrot_accel=lambda _:1.3
    sm['carState'].vEgo=12.;sm['carState'].standstill=False
    sm['controlsState'].desiredCurvature=.03
    off.approach.update=lambda *a,**kw:ApproachRequest()
    off.long_evidence.due=lambda *args:True
    sm.logMonoTime={k:time.monotonic_ns() for k in sm};off.update(sm,carrots[1])
    b=off.long_evidence_pending['bounds']
    expected=lp.limit_accel_in_turns(12.,.03,[-2.,1.56],3.)[1]
    assert abs(b['before_approach']-max(expected,b['a_desired_before']-.05))<1e-9,b
    off.approach.update=lambda *a,**kw:ApproachRequest(.4)
    sm.logMonoTime={k:time.monotonic_ns() for k in sm};off.update(sm,carrots[1])
    b=off.long_evidence_pending['bounds']
    assert b['after_approach']<=b['before_approach'] and b['after_approach']==b['mpc_max'],b
    off.launch_follow.update=original_launch
    result['actual_launch_turn_feasibility_approach_precedence']=True
  args.output.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result))


if __name__=='__main__':main()
