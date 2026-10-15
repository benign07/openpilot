"""Actual linked lateral MPC compatibility; isolated synthetic CI only."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS

import numpy as np


def main():
  assert os.environ.get('GITHUB_ACTIONS') == 'true', 'Isolated CI only, not a device tool'
  parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
  from cereal import car,log,custom
  from openpilot.common.params import Params
  from openpilot.selfdrive.controls.lib import lateral_planner as lp
  from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences
  from openpilot.selfdrive.modeld.constants import ModelConstants
  fixture=Path(__file__).parent/'fixtures/lx3_lateral_ota10_before.txt'
  raw=fixture.read_bytes()
  baseline_scope={'__name__':'lx3_ota10_lateral_baseline'}
  exec(compile(raw,str(fixture),'exec'),baseline_scope)
  baseline_class=baseline_scope['LateralPlanner']
  with tempfile.TemporaryDirectory(prefix='lx3-native-prefs-') as directory:
    os.environ['PARAMS_ROOT']=directory
    params=Params()
    for name,value in {'UseLaneLineSpeed':5,'UseLaneLineCurveSpeed':0,'PathOffset':0,'AdjustLaneOffset':0}.items():
      params.put_int(name,value)
    cp=car.CarParams.new_message();cp.carFingerprint='HYUNDAI_PALISADE_LX3_HEV'
    cp.wheelbase=2.97;cp.centerToFront=1.2;cp.mass=2400.;cp.tireStiffnessRear=160000.
    class SM(dict):
      def all_checks(self,service_list):return True
    sm=SM(carState=car.CarState.new_message(),carControl=car.CarControl.new_message(),
      controlsState=log.ControlsState.new_message(),radarState=log.RadarState.new_message(),
      modelV2=log.ModelDataV2.new_message(),carrotMan=custom.CarrotMan.new_message())
    sm.valid={k:True for k in sm};sm.alive={k:True for k in sm};sm.logMonoTime={k:0 for k in sm}
    cs=sm['carState'];cs.vEgo=12.;cs.useLaneLineSpeed=5;cs.canValid=True
    cs.leftLaneLine=21;cs.rightLaneLine=10;sm['carControl'].latActive=True
    sm['carrotMan'].vTurnSpeed=200
    md=sm['modelV2'];t=ModelConstants.T_IDXS.tolist();x=(ModelConstants.T_IDXS*12).tolist()
    for name in ('position','orientation','orientationRate','velocity','acceleration'):
      value=getattr(md,name);value.t=t;value.x=x if name=='position' else [12. if name=='velocity' else 0.]*33
      value.y=[-0.0]*33;value.z=[0.]*33
    lines=md.init('laneLines',4)
    for line,y in zip(lines,(-5.1,-1.7,1.7,5.1)):
      line.t=t;line.x=x;line.y=[y]*33;line.z=[0.]*33
    md.laneLineProbs=[.1,.95,.95,.1];md.laneLineStds=[.05]*4
    edges=md.init('roadEdges',2)
    for edge,y in zip(edges,(-5.1,5.1)):
      edge.t=t;edge.x=x;edge.y=[y]*33;edge.z=[0.]*33
    md.roadEdgeStds=[.05,.05];md.meta.laneWidthLeft=3.4;md.meta.laneWidthRight=3.4
    carrot=NS(atc_active=False)
    baseline=baseline_class(cp);off=lp.LateralPlanner(cp);shadow=lp.LateralPlanner(cp);active=lp.LateralPlanner(cp)
    off.margin_preferences=DrivingPreferences(Path(directory)/'off.json')
    shadow.margin_preferences=DrivingPreferences(Path(directory)/'shadow.json')
    shadow.margin_preferences.set('YellowLineBiasCm',5)
    active.margin_preferences=DrivingPreferences(Path(directory)/'active.json')
    active.margin_preferences.set('YellowLineBiasCm',5);active.margin_preferences.set('LateralMarginShadow',0)
    now=[1_000_000_000];old_clock=lp.time.monotonic_ns;old_log=lp.cloudlog.debug
    lp.time.monotonic_ns=lambda:now[0];records=[];lp.cloudlog.debug=records.append
    active_values=[]
    try:
      for i in range(160):
        now[0]=1_000_000_000+i*50_000_000
        sm.logMonoTime={k:now[0] for k in sm}
        for planner in (baseline,off,shadow,active):planner.update(sm,carrot)
        assert off.path_xyz.tobytes()==baseline.path_xyz.tobytes()
        assert shadow.path_xyz.tobytes()==baseline.path_xyz.tobytes()
        assert off.lat_mpc.x_sol.tobytes()==baseline.lat_mpc.x_sol.tobytes()
        assert shadow.lat_mpc.x_sol.tobytes()==baseline.lat_mpc.x_sol.tobytes()
        assert active.lat_mpc.solution_status==0 and np.isfinite(active.lat_mpc.x_sol).all()
        active_values.append(active.margin_applied)
      assert baseline.lanelines_active and active_values[-1]<-.049
      assert max(abs(v) for v in active_values)<=.05+1e-9
      # Existing laneless mode wins; this feature does not silently switch it.
      cs.useLaneLineSpeed=0
      now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm};active.readParams=0
      active.update(sm,carrot)
      assert not active.lanelines_active and active.margin_applied==0
      # A different supported vehicle never reads LX3 preference data.
      cp.carFingerprint='KIA_EV9';other=lp.LateralPlanner(cp)
      other.margin_preferences.read=lambda: (_ for _ in ()).throw(AssertionError('LX3 preferences leaked'))
      other.update(sm,carrot)
      assert other.margin_applied==0 and not other.margin_supported
    finally:
      lp.time.monotonic_ns=old_clock;lp.cloudlog.debug=old_log
    result={'baseline_commit':'2bd232be118008f497037ad0272ab35a5632647b',
      'baseline_source_sha256':hashlib.sha256(raw).hexdigest(),
      'actual_native_lateral_cycles':160,'off_path_and_solver_byte_identical':True,
      'shadow_path_and_solver_byte_identical':True,'active_shift_bounded':True,
      'laneless_exit_reset':True,'non_lx3_isolated':True,'diagnostic_records':len(records),
      'scope':'synthetic actual solver checks; no closed-loop vehicle or safety qualification'}
  args.output.write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))


if __name__=='__main__':main()
