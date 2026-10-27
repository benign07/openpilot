"""Actual linked lateral MPC compatibility; isolated synthetic CI only."""
import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS

import numpy as np


def main():
  parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True)
  parser.add_argument('--isolated-local',action='store_true',help='Run in a disposable x86 PC source snapshot, never on a vehicle device')
  args=parser.parse_args()
  assert os.environ.get('GITHUB_ACTIONS') == 'true' or (
    args.isolated_local and platform.machine().lower() in ('x86_64','amd64') and not Path('/TICI').exists()
  ), 'Isolated CI or explicit disposable x86 PC snapshot only, not a device tool'
  from cereal import car,log,custom
  from openpilot.common.params import Params
  from openpilot.selfdrive.controls.lib import lateral_planner as lp
  from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences,ResponsePreferences
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
    md=sm['modelV2'];t=list(ModelConstants.T_IDXS);x=(np.asarray(t)*12).tolist()
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
      # The owner explicitly opted into stock lane following from 30 km/h
      # when actual margins are on and native selection is disabled.
      cs.useLaneLineSpeed=0
      now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm};active.readParams=0
      active.update(sm,carrot)
      assert not active.lanelines_active and active.effective_lane_speed==0
      assert active.margin_applied==0 and active.margin.reason=='native_lane_mode_disabled'
      assert params.get_int('UseLaneLineSpeed')==5
      params.put_int('UseLaneLineSpeed',0)
      active.readParams=0;active.update(sm,carrot)
      assert active.lanelines_active and active.effective_lane_speed==30
      assert params.get_int('UseLaneLineSpeed')==0
      active.margin_preferences.set('YellowLineBiasCm',0);active.readParams=0
      active.update(sm,carrot)
      assert not active.lanelines_active and active.margin_applied==0 and active.effective_lane_speed==0
      # OFF/shadow retain the native-zero model path; actual is a deliberate
      # lane-mode opt-in, still bounded by all existing margin conditions.
      active.margin_preferences.set('YellowLineBiasCm',5)
      for planner in (baseline,off,shadow,active):planner.readParams=0
      for i in range(160):
        now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm}
        for planner in (baseline,off,shadow,active):planner.update(sm,carrot)
        for planner in (off,shadow):
          assert not planner.lanelines_active and planner.effective_lane_speed==0
          assert planner.margin_applied==0
          assert planner.path_xyz.tobytes()==baseline.path_xyz.tobytes()
        assert off.lat_mpc.x_sol.tobytes()==baseline.lat_mpc.x_sol.tobytes()
        assert shadow.lat_mpc.x_sol.tobytes()==baseline.lat_mpc.x_sol.tobytes()
        assert active.lanelines_active and active.effective_lane_speed==30
        assert active.lat_mpc.solution_status==0 and np.isfinite(active.lat_mpc.x_sol).all()
        assert abs(active.margin_applied)<=.05+1e-9
      assert active.margin_applied<-.049
      assert shadow.margin.value<-.049
      # Derive the sign from the exact production controlsd convention.
      import ast, math
      control=ast.parse((Path(__file__).parents[1]/'selfdrive/controls/controlsd.py').read_text())
      expr=next(n.value for n in ast.walk(control) if isinstance(n,ast.Assign) and
                any(isinstance(t,ast.Attribute) and t.attr=='curvature' for t in n.targets) and
                isinstance(n.value,ast.UnaryOp) and isinstance(n.value.op,ast.USub))
      # Find production -VM.calc_curvature with a left steering angle.
      assert 'calc_curvature' in ast.unparse(expr)
      scope={'self':NS(VM=NS(calc_curvature=lambda *args:.0003)),
             'CS':NS(vEgo=12.),'steer_angle_without_offset':.01,'lp':NS(roll=0.),'math':math}
      signed=eval(compile(ast.Expression(expr),'controlsd_curvature','eval'),scope)
      assert signed<0
      for i in range(100):
        now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm}
        sm['controlsState'].curvature=signed;shadow.update(sm,carrot)
      assert abs(shadow.margin.value)<1e-9
      # Real native-path hold after a synthetic 6cm lane-relative movement.
      # No actuator/ECU model: solver feasibility and timing only.
      sm['controlsState'].curvature=0.
      params.put_int('UseLaneLineSpeed',5);cs.useLaneLineSpeed=5
      hold=lp.LateralPlanner(cp)
      hold.margin_preferences=DrivingPreferences(Path(directory)/'hold-margin.json')
      hold.response_preferences=ResponsePreferences(Path(directory)/'hold-response.json')
      hold.response_preferences.set('DriverPositionHold',1)
      for i in range(40):
        now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm};hold.update(sm,carrot)
      for i in range(15):
        for line,y in zip(md.laneLines,(-5.1,-1.7,1.7,5.1)):line.y=[y-.06*min(1.,i/4)]*33
        cs.steeringPressed=True
        now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm};hold.update(sm,carrot)
      cs.steeringPressed=False;held=[]
      for i in range(161):
        now[0]+=50_000_000;sm.logMonoTime={k:now[0] for k in sm};hold.update(sm,carrot)
        assert hold.lat_mpc.solution_status==0 and np.isfinite(hold.lat_mpc.x_sol).all()
        held.append(hold.driver_position_applied)
      # Lane y is Float32 in the real schema, unlike Python-only fixtures.
      assert all(abs(value-.06)<1e-6 for value in held[:61]),held[:3]
      assert abs(held[110]-.03)<1e-6 and held[-1]==0.
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
      'owner_actual_margin_native_zero_lane_opt_in_preserved':True,'laneless_exit_reset':True,
      'laneless_off_shadow_path_byte_identical':True,
      'laneless_shadow_byte_identical':True,'production_curvature_sign_veto':True,
      'non_lx3_isolated':True,'diagnostic_records':len(records),
      'manual_position_hold_native_solver_cycles':161,'manual_position_hold_3s_return_5s':True,
      'scope':'synthetic actual solver checks; no closed-loop vehicle or safety qualification'}
  args.output.write_text(json.dumps(result,indent=2),encoding='utf-8');print(json.dumps(result))


if __name__=='__main__':main()
