"""CI-only old/new Python planning on the same actual Acados model and ideal plant.

Historical sources are hash-bound. No CAN, device, real Params or actuator I/O.
Outputs are comparative model results, not road comfort/safety qualification.
"""
import ast
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import ModuleType, SimpleNamespace as NS

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BEFORE = '32ff4a1101fc522ce43d44e0989cd52862abc203'


def module(name, data, path):
  m = ModuleType(name)
  m.__file__ = str(path)
  sys.modules[name] = m
  exec(compile(data, str(path), 'exec'), m.__dict__)
  return m


class SolverProbe:
  """Observe actual return codes before the production failure reset clears them."""
  def __init__(self, solver):
    self.solver, self.codes = solver, []
  def __getattr__(self, name):
    return getattr(self.solver, name)
  def solve(self):
    code = self.solver.solve()
    self.codes.append(int(code))
    return code


def main():
  assert os.environ.get('GITHUB_ACTIONS') == 'true'
  out = Path(sys.argv[1]);out.mkdir(exist_ok=True)
  before_helper = (ROOT/'tools/fixtures/lx3_approach_20261012_before.txt').read_bytes()
  assert hashlib.sha256(before_helper).hexdigest() == 'b70aa802f06db801c507a18cf2b63c29eb47367c64df03e0c8574f38e947c103'
  paths = {'mpc':'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py',
           'flex':'selfdrive/controls/lib/flexible_gap.py'}
  old = {k:subprocess.check_output(['git','show',BEFORE+':'+p],cwd=ROOT) for k,p in paths.items()}
  assert hashlib.sha256(old['mpc']).hexdigest() == '42d8f703ba206eee85a097c7612335b684b1f6b9d43936ebcbea8589dc99dff7'
  assert hashlib.sha256(old['flex']).hexdigest() == 'f23bc4295f2ded40099115050ec3eabbefdd88842facf0800b34acd6387a7443'
  # Python cost/reference wiring differs. Numerical physical-model and OCP
  # generation functions are identical, so sharing the linked solver is valid.
  def generators(data):
    return [ast.dump(n,include_attributes=False) for n in ast.parse(data).body
            if isinstance(n,ast.FunctionDef) and n.name in ('gen_long_model','gen_long_ocp')]
  assert generators(old['mpc']) == generators((ROOT/paths['mpc']).read_bytes())
  with tempfile.TemporaryDirectory(prefix='lx3-native-comparison-') as tmp:
    os.environ['PARAMS_ROOT'] = tmp
    from openpilot.common.params import Params
    Params().put_int('MyDrivingMode',3)
    from cereal import log
    from openpilot.selfdrive.carrot.carrot_functions import CarrotPlanner, DrivingMode
    from openpilot.selfdrive.controls.lib import approach_comfort as new_helper
    from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib import long_mpc as new_mpc
    old_helper = module('lx3_exact_old_approach',before_helper,ROOT/'tools/fixtures/lx3_approach_20261012_before.txt')
    old_flex = module('lx3_exact_old_flex',old['flex'],ROOT/paths['flex'])
    old_mpc = module('lx3_exact_old_mpc',old['mpc'],ROOT/paths['mpc'])
    old_mpc.FlexibleGap, old_mpc.FlexibleGapSettings = old_flex.FlexibleGap, old_flex.FlexibleGapSettings
    variants = [('installed_40',old_helper,old_mpc,False,(40,40,40,40)),
                ('candidate_same_40',new_helper,new_mpc,False,(40,40,40,40)),
                ('candidate_60_80',new_helper,new_mpc,False,(60,80,60,60)),
                ('candidate_flex80',new_helper,new_mpc,True,(60,80,60,60))]
    cases = ['mild_braking','stop_go','strong_braking','cut_in','free_cruise','mild_low_speed']
    results = []
    for scenario in cases:
      for mode in (1,2,3,4):
        for label, helper, mpc_module, flexible, strengths in variants:
          c = CarrotPlanner()
          c.myDrivingMode = DrivingMode(mode)
          c.mySafeFactor = {1:.9,2:.8,3:1.,4:1.}[mode]
          c.tFollowGap1,c.tFollowGap2,c.tFollowGap3,c.tFollowGap4 = .7,.8,1.,1.15
          c.t_follow_last = .7
          c.tFollowDecelBoost, c.dynamicTFollow = .35, 0.
          c.mode,c.v_cruise,c.stop_dist = 'acc',18.,1000.
          c.j_lead_factor,c.stop_distance,c.comfort_brake = 0.,5.5,2.4*c.mySafeFactor
          v = 0. if scenario=='stop_go' else (3. if scenario=='mild_low_speed' else 50/3.6)
          vl = 2.8 if scenario=='mild_low_speed' else v
          distance = {'stop_go':6.,'mild_low_speed':8.}.get(scenario,40.)
          a = 0.
          planner, m = helper.ApproachComfort(), mpc_module.LongitudinalMpc()
          probe = SolverProbe(m.solver);m.solver = probe
          settings = helper.ApproachSettings(True,*strengths)
          flex_type = old_flex.FlexibleGapSettings if mpc_module is old_mpc else new_mpc.FlexibleGapSettings
          trace=[];first_decel=None;maximum_jerk=0.;peak=None;minimum_gap=distance;flex_seen=False
          for frame in range(800):
            t = frame*.05
            al = 0.
            if scenario=='mild_braking' and 3.<=t<24.:al=-.6
            elif scenario=='strong_braking' and 3.<=t<8.:al=-2.5
            elif scenario=='mild_low_speed' and t<9.4:al=-.3
            elif scenario=='stop_go':
              if 3.<=t<8.:al=.5
              elif 12.<=t<18.3:al=-.4
            if scenario=='cut_in' and frame==100:distance,vl=18.,v-3.
            if vl<=0. and al<0.:al=0.
            lead=NS(status=scenario!='free_cruise',radar=True,radarTrackId=2 if scenario=='cut_in' and t>=5. else 1,
                    dRel=distance,vLead=vl,vRel=vl-v,aLeadK=al,aLeadTau=1.5,jLead=0.,modelProb=.99)
            radar=NS(leadOne=lead,leadTwo=NS(**{**vars(lead),'status':False}))
            request=planner.update(settings,(radar.leadOne,radar.leadTwo),eligible=True,
              v_ego=v,a_ego=a,base_tf=c.t_follow_last,stop_distance=c.stop_distance,
              maximum_accel=c.get_carrot_accel(v),comfort_brake=c.comfort_brake,cruise_speed=c.v_cruise,planned_accel=a)
            m.set_cur_state(v,a)
            m.set_weights(jerk_factor=c.jerk_factor_apply*request.jerk_factor)
            m.set_accel_limits(-1.2,helper.apply_approach_ceiling(c.get_carrot_accel(v),a,request))
            kwargs=dict(personality=log.LongitudinalPersonality.aggressive,approach_extra_tf=request.extra_tf,
                        flex_gap_settings=flex_type(flexible,80),flex_gap_eligible=flexible,observed_a_ego=a)
            if mpc_module is new_mpc:kwargs['flex_gap_gentle_stop']=strengths[2]>0
            m.update(c,False,radar,c.v_cruise,*[np.zeros(mpc_module.N+1) for _ in range(4)],**kwargs)
            assert np.isfinite(m.x_sol).all() and np.isfinite(m.u_sol).all()
            next_a=float(np.interp(.05,mpc_module.T_IDXS,m.a_solution))
            jerk=(next_a-a)/.05
            if abs(jerk)>maximum_jerk:
              maximum_jerk=abs(jerk)
              peak={'t':t,'v':v,'a':a,'next_a':next_a,'gap':distance,'v_rel':vl-v,'lead_a':al,
                    'extra_tf':request.extra_tf,'stock_tf':m.t_follow,'yref':float(m.yref[0,0]),
                    'jerk_factor':request.jerk_factor,'accel_ceiling':request.accel_ceiling}
            if next_a<-.1 and first_decel is None:first_decel=t
            flex_seen |= m.flex_gap.reduction>0.
            if frame%4==0:
              trace.append({'t':t,'v':v,'a':next_a,'jerk':jerk,'gap':distance,'lead_v':vl,
                            'extra_tf':request.extra_tf,'stock_tf':m.t_follow,'yref':float(m.yref[0,0]),
                            'flex_fraction':m.flex_gap.reduction,'jerk_factor':request.jerk_factor})
            next_v,next_vl=max(0.,v+next_a*.05),max(0.,vl+al*.05)
            distance+=((vl+next_vl)-(v+next_v))*.025
            minimum_gap=min(minimum_gap,distance)
            v,vl,a=next_v,next_vl,next_a
          result={'scenario':scenario,'mode':mode,'variant':label,'minimum_gap':minimum_gap,
                  'first_decel_s':first_decel,'max_jerk':maximum_jerk,'peak':peak,'flex_seen':bool(flex_seen),
                  'solver_return_codes':dict(Counter(probe.codes))}
          results.append(result)
          (out/f'{scenario}-{mode}-{label}.json').write_text(json.dumps(trace),encoding='utf-8')
    summary={'scope':'exact_installed_old_sources_and_same_settings_vs_candidate_on_ideal_kinematic_plant_not_vehicle',
             'before_commit':BEFORE,'before_helper_sha256':hashlib.sha256(before_helper).hexdigest(),
             'assumptions':{'radar':'ideal','dt':.05,'stop_distance':5.5,'gap1':.7,'decel_boost':.35,
                            'dynamic_tf':0,'stock_carrot_update':'only TF methods and mode coefficients, not full navigation/planner'},
             'results':results}
    (out/'comparison.json').write_text(json.dumps(summary,indent=2),encoding='utf-8')
    print(json.dumps({'scenarios':len(results),'nonzero_solver_codes':sum(sum(v for k,v in r['solver_return_codes'].items() if k!=0) for r in results),
                      'minimum_gap':min(r['minimum_gap'] for r in results),'all_traces_saved':True}))
    assert all(set(r['solver_return_codes'])=={0} and r['minimum_gap']>0 for r in results)


if __name__=='__main__':
  main()
