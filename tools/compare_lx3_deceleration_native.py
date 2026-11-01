"""Exact source22/candidate Python references on the unchanged linked MPC.

Synthetic ideal acceleration plant, not a vehicle/actuator/energy simulation.
No CAN, Params writes, device connection, firmware or model generation.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import subprocess
import sys
from types import ModuleType, SimpleNamespace as NS

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
BEFORE = 'dd81cf729baf6316cc8517b377dc39f6aaa137b6'
FILES = {'flex': 'selfdrive/controls/lib/flexible_gap.py',
         'mpc': 'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py'}
HASHES = {'flex': 'dc799bfb40dea4f3e10847a57a072aedab1a6a74cb98c490b664926b7d77ff73',
          'mpc': 'b6ee87678c167c2956686496de1db369c4ad19300bf764e335de0d1027d9513c'}


def load(name, source):
  m = ModuleType(name); m.__file__ = str(ROOT / FILES['mpc']); sys.modules[name] = m
  exec(compile(source, m.__file__, 'exec'), m.__dict__)
  return m


def run(module, scenario, ratio):
  high = scenario in ('high_mild', 'high_rebraking', 'urgent', 'cutin', 'far', 'secondary', 'disabled')
  v = 100/3.6 if high else 4.
  vl = v-.2; gap = 35. if high else 10.
  if scenario == 'far': gap = 150.
  a = -.35 if high else -.2
  c = NS(get_T_FOLLOW=lambda *args: .9, dynamic_t_follow=lambda tf,*args: tf,
         flex_gap_nominal_tf=.9, comfort_brake=2.5, stop_distance=5.5, j_lead_factor=0.,
         mode='acc', v_cruise=v, stop_dist=1000., trafficStopDistanceAdjust=0., autoNaviSpeedDecelRate=1.)
  m = module.LongitudinalMpc()
  m.evidence_requested = True
  codes = []; solve = m.solver.solve
  # Wrapping the Python object observes failures before production reset.
  class Probe:
    def __getattr__(self, name): return getattr(m._solver, name)
    def solve(self):
      code = solve(); codes.append(int(code)); return code
  m._solver = m.solver; m.solver = Probe()
  trace = []
  for frame in range(1400):
    t = frame*.05
    al = (-.35 if high else -.2) if t < (12. if high else 18.) else 0.
    if scenario == 'high_rebraking' and 22. <= t < 26.: al = -.3
    if scenario == 'urgent' and 5. <= t < 9.: al = -3.
    if scenario == 'cutin' and frame == 100: gap,vl = 25.,max(0.,v-3.)
    if vl <= 0. and al < 0.: al = 0.
    lead = NS(status=True, radar=True, radarTrackId=2 if scenario=='cutin' and frame>=100 else 1,
              dRel=gap, vLead=vl, vRel=vl-v, aLeadK=al, aLeadTau=1.5, jLead=0., modelProb=.99)
    absent = NS(status=False, radar=False, radarTrackId=-1, dRel=1000., vLead=0.,
                vRel=0., aLeadK=0., jLead=0., modelProb=0.)
    second = NS(**vars(lead)) if scenario == 'secondary' else absent
    if second.status: second.radarTrackId=3;second.dRel=gap-3.
    radar = NS(leadOne=lead, leadTwo=second)
    m.set_cur_state(v,a); m.set_accel_limits(-3.5,1.2); m.set_weights()
    m.update(c,False,radar,c.v_cruise,*[np.zeros(13) for _ in range(4)],
             flex_gap_settings=module.FlexibleGapSettings(scenario!='disabled',ratio),
             flex_gap_eligible=True, observed_a_ego=a, flex_gap_gentle_stop=True,
             speed_gap_eligible=True)
    desired = float(np.interp(.2,module.T_IDXS,m.a_solution))
    # Ideal bounded acceleration actuation; no measured hydraulic/regen model.
    next_a = float(np.clip(desired,-3.5,1.2))
    if v <= 0. and next_a < 0.: next_a=0.
    decel = getattr(m,'decel_gap',None)
    trace.append(dict(t=t,v=v,a=a,target=next_a,gap=gap,lead_v=vl,lead_a=al,
                      jerk=(next_a-a)/.05,crash_cnt=m.crash_cnt,
                      phase=decel.phase if decel else 'legacy',
                      reason=decel.reason if decel else 'legacy',
                      stable_s=decel.stable_s if decel else 0.,
                      planned_a=float(m.x0[2]),
                      reduction=decel.reduction if decel else 0.,status=codes[-1]))
    next_v=max(0.,v+next_a*.05);next_vl=max(0.,vl+al*.05)
    gap += .5*(vl+next_vl-v-next_v)*.05
    v,vl,a=next_v,next_vl,next_a
  assert all(code==0 for code in codes), (scenario, 'solver_failure', codes)
  assert min(r['gap'] for r in trace) > .25, (scenario, 'collision_in_ideal_plant')
  return dict(min_gap=min(r['gap'] for r in trace), peak_decel=-min(r['a'] for r in trace),
              peak_abs_jerk=max(abs(r['jerk']) for r in trace),
              crash_count_max=max(r['crash_cnt'] for r in trace),
              relaxed_frames=sum(r['reduction']>0 for r in trace),
              solver_calls=len(codes), solver_nonzero=sum(c!=0 for c in codes),trace=trace)


def main():
  p=argparse.ArgumentParser(description=__doc__)
  p.add_argument('--baseline-json',type=Path)
  p.add_argument('--output',type=Path,required=True)
  args=p.parse_args()
  old={k:(json.loads(args.baseline_json.read_text(encoding='utf-8'))[path].encode() if args.baseline_json
          else subprocess.check_output(['git','show',BEFORE+':'+path],cwd=ROOT)) for k,path in FILES.items()}
  assert {k:hashlib.sha256(b).hexdigest() for k,b in old.items()} == HASHES
  def gen(b):return [ast.dump(n,include_attributes=False) for n in ast.parse(b).body
                     if isinstance(n,ast.FunctionDef) and n.name in ('gen_long_model','gen_long_ocp')]
  assert gen(old['mpc'])==gen((ROOT/FILES['mpc']).read_bytes())
  from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib import long_mpc as candidate
  legacy_flex=load('lx3_source22_flex',old['flex']);legacy=load('lx3_source22_mpc',old['mpc'])
  legacy.FlexibleGap=legacy_flex.FlexibleGap;legacy.FlexibleGapSettings=legacy_flex.FlexibleGapSettings
  results={}
  for case in ('high_mild','low_mild','high_rebraking','urgent','cutin','far','secondary','disabled'):
    results[case]={label:run(mod,case,80) for label,mod in (('source22',legacy),('candidate',candidate))}
  # Opted-out code must yield exactly the same numerical trace (extra fields excluded).
  for case in ('disabled','far'):
    a,b=results[case]['source22']['trace'],results[case]['candidate']['trace']
    for name in ('v','a','target','gap','jerk'):
      np.testing.assert_array_equal([r[name] for r in a],[r[name] for r in b])
  report={'baseline':BEFORE,'source_hashes':{k:hashlib.sha256((ROOT/path).read_bytes().replace(b'\r\n',b'\n')).hexdigest() for k,path in FILES.items()},
          'scope':'Production MPC on linked unchanged solver; synthetic ideal bounded acceleration plant, not road/actuator/energy qualification.',
          'results':results}
  args.output.parent.mkdir(parents=True,exist_ok=True)
  args.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
  assert results['high_mild']['candidate']['relaxed_frames']>0
  assert results['low_mild']['candidate']['relaxed_frames']>0
  for case in ('high_mild','low_mild','high_rebraking'):
    assert results[case]['candidate']['peak_decel'] <= results[case]['source22']['peak_decel'] + 1e-6
    assert results[case]['candidate']['crash_count_max'] <= results[case]['source22']['crash_count_max']
  assert all(r['reduction']==0 for r in results['urgent']['candidate']['trace'] if r['lead_a'] <= -1.2)
  assert results['cutin']['candidate']['trace'][100]['reduction']==0
  report['checks_passed']=True
  args.output.write_text(json.dumps(report,indent=2),encoding='utf-8')
  print(json.dumps({case:{label:{k:v for k,v in row.items() if k!='trace'} for label,row in rows.items()} for case,rows in results.items()},indent=2))


if __name__=='__main__': main()
