"""CI-only actual Acados/Carrot calculation checks; no vehicle or CAN connection.

The closed loop uses a simple kinematic plant and ideal radar. It exercises
the linked solver, not real actuators, perception, road safety or comfort.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace as NS

import numpy as np


def main():
  assert os.environ.get('GITHUB_ACTIONS') == 'true', 'Run on isolated CI, never a vehicle'
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--output', type=Path, required=True)
  args = parser.parse_args()
  with tempfile.TemporaryDirectory(prefix='lx3-approach-ci-') as temporary:
    os.environ['PARAMS_ROOT'] = temporary
    from openpilot.common.params import Params
    Params().put_int('MyDrivingMode', 3)
    from cereal import log
    from openpilot.selfdrive.carrot.carrot_functions import CarrotPlanner, DrivingMode
    from openpilot.selfdrive.controls.lib.approach_comfort import ApproachComfort, ApproachSettings, apply_approach_ceiling
    from openpilot.selfdrive.controls.lib.flexible_gap import FlexibleGapSettings
    from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import LongitudinalMpc, N

    personality = log.LongitudinalPersonality.aggressive
    records = []

    def carrot(mode, gap):
      c = CarrotPlanner()
      c.myDrivingMode = DrivingMode(mode)
      # Same stock coefficients selected by CarrotPlanner.update; this fixture
      # does not exercise traffic lights, navigation or the full update method.
      c.mySafeFactor = {1: .9, 2: .8, 3: 1., 4: 1.}[mode]
      c.tFollowGap1, c.tFollowGap2, c.tFollowGap3, c.tFollowGap4 = .7, .8, 1., 1.15
      c.personality = personality
      c.tFollowGap1 = gap
      c.t_follow_last = c._clip_t_follow(gap*c.mySafeFactor)
      c.tFollowDecelBoost = .35
      c.mode, c.v_cruise, c.stop_dist = 'acc', 18., 1000.
      c.j_lead_factor = 0.
      c.stop_distance = 5.5
      c.comfort_brake = c.comfortBrake*c.mySafeFactor
      return c

    def radar(v, vl, distance, al, identity=1, valid=True):
      lead = NS(status=valid, radar=True, radarTrackId=identity, dRel=distance,
                vLead=vl, vRel=vl-v, aLeadK=al, aLeadTau=1.5, jLead=0., modelProb=.99)
      return NS(leadOne=lead, leadTwo=NS(**{**vars(lead), 'status':False}))

    def solve(m, c, r, v, a, request, flexible=False, speed_gap=False):
      m.set_cur_state(v, a)
      m.set_weights(jerk_factor=c.jerk_factor_apply*request.jerk_factor)
      m.set_accel_limits(-1.2, apply_approach_ceiling(c.get_carrot_accel(v), a, request))
      m.update(c, False, r, c.v_cruise, *[np.zeros(N+1) for _ in range(4)],
               personality=personality, approach_extra_tf=request.extra_tf,
               flex_gap_settings=FlexibleGapSettings(flexible, 80), flex_gap_eligible=flexible,
               observed_a_ego=a, flex_gap_gentle_stop=True, speed_gap_eligible=speed_gap)
      assert m.solution_status == 0, ('solver_status', m.solution_status)
      assert np.isfinite(m.x_sol).all() and np.isfinite(m.u_sol).all()
      assert (m.v_solution >= -1e-3).all()
      return float(np.interp(.05, __import__(
        'openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc', fromlist=['T_IDXS']).T_IDXS, m.a_solution))

    # Actual stock mode/TF/decel/dynamic composition with linked native solver.
    for mode in (1, 2, 3, 4):
      for gap in (.7, 1.15):
        c = carrot(mode, gap)
        c.dynamicTFollow = .2
        m = LongitudinalMpc()
        p = ApproachComfort()
        v, a = 50/3.6, -.3
        r = radar(v, v-2., 30., -.6)
        for _ in range(80):
          request = p.update(ApproachSettings(True, 60, 80, 60, 60), (r.leadOne, r.leadTwo),
            eligible=True, v_ego=v, a_ego=a, base_tf=c.t_follow_last,
            stop_distance=c.stop_distance, maximum_accel=c.get_carrot_accel(v), comfort_brake=c.comfort_brake,
            cruise_speed=c.v_cruise, planned_accel=a)
          solve(m, c, r, v, a, request)
        records.append({'case':'mode_gap_dynamic', 'mode':mode, 'gap':gap,
                        't_follow':m.t_follow, 'desired_distance':m.desired_distance, 'status':m.solution_status})

    # Compare feature OFF/ON under the same synthetic lead manoeuvre. Record
    # onset/jerk rather than claiming a road comfort improvement from a plant.
    for enabled in (False, True):
      c, m, p = carrot(3, .8), LongitudinalMpc(), ApproachComfort()
      v, a, vl, distance = 50/3.6, 0., 50/3.6, 40.
      minimum_gap, first_decel, max_jerk, flex_seen = distance, None, 0., False
      for frame in range(800):
        t = frame*.05
        al = -.6 if 3. <= t < 24. else 0.
        if vl <= 0.: al = 0.
        r = radar(v, vl, distance, al)
        request = p.update(ApproachSettings(enabled, 60, 80, 60, 60), (r.leadOne, r.leadTwo),
          eligible=True, v_ego=v, a_ego=a, base_tf=c.t_follow_last,
          stop_distance=c.stop_distance, maximum_accel=c.get_carrot_accel(v), comfort_brake=c.comfort_brake,
          cruise_speed=c.v_cruise, planned_accel=a)
        next_a = solve(m, c, r, v, a, request, flexible=enabled)
        flex_seen |= m.flex_gap.reduction > 0.
        if next_a < -.1 and first_decel is None: first_decel = t
        max_jerk = max(max_jerk, abs(next_a-a)/.05)
        next_v, next_vl = max(0., v+next_a*.05), max(0., vl+al*.05)
        distance += ((vl+next_vl)-(v+next_v))*.025
        minimum_gap = min(minimum_gap, distance)
        assert distance > 0., ('synthetic_collision', enabled, t)
        v, vl, a = next_v, next_vl, next_a
      records.append({'case':'kinematic_slowdown', 'enabled':enabled, 'minimum_gap':minimum_gap,
                      'first_decel_s':first_decel, 'max_jerk':max_jerk, 'flex_seen':bool(flex_seen)})

    # Direct feasible low-speed sequence demonstrates that the new branch
    # reaches the *numerical* cost reference, then cancels on a changed lead.
    c, m = carrot(3, .8), LongitudinalMpc()
    from openpilot.selfdrive.controls.lib.approach_comfort import ApproachRequest
    r = radar(3., 2.8, 8., -.3)
    for _ in range(300):
      solve(m, c, r, 3., -.3, ApproachRequest(), flexible=True)
    assert m.flex_gap.reduction > 0. and np.any(m.yref[:,0] < 0.)
    acquired = m.flex_gap.reduction
    r.leadOne.radarTrackId = 2
    solve(m, c, r, 3., -.3, ApproachRequest(), flexible=True)
    assert m.flex_gap.reduction == 0.
    records.append({'case':'native_mild_stop_and_identity_reset','acquired':acquired,'reset':True})

    # Requested speed preference with the real linked solver. Idealised fixed
    # lead/plant only; this is not physical headway or road qualification.
    from openpilot.selfdrive.controls.lib.longitudinal_mpc_lib.long_mpc import desired_follow_distance
    comparisons = []
    for enabled in (False, True):
      c,m = carrot(4,.7),LongitudinalMpc()
      c.v_cruise=110/3.6
      v,vl,a=100/3.6,100/3.6,0.
      distance=desired_follow_distance(v,vl,c.comfort_brake,c.stop_distance,.7)
      gaps=[];minimum=distance
      for frame in range(800):
        r=radar(v,vl,distance,0.)
        next_a=solve(m,c,r,v,a,ApproachRequest(),speed_gap=enabled)
        next_v=max(0.,v+next_a*.05)
        distance+=(2*vl-v-next_v)*.025
        minimum=min(minimum,distance)
        assert distance>0.,('speed_gap_ideal_collision',enabled,frame)
        v,a=next_v,next_a
        if frame>=700:gaps.append(distance)
      comparisons.append({'enabled':enabled,'mean_final_gap':float(np.mean(gaps)),
                          'minimum_gap':minimum,'t_follow':m.t_follow,'solver_status':m.solution_status})
    assert comparisons[1]['mean_final_gap'] < comparisons[0]['mean_final_gap'], comparisons
    records.append({'case':'speed_gap_100_kph_ideal_comparison','comparisons':comparisons})

    root = Path(__file__).resolve().parents[1]
    paths = ['selfdrive/controls/lib/approach_comfort.py','selfdrive/controls/lib/flexible_gap.py',
             'selfdrive/controls/lib/longitudinal_mpc_lib/long_mpc.py','selfdrive/carrot/carrot_functions.py']
    result = {'scope':'actual_x86_acados_solver_stock_tf_and_ideal_kinematic_plant_not_vehicle_qualification',
              'source_hashes':{p:hashlib.sha256((root/p).read_bytes()).hexdigest() for p in paths}, 'cases':records}
    args.output.write_text(json.dumps(result, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(result))


if __name__ == '__main__':
  main()
