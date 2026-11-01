"""Temporal reference and production MPC wiring; not vehicle qualification."""
from types import SimpleNamespace as NS

import numpy as np
import pytest

from openpilot.selfdrive.controls.lib.flexible_gap import DecelerationGap, FlexibleGapSettings
from openpilot.selfdrive.controls.tests.test_flexible_gap_mpc import production_mpc, update


def lead(**kw):
  return NS(**(dict(status=True, radar=True, radarTrackId=1, dRel=25., vLead=19.8,
                   vRel=-.2, aLeadK=-.3, jLead=0., modelProb=.99) | kw))


def step(f, count=1, leads=None, settings=FlexibleGapSettings(True, 80), **kw):
  args = dict(eligible=True, v_ego=20., a_ego=-.3, planned_a=-.3, base_tf=.9,
              stop_distance=5.5, comfort_brake=2.5) | kw
  for _ in range(count):
    result = f.update(settings, leads or (lead(),), **args)
  return result


def test_acquisition_requires_braking_already_started_and_is_bounded():
  for kw in (dict(a_ego=0.), dict(planned_a=0.)):
    assert step(DecelerationGap(), 200, **kw) == 0.
  assert step(DecelerationGap(), 200, leads=(lead(aLeadK=0.),)) == 0.
  f = DecelerationGap()
  assert step(f, 15) == 0.
  assert 0. < step(f) <= .002
  assert step(f, 200) == pytest.approx(.18)
  assert f.braking


@pytest.mark.parametrize('minimum', [70, 75, 80, 85, 90, 95, 100])
def test_profile_ratio_domain_and_neutral_100(minimum):
  result = step(DecelerationGap(), 220, settings=FlexibleGapSettings(True, minimum))
  assert result == pytest.approx(.9*(1.-minimum/100.))


@pytest.mark.parametrize('v,tf,d', [(3., .8, 9.), (50/3.6, .9, 20.), (100/3.6, .9, 34.)])
def test_low_middle_and_high_speed_use_only_nominal_moving_component(v, tf, d):
  f = DecelerationGap()
  result = step(f, 200, v_ego=v, base_tf=tf, leads=(lead(dRel=d, vLead=v-.2),))
  assert result == pytest.approx(tf*.2)


@pytest.mark.parametrize('kw', [dict(eligible=False), dict(a_ego=-1.2), dict(base_tf=float('nan')),
                               dict(stop_distance=-1.), dict(lead_brake=0.)])
def test_invalid_or_risk_cancels_without_stale_offset(kw):
  f = DecelerationGap(); assert step(f, 200) > 0.
  assert step(f, **kw) == 0.
  assert f.reduction == 0.


@pytest.mark.parametrize('change', [dict(radar=False), dict(modelProb=.5), dict(aLeadK=-2.),
                                   dict(vRel=-2.), dict(dRel=0.), dict(radarTrackId=8),
                                   dict(jLead=-1.), dict(vLead=float('nan'))])
def test_lead_risk_or_identity_never_keeps_comfort_relaxation(change):
  f = DecelerationGap(); assert step(f, 200) > 0.
  assert step(f, leads=(lead(**change),)) == 0.


def test_secondary_is_validated_and_cannot_be_relaxed_around():
  f = DecelerationGap()
  pair = (lead(), lead(radarTrackId=2, dRel=50.))
  assert step(f, 200, leads=pair) > 0.
  assert step(f, leads=(pair[0], lead(radarTrackId=2, dRel=0.))) == 0.


def test_entry_clearance_does_not_abruptly_cancel_an_existing_mild_maneuver():
  f = DecelerationGap(); before = step(f, 200)
  # A gradual range change crosses an acquisition boundary, not an identity
  # discontinuity or TTC/severe-braking risk threshold.
  for d in np.arange(24.9, 19.9, -.1):
    remaining = step(f, leads=(lead(dRel=float(d)),))
  assert remaining >= before and f.phase == 'holding'
  assert step(DecelerationGap(), 200, leads=(lead(dRel=20.),)) == 0.


def test_recovery_is_monotonic_takes_thirty_seconds_and_does_not_cap_launch():
  f = DecelerationGap(); initial = step(f, 200)
  trace = [step(f, leads=(lead(aLeadK=0.),), a_ego=0., planned_a=.2) for _ in range(600)]
  assert initial-trace[0] < 1e-7
  assert all(a >= b >= 0. for a, b in zip(trace, trace[1:]))
  assert trace[399] > 0.  # Still recovering after 20 s.
  assert trace[-1] == 0.
  assert f.phase == 'idle' and not f.braking


def test_traffic_braking_pauses_recovery_then_restarts_from_remaining_offset():
  f = DecelerationGap(); step(f, 200)
  remaining = step(f, 200, leads=(lead(aLeadK=0.),), a_ego=0., planned_a=0.)
  assert step(f, 80, a_ego=0., planned_a=0.) == remaining
  assert f.phase == 'holding' and f.recovery_s == 0.
  assert step(f, leads=(lead(aLeadK=0.),), a_ego=0., planned_a=0.) == pytest.approx(remaining, abs=1e-8)


def test_old_speed_boundary_does_not_reset_maneuver():
  f = DecelerationGap()
  step(f, 200, v_ego=6.01, base_tf=1.1, leads=(lead(dRel=14., vLead=5.81),))
  before = f.reduction
  step(f, v_ego=5.99, base_tf=1.1, leads=(lead(dRel=13.99, vLead=5.79),))
  assert before > 0. and f.reduction >= before


def test_identity_change_after_completed_recovery_requires_new_confirmation():
  f = DecelerationGap(); step(f, 200)
  step(f, 600, leads=(lead(aLeadK=0.),), a_ego=0., planned_a=0.)
  assert f.phase == 'idle' and f.reduction == 0.
  assert step(f, leads=(lead(radarTrackId=2),)) == 0.
  assert f.identity == (2,) and f.reason == 'track_identity_changed'


def mpc_braking():
  m, c, r, ns = production_mpc()
  m.x0[2] = -.3; m.x_sol[:, 2] = -.3
  c.get_T_FOLLOW = lambda *args: .9; c.flex_gap_nominal_tf = .9
  r.leadOne = lead()
  m.process_lead = lambda obj, j: (np.column_stack([obj.dRel+19.8*ns['T_IDXS'], np.full(13,19.8)]),19.8)
  return m, c, r, ns


def test_production_cost_changes_but_physical_obstacles_and_braking_bounds_do_not():
  a,c,r,_ = mpc_braking(); a.max_a=0.; update(a,c,r,observed_a_ego=-.3)
  b,c,r,_ = mpc_braking(); b.max_a=0.
  for _ in range(200):
    update(b,c,r,observed_a_ego=-.3,flex_gap_settings=FlexibleGapSettings(True,80),flex_gap_eligible=True)
  assert b.decel_gap.reduction > 0. and np.any(b.yref[:,0] < 0.)
  np.testing.assert_array_equal(a.params,b.params)
  assert b.desired_distance >= a.desired_distance - 20*.9*.2 - 1e-9
  # A limiting second object/stop/cruise obstacle still wins unmodified.
  r.leadTwo=lead(radarTrackId=2,dRel=18.)
  update(b,c,r,observed_a_ego=-.3,flex_gap_settings=FlexibleGapSettings(True,80),flex_gap_eligible=True)
  assert b.decel_gap.reduction == 0.


def test_recovery_does_not_zero_positive_acceleration_or_change_negative_limit():
  m,c,r,_=mpc_braking(); kw=dict(flex_gap_settings=FlexibleGapSettings(True,80),flex_gap_eligible=True)
  for _ in range(200):
    m.max_a=1.2; update(m,c,r,observed_a_ego=-.3,**kw)
  assert m.max_a==0.
  m.x0[2]=0.; r.leadOne.aLeadK=0.; m.max_a=1.2
  update(m,c,r,observed_a_ego=0.,**kw)
  assert m.decel_gap.phase=='recovering' and m.max_a==1.2
  assert np.all(m.params[:,0]==-3.5)


def test_disabled_and_ratio100_leave_all_mpc_targets_identical():
  a,c,r,_=mpc_braking(); update(a,c,r,observed_a_ego=-.3)
  for settings in (FlexibleGapSettings(False,80),FlexibleGapSettings(True,100)):
    b,c,r,_=mpc_braking()
    for _ in range(200):update(b,c,r,observed_a_ego=-.3,flex_gap_settings=settings,flex_gap_eligible=True)
    np.testing.assert_array_equal(a.params,b.params);np.testing.assert_array_equal(a.yref,b.yref)
