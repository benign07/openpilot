"""Comfort preferences compose after stock TF; physical constraints stay stock."""
from types import SimpleNamespace as NS

import numpy as np
import pytest

from openpilot.selfdrive.controls.lib.approach_comfort import ApproachComfort, ApproachSettings
from openpilot.selfdrive.controls.lib.flexible_gap import FlexibleGapSettings
from openpilot.selfdrive.controls.tests.test_flexible_gap_mpc import production_mpc, update


def test_existing_decel_headroom_is_credited_instead_of_added_twice():
  m, c, r, _ = production_mpc()
  c.flex_gap_nominal_tf = 1.8  # Applied TF is 2.0: stock already added 0.2 s.
  update(m, c, r, approach_extra_tf=.3)
  np.testing.assert_allclose(m.yref[:, 0], 20. * .1 / 30.)


def test_larger_stock_boost_never_gets_removed_by_early_preference():
  m, c, r, _ = production_mpc()
  c.flex_gap_nominal_tf = 1.6
  update(m, c, r, approach_extra_tf=.3)
  np.testing.assert_array_equal(m.yref[:, 0], 0.)
  assert m.t_follow == 2.


def test_mid_speed_margin_does_not_fade_away_at_50_kph():
  p = ApproachComfort()
  lead = NS(status=True, radar=True, radarTrackId=1, dRel=12., vLead=50/3.6-3., vRel=-3., aLeadK=-1.)
  for _ in range(200):
    request = p.update(ApproachSettings(True, 60, 80, 60, 60), [lead], eligible=True,
                       v_ego=50/3.6, a_ego=-.2, base_tf=1., stop_distance=5.5, maximum_accel=1.2)
  assert .35 < request.extra_tf <= .4


def test_mid_speed_margin_does_not_expand_low_speed_opening_lead_preview():
  lead = NS(status=True, radar=True, radarTrackId=1, dRel=10., vLead=2.5, vRel=.5, aLeadK=0.)
  requests = []
  for margin in (40, 80):
    p = ApproachComfort()
    for _ in range(120):
      request = p.update(ApproachSettings(True, 40, margin, 40, 40), [lead], eligible=True,
                         v_ego=2., a_ego=.2, base_tf=.7, stop_distance=5.5,
                         maximum_accel=1.2, cruise_speed=18., planned_accel=.5)
    requests.append(request)
  assert requests[0].accel_ceiling == requests[1].accel_ceiling


def stop_mpc():
  m, c, r, ns = production_mpc()
  m.x0 = np.array([0., 3., -.3])
  m.x_sol = np.tile([0., 3., -.3], (13, 1))
  c.get_T_FOLLOW = lambda *args: .8
  c.flex_gap_nominal_tf = .8
  r.leadOne = NS(status=True, radar=True, radarTrackId=1, dRel=8., vLead=2.8,
                 vRel=-.2, aLeadK=-.3, jLead=0., modelProb=.99)
  m.process_lead = lambda obj, j: (
    np.column_stack([obj.dRel + 2.8 * ns['T_IDXS'], np.full(13, 2.8)]), 2.8)
  return m, c, r


def test_opt_in_mild_stop_can_use_short_gap_without_rewriting_constraint():
  m, c, r = stop_mpc()
  baseline, bc, br = stop_mpc()
  baseline.max_a = m.max_a = 0.
  update(baseline, bc, br)
  for _ in range(300):
    update(m, c, r, flex_gap_settings=FlexibleGapSettings(True, 80), flex_gap_eligible=True,
           flex_gap_gentle_stop=True, observed_a_ego=-.3)
  assert m.flex_gap.reduction > 0.
  assert np.any(m.yref[:, 0] < 0.)
  np.testing.assert_array_equal(m.params, baseline.params)
  assert m.desired_distance >= baseline.desired_distance - 3. * .8 * .2 - 1e-9


@pytest.mark.parametrize('change', [dict(aLeadK=-2.), dict(vRel=-2.), dict(jLead=-1.),
                                         dict(modelProb=.1), dict(radarTrackId=8), dict(dRel=6.)])
def test_mild_stop_risk_restores_unmodified_target(change):
  m, c, r = stop_mpc()
  kw = dict(flex_gap_settings=FlexibleGapSettings(True, 80), flex_gap_eligible=True,
            flex_gap_gentle_stop=True, observed_a_ego=-.3)
  for _ in range(300):
    update(m, c, r, **kw)
  assert m.flex_gap.reduction > 0.
  for key, value in change.items():
    setattr(r.leadOne, key, value)
  update(m, c, r, **kw)
  assert m.flex_gap.reduction == 0.
  np.testing.assert_array_equal(m.yref[:, 0], 0.)


def test_standstill_never_uses_gap_to_request_a_creep():
  m, c, r = stop_mpc()
  m.x0[1] = 0.
  for _ in range(300):
    update(m, c, r, flex_gap_settings=FlexibleGapSettings(True, 80), flex_gap_eligible=True,
           flex_gap_gentle_stop=True, observed_a_ego=-.3)
  assert m.flex_gap.reduction == 0.
  np.testing.assert_array_equal(m.yref[:, 0], 0.)
