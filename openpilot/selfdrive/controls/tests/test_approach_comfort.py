import math
from types import SimpleNamespace
import unittest

from openpilot.selfdrive.controls.lib.approach_comfort import (
  ApproachComfort, ApproachRequest, ApproachSettings, apply_approach_ceiling,
)


def lead(d=95.0, v=20.0, vr=-5.0, a=0.0, track=1, radar=True):
  return SimpleNamespace(status=True, radar=radar, radarTrackId=track, dRel=d, vLead=v, vRel=vr, aLeadK=a)


class TestApproachComfort(unittest.TestCase):
  def setUp(self):
    self.state = ApproachComfort()
    self.settings = ApproachSettings(True, 100, 100, 100, 100)

  def run_profile(self, leads=None, count=100, **kwargs):
    inputs = dict(eligible=True, v_ego=25.0, a_ego=0.0, base_tf=1.5, stop_distance=6.0, maximum_accel=1.2)
    inputs.update(kwargs)
    result = ApproachRequest()
    for _ in range(count):
      result = self.state.update(self.settings, leads or [lead()], **inputs)
    return result

  def test_disabled_is_exact_stock(self):
    self.run_profile()
    self.settings = ApproachSettings(False)
    result = self.run_profile(count=1)
    self.assertEqual(result, ApproachRequest())
    for maximum in (-2.5, -0.2, 0.0, 1.7):
      self.assertEqual(apply_approach_ceiling(maximum, 1.0, result), maximum)

  def test_slow_lead_before_current_braking_distance(self):
    # Base kinematic reference = 6+37.5+(625-400)/5 = 88.5m.
    result = self.run_profile([lead(d=95)])
    self.assertLess(result.accel_ceiling, 1.2)
    self.assertGreater(result.extra_tf, 0.0)
    self.assertLessEqual(result.extra_tf, 0.5)

  def test_braking_equal_speed_lead(self):
    result = self.run_profile([lead(d=50, v=25, vr=0, a=-1.0)])
    self.assertLess(result.accel_ceiling, 1.2)

  def test_far_or_opening_lead_does_not_interfere(self):
    for obj in (lead(d=250, a=-2), lead(v=35, vr=10, a=-0.5), lead(v=25, vr=0, a=0)):
      self.state.reset()
      self.assertEqual(self.run_profile([obj]), ApproachRequest())

  def test_requires_stable_radar_track(self):
    self.assertEqual(self.run_profile(count=2), ApproachRequest())
    self.assertIsNotNone(self.run_profile(count=1).accel_ceiling)
    self.assertEqual(self.run_profile([lead(track=2)], count=1), ApproachRequest())
    self.assertEqual(self.run_profile([lead(radar=False)]), ApproachRequest())

  def test_duplicate_slots_do_not_double_confirmation(self):
    obj = lead()
    self.assertEqual(self.run_profile([obj, obj], count=2), ApproachRequest())
    self.assertIsNotNone(self.run_profile([obj, obj], count=1).accel_ceiling)

  def test_invalid_and_ineligible_reset(self):
    for override in ({'eligible': False}, {'v_ego': math.nan}, {'comfort_brake': 0.0}):
      self.run_profile()
      self.assertEqual(self.run_profile(count=1, **override), ApproachRequest())
    self.run_profile()
    self.assertEqual(self.run_profile([lead(d=math.nan)], count=1), ApproachRequest())

  def test_no_negative_ceiling_is_relaxed(self):
    request = self.run_profile()
    for limit in (-3.5, -1.0, -0.1):
      self.assertEqual(apply_approach_ceiling(limit, 0.5, request), limit)
    self.assertGreaterEqual(apply_approach_ceiling(1.2, 1.0, request), 0.95)

  def test_stop_profile_does_not_change_stop_distance(self):
    obj = lead(d=12, v=1.0, vr=-1.0)
    result = self.run_profile([obj], v_ego=2.0, a_ego=-0.3)
    self.assertGreater(result.jerk_factor, 1.0)
    self.assertLessEqual(result.jerk_factor, 1.5)
    self.assertEqual(result.extra_tf, 0.0)
    self.assertGreaterEqual(result.accel_ceiling, 0.0)

  def test_close_second_lead_disables_smoothing(self):
    result = self.run_profile([lead(d=12, v=1, vr=-1), lead(d=7, v=1, vr=-1, track=2)], v_ego=2, a_ego=-0.3)
    self.assertEqual(result.jerk_factor, 1.0)

  def test_stronger_braking_keeps_original_jerk_cost(self):
    result = self.run_profile([lead(d=12, v=1, vr=-1)], v_ego=2, a_ego=-1.0)
    self.assertEqual(result.jerk_factor, 1.0)

  def test_margin_and_accel_release_are_progressive(self):
    first = self.run_profile([lead(d=30)])
    after = self.run_profile([lead(d=60, v=30, vr=5)], count=1)
    self.assertGreater(after.extra_tf, 0.0)
    self.assertLessEqual(first.extra_tf - after.extra_tf, 0.00400001)
    self.assertLess(after.accel_ceiling, 1.2)

  def test_zero_preferences_neutral(self):
    self.settings = ApproachSettings(True, 0, 0, 0, 0)
    self.assertEqual(self.run_profile(), ApproachRequest())

  def test_param_clamping(self):
    params = SimpleNamespace(get_bool=lambda n: True, get_int=lambda n: 10000)
    self.assertEqual(ApproachSettings.read(params), self.settings)

  def test_departing_lead_does_not_hold_traffic_cap(self):
    moving_away = lead(d=12, v=4, vr=2)
    self.assertEqual(self.run_profile([moving_away], v_ego=2), ApproachRequest())
    self.run_profile([lead(d=12, v=1, vr=-1)], v_ego=2)
    result = self.run_profile([moving_away], v_ego=2, count=21)
    self.assertIsNone(result.accel_ceiling)

  def test_small_clearance_noise_does_not_reenter_smoothing(self):
    self.run_profile([lead(d=12, v=1, vr=-1)], v_ego=2, a_ego=-0.3)
    for _ in range(20):
      for distance in (9.4, 10.1):
        result = self.run_profile([lead(d=distance, v=1, vr=-1)], count=1, v_ego=2, a_ego=-0.3)
        self.assertEqual(result.jerk_factor, 1.0)

  def test_smoothing_holds_inside_hysteresis_band(self):
    first = self.run_profile([lead(d=12, v=1, vr=-1)], v_ego=2, a_ego=-0.3)
    after = self.run_profile([lead(d=9.8, v=1, vr=-1)], count=5, v_ego=2, a_ego=-0.3)
    self.assertEqual(after.jerk_factor, first.jerk_factor)
    threat = self.run_profile([lead(d=9.4, v=1, vr=-1)], count=1, v_ego=2, a_ego=-0.3)
    self.assertEqual(threat.jerk_factor, 1.0)

  def test_benign_smoothing_exit_is_progressive(self):
    first = self.run_profile([lead(d=12, v=1, vr=-1)], v_ego=2, a_ego=-0.3)
    after = self.run_profile([lead(d=15, v=3, vr=1)], count=1, v_ego=2, a_ego=-0.3)
    self.assertGreater(after.jerk_factor, 1.0)
    self.assertLessEqual(first.jerk_factor-after.jerk_factor, 0.0250001)

  def test_smoothing_requires_half_second_after_acquisition(self):
    obj = lead(d=12, v=1, vr=-1)
    first = self.run_profile([obj], count=11, v_ego=2, a_ego=-0.3)
    self.assertEqual(first.jerk_factor, 1.0)
    after = self.run_profile([obj], count=1, v_ego=2, a_ego=-0.3)
    self.assertGreater(after.jerk_factor, 1.0)


if __name__ == '__main__':
  unittest.main()
