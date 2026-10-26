"""Preserve the owner's explicit 30 km/h lane-following opt-in for margins.

Uses the production-planner harness (native MPC doubled); no vehicle IO.
"""
import importlib.util
from pathlib import Path


_spec = importlib.util.spec_from_file_location(
  "owner_margin_harness", Path(__file__).with_name("test_lateral_margin_mode_audit.py"))
_harness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_harness)


def test_actual_margin_preserves_owner_native_zero_opt_in_at_30():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=0)
  assert not any(h.run(40, kph=31.99))
  assert h.planner.effective_lane_speed == 30
  assert h.step(kph=32.)
  assert h.step(kph=28.)
  assert not h.step(kph=27.99)
  assert h.params.values['UseLaneLineSpeed'] == 0


def test_owner_opt_in_does_not_lower_an_explicit_positive_lane_threshold():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=50)
  assert not any(h.run(40, kph=49.))
  assert h.planner.effective_lane_speed == 50
  assert h.step(kph=52.)


def test_off_and_shadow_preserve_native_zero_without_lane_selection():
  for settings in (_harness.OFF, _harness.SHADOW):
    h = _harness.Harness(settings, lane_speed=0)
    assert not any(h.run(180))
    assert h.planner.effective_lane_speed == 0
    assert h.planner.margin_applied == 0.


def test_owner_lane_opt_in_does_not_relax_narrow_corridor_margin_veto():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=0, lane_width=2.96)
  h.run(180)
  assert h.planner.lanelines_active
  assert h.planner.margin_applied == 0.
  assert h.planner.margin.reason == 'corridor_unqualified'


def test_lfa_long_press_off_does_not_turn_positive_native_setting_into_30():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=50)
  h.lane_speed = 0  # Runtime value after the preserved physical LFA gesture.
  h.run(60, kph=40.)
  assert h.planner.effective_lane_speed == 0
  assert not h.planner.lanelines_active


def test_lfa_long_press_on_preserves_runtime_selection_with_native_zero():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=0)
  h.lane_speed = 1
  h.run(40, kph=20.)
  assert h.planner.effective_lane_speed == 1
  assert h.planner.lanelines_active
