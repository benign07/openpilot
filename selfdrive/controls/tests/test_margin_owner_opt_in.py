"""Native model-mode default is independent of position preferences.

Uses the production-planner harness (native MPC doubled); no vehicle IO.
"""
import importlib.util
import json
from pathlib import Path


_spec = importlib.util.spec_from_file_location(
  "owner_margin_harness", Path(__file__).with_name("test_lateral_margin_mode_audit.py"))
_harness = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_harness)


def test_native_lane_setting_defaults_to_model_mode():
  data=json.loads((_harness.ROOT/'selfdrive/carrot_settings.json').read_text(encoding='utf8'))
  assert next(p for p in data['params'] if p['name']=='UseLaneLineSpeed')['default']==0


def test_actual_margin_keeps_native_zero_in_model_mode_at_all_speeds():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=0)
  for speed in (20., 28., 32., 60., 100., 130.):
    assert not any(h.run(40, kph=speed))
    assert h.planner.useLaneLineSpeedApply == 0
    assert h.planner.margin_applied == 0.
  assert h.params.values['UseLaneLineSpeed'] == 0


def test_position_preferences_do_not_lower_an_explicit_positive_lane_threshold():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=50)
  assert not any(h.run(40, kph=49.))
  assert h.planner.useLaneLineSpeedApply == 50
  assert h.step(kph=52.)


def test_off_and_shadow_preserve_native_zero_without_lane_selection():
  for settings in (_harness.OFF, _harness.SHADOW):
    h = _harness.Harness(settings, lane_speed=0)
    assert not any(h.run(180))
    assert h.planner.useLaneLineSpeedApply == 0
    assert h.planner.margin_applied == 0.


def test_explicit_native_lane_mode_retains_narrow_corridor_margin_veto():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=50, lane_width=2.96)
  h.run(180)
  assert h.planner.lanelines_active
  assert h.planner.margin_applied == 0.
  assert h.planner.margin.reason == 'corridor_unqualified'


def test_lfa_long_press_off_does_not_turn_positive_native_setting_into_30():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=50)
  h.lane_speed = 0  # Runtime value after the preserved physical LFA gesture.
  h.run(60, kph=40.)
  assert h.planner.useLaneLineSpeedApply == 0
  assert not h.planner.lanelines_active


def test_lfa_long_press_on_preserves_runtime_selection_with_native_zero():
  h = _harness.Harness(_harness.ACTUAL, lane_speed=0)
  h.lane_speed = 1
  h.run(40, kph=20.)
  assert h.planner.useLaneLineSpeedApply == 1
  assert h.planner.lanelines_active
