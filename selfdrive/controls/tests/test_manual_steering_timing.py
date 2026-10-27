"""Real handoff envelope and actuator ceiling; no hardware grants or CAN TX."""
import pytest

from openpilot.selfdrive.carrot.manual_steering import ManualSteeringPause
from opendbc.car.hyundai.manual_steering import ManualSteeringCeiling


DT = .01


def update(pause, **changes):
  args = dict(enabled=True, lane_change_off=True, left_blinker=False, right_blinker=False,
              steering_pressed=False, steering_angle=0., can_valid=True, can_timeout=False,
              lateral_active=True, explicit_resume=False)
  args.update(changes)
  return pause.update(**args)


def handoff():
  pause, ceiling = ManualSteeringPause(DT), ManualSteeringCeiling(DT)
  ceiling.update(250., 1., True)
  for _ in range(110):
    paused = update(pause, left_blinker=True)
    ceiling.update(250., pause.scale, not (paused and pause.scale <= 0.), pause.resuming)
  assert pause.paused and pause.scale == 0. and ceiling.previous == 0.
  return pause, ceiling


def test_normal_return_waits_half_second_then_fades_in_without_step():
  pause, ceiling = handoff()
  for _ in range(49):
    assert update(pause) and pause.scale == 0.
  assert not update(pause)  # Old one-second clear delay must not remain.
  assert pause.resuming and pause.scale == 0.  # No old direct jump to 1.
  value = ceiling.update(250., pause.scale, True, pause.resuming)
  for i in range(1, 51):
    assert not update(pause)
    assert pause.scale == pytest.approx(min(1., i / 50))
    next_value = ceiling.update(250., pause.scale, True, pause.resuming)
    assert value <= next_value <= value + 5.000001
    value = next_value
  assert value == pytest.approx(250.) and not pause.resuming


def test_explicit_accepted_resume_uses_same_half_second_fade():
  pause, ceiling = handoff()
  for i in range(1, 51):
    assert not update(pause, explicit_resume=i == 1)
    assert pause.scale == pytest.approx(min(1., i / 50))
    value = ceiling.update(250., pause.scale, True, pause.resuming)
    assert value == pytest.approx(min(250., i * 5))


@pytest.mark.parametrize('changes', [dict(left_blinker=True), dict(right_blinker=True),
  dict(left_blinker=True, right_blinker=True), dict(steering_pressed=True),
  dict(steering_angle=16.), dict(can_valid=False), dict(can_timeout=True)])
def test_shorter_delay_does_not_bypass_existing_clear_requirements(changes):
  pause, _ = handoff()
  for _ in range(160):
    assert update(pause, **changes) and pause.scale == 0.


def test_return_never_supplies_output_without_lateral_permission():
  pause, ceiling = handoff()
  for _ in range(200):
    update(pause, lateral_active=False)
    assert pause.scale == 0.
    assert ceiling.update(250., pause.scale, False, pause.resuming) == 0.


def test_new_signal_during_fade_in_reduces_from_actual_partial_output():
  pause, ceiling = handoff()
  for i in range(10):
    update(pause, explicit_resume=i == 0)
    previous = ceiling.update(250., pause.scale, True, pause.resuming)
  assert previous == pytest.approx(50.)
  assert update(pause, right_blinker=True)
  value = ceiling.update(250., pause.scale, True, pause.resuming)
  assert 0 <= value <= previous and not pause.resuming


def test_short_signal_partial_fade_cannot_step_to_full_on_automatic_return():
  pause, ceiling = ManualSteeringPause(DT), ManualSteeringCeiling(DT)
  ceiling.update(100., 1., True)
  for _ in range(10):
    update(pause, left_blinker=True)
    ceiling.update(250., pause.scale, True, pause.resuming)
  for _ in range(50):
    update(pause)
    value = ceiling.update(250., pause.scale, True, pause.resuming)
  assert pause.resuming and value < 100.
  for _ in range(60):
    update(pause)
    next_value = ceiling.update(250., pause.scale, True, pause.resuming)
    assert value <= next_value <= value + 5.000001
    value = next_value
  assert value == pytest.approx(250.)


def test_hud_description_matches_timing_and_preserves_parameter_contract():
  import copy
  import json
  from pathlib import Path
  from openpilot.selfdrive.carrot.server.services.settings import inject_driving_preferences
  original = json.loads((Path(__file__).resolve().parents[2] / 'carrot_settings.json').read_text(encoding='utf8'))
  old = next(p for p in original['params'] if p['name'] == 'ManualSteerWithBlinker')
  data = inject_driving_preferences(copy.deepcopy(original))
  new = next(p for p in data['params'] if p['name'] == old['name'])
  assert all(new[k] == old[k] for k in ('name', 'min', 'max', 'default', 'unit', 'control'))
  assert '0.2초' in new['descr'] and '0.2seconds' in new['edescr']
  assert new['group'] == '개발 중 기능'
  def names(nodes):
    return [p for node in nodes for p in node.get('params', [])] + [p for node in nodes for p in names(node.get('groups', []))]
  assert names(data['menu']).count(old['name']) == 1
  assert old['name'] in names(data['menu'][:1])
  assert inject_driving_preferences(copy.deepcopy(data)) == data
