"""Exercise the production planner glue without native Params or an MPC binary.

The de-identified scene widths/speeds cover the 2026-10-26 audit: a rejected
margin must not silently select lane following instead of the model action.
"""
import ast
import math
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np
import pytest


ROOT = Path(__file__).resolve().parents[3]
DT = 0.05
OFF = {'YellowLineBiasCm': 0, 'NearbyVehicleBiasCm': 0, 'LateralMarginShadow': 0}
ACTUAL = {'YellowLineBiasCm': 10, 'NearbyVehicleBiasCm': 10, 'LateralMarginShadow': 0}
SHADOW = {**ACTUAL, 'LateralMarginShadow': 1}


def definitions(relative, names, scope):
  path = ROOT / relative
  tree = ast.parse(path.read_bytes(), filename=str(path))
  nodes = [n for n in tree.body if isinstance(n, (ast.ClassDef, ast.FunctionDef)) and n.name in names]
  assert {n.name for n in nodes} == set(names)
  exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), scope)


class ReadOnlyParams:
  def __init__(self, lane_speed, curve_threshold):
    self.values = {'UseLaneLineSpeed': lane_speed, 'UseLaneLineCurveSpeed': curve_threshold,
                   'PathOffset': 0, 'AdjustLaneOffset': 0, 'LatMpcInputOffset': 4}

  def get_int(self, key):
    return int(self.values.get(key, 0))

  def get_float(self, key):
    return float(self.values.get(key, 0))


class MpcDouble:
  def __init__(self):
    self.x_sol = np.zeros((17, 4))
    self.u_sol = np.zeros((16, 1))
    self.solution_status = 0
    self.cost = self.solve_time = 0.

  def reset(self, **_):
    pass

  def set_weights(self, *_):
    pass

  def run(self, _x0, _p, y_pts, _heading_pts, _yaw_rate_pts):
    self.last_y = y_pts.copy()


class SubMasterDouble(dict):
  pass


class Harness:
  def __init__(self, settings, lane_speed, lane_width=3.6, curve_threshold=0):
    self.now_ns = 10_000_000_000
    self.lane_speed = lane_speed
    self.lane_width = lane_width
    self.settings = dict(settings)
    self.params = ReadOnlyParams(lane_speed, curve_threshold)
    prefs = NS(values=dict(settings), read=lambda: dict(self.settings))
    lane_scope = {'np': np, 'math': math, 'DT_MDL': DT, 'TRAJECTORY_SIZE': 33,
                  'CAMERA_OFFSET': 0., 'Params': lambda: self.params,
                  'log': NS(Desire=NS(none=0, laneChangeLeft=3, laneChangeRight=4))}
    definitions('common/filter_simple.py', ['FirstOrderFilter'], lane_scope)
    definitions('selfdrive/controls/lib/lane_planner_2.py', ['LanePlanner', 'LateralMargin'], lane_scope)
    scope = {
      'np': np, 'DT_MDL': DT, 'TRAJECTORY_SIZE': 33, 'LAT_MPC_N': 16, 'MIN_SPEED': 1.,
      'LATERAL_ACCEL_COST': 0., 'LATERAL_JERK_COST': .04, 'STEERING_RATE_COST': 700.,
      'time': NS(monotonic_ns=lambda: self.now_ns, monotonic=lambda: self.now_ns / 1e9),
      'cloudlog': NS(debug=lambda *_: None, warning=lambda *_: None),
      'log': lane_scope['log'], 'Params': lambda: self.params, 'LateralMpc': MpcDouble,
      'DrivingPreferences': lambda: prefs, 'LanePlanner': lane_scope['LanePlanner'],
      'LateralMargin': lane_scope['LateralMargin'],
    }
    definitions('selfdrive/controls/lib/lateral_planner.py',
                ['LateralPlanner', 'smooth_moving_avg', 'yaw_from_path_no_scipy'], scope)
    cp = NS(wheelbase=3., centerToFront=1.2, mass=2100., tireStiffnessRear=100000.,
            carFingerprint='HYUNDAI_PALISADE_LX3_HEV')
    self.planner = scope['LateralPlanner'](cp)
    self.carrot = NS(atc_active=False)

  def step(self, kph=60., slowdown=False, curve_speed=250., lat_active=True):
    self.now_ns += 50_000_000
    t = np.arange(33) * .1
    x = np.linspace(0., 60., 33)
    zero = np.zeros(33)
    # Model starts at the vehicle origin; a lane centre 15 cm to the right
    # makes accidental controller/path substitution observable.
    centre = .15
    lane_y = (centre - 1.5 * self.lane_width, centre - self.lane_width / 2,
              centre + self.lane_width / 2, centre + 1.5 * self.lane_width)
    lines = [NS(x=x, y=np.full(33, y), t=t) for y in lane_y]
    edges = [NS(x=x, y=np.full(33, y), t=t) for y in (-5.4, 5.4)]
    v = np.full(33, kph / 3.6)
    if slowdown:
      v[-1] *= .6
    md = NS(position=NS(x=x, y=zero, z=zero, t=t),
            orientation=NS(x=zero, z=zero), orientationRate=NS(z=zero),
            velocity=NS(x=v, y=zero, z=zero), acceleration=NS(x=zero),
            laneLines=lines, roadEdges=edges, roadEdgeStds=[1., 1.],
            laneLineProbs=[.9, .99, .99, .9], laneLineStds=[.05] * 4,
            meta=NS(desire=0, desireState=[], laneWidthLeft=3.6, laneWidthRight=3.6))
    cs = NS(vEgo=kph / 3.6, useLaneLineSpeed=self.lane_speed, canValid=True, canTimeout=False,
            leftBlinker=False, rightBlinker=False, steeringPressed=False,
            leftLaneLine=21, rightLaneLine=10, leftBlindspot=False, rightBlindspot=False)
    sm = SubMasterDouble(carState=cs, carControl=NS(latActive=lat_active),
                         controlsState=NS(curvature=0.), carrotMan=NS(vTurnSpeed=curve_speed),
                         radarState=NS(leadsLeft=[], leadsRight=[], leadsCutIn=[]), modelV2=md)
    sm.valid = sm.alive = {name: True for name in sm}
    sm.logMonoTime = {name: self.now_ns for name in sm}
    self.planner.update(sm, self.carrot)
    return self.planner.lanelines_active

  def run(self, frames, **kwargs):
    return [self.step(**kwargs) for _ in range(frames)]


@pytest.mark.parametrize('settings', [OFF, ACTUAL, SHADOW], ids=['off', 'actual', 'shadow'])
@pytest.mark.parametrize('width', [2.70, 2.92, 2.96], ids=['scene_a', 'scene_b', 'scene_c'])
def test_native_zero_never_selects_lanes_for_a_margin(settings, width):
  h = Harness(settings, lane_speed=0, lane_width=width)
  # Also clear a pre-existing mode latch; zero is an unconditional native OFF.
  h.planner.useLaneLineMode = True
  modes = []
  for speed in (42.7, 49.6, 55.5, 64.3, 76.1, 61.7):
    modes.extend(h.run(30, kph=speed))
    assert h.planner.effective_lane_speed == 0
    assert h.planner.margin_applied == 0.
    np.testing.assert_array_equal(h.planner.path_xyz[:, 1], np.zeros(33))
  assert not any(modes)
  assert sum(a != b for a, b in zip(modes, modes[1:])) == 0
  if settings == ACTUAL:
    assert h.planner.margin.reason == 'native_lane_mode_disabled'


@pytest.mark.parametrize('settings', [OFF, ACTUAL, SHADOW], ids=['off', 'actual', 'shadow'])
def test_native_positive_hysteresis_is_independent_of_margin_options(settings):
  h = Harness(settings, lane_speed=50)
  assert h.run(40)[-1]
  speeds = [51., 50., 48., 47.9, 50., 51.99, 52.]
  assert [h.step(kph=speed) for speed in speeds] == [True, True, True, False, False, False, True]
  assert h.planner.effective_lane_speed == 50


@pytest.mark.parametrize('settings', [OFF, ACTUAL, SHADOW], ids=['off', 'actual', 'shadow'])
def test_model_slowdown_still_forces_laneless_then_waits_for_recovery(settings):
  h = Harness(settings, lane_speed=50)
  assert h.run(40)[-1]
  assert not h.step(slowdown=True)
  assert not any(h.run(20))
  assert h.step()


def test_explicit_native_lane_mode_still_allows_guarded_preference():
  h = Harness(ACTUAL, lane_speed=50)
  h.run(180)
  assert h.planner.lanelines_active
  assert h.planner.margin.reason == 'toward_left_yellow'
  assert h.planner.margin_applied < -.09


def test_native_curve_gate_is_not_bypassed_by_margin():
  h = Harness(ACTUAL, lane_speed=50, curve_threshold=100)
  h.run(180, curve_speed=50)
  assert h.planner.lanelines_active
  assert h.planner.margin_applied == 0.
  assert h.planner.margin.reason == 'inactive_laneless_or_control_off'
  h.run(180, curve_speed=250)
  assert h.planner.margin_applied < -.09


def test_narrow_corridor_guard_remains_when_native_lane_mode_is_explicit():
  h = Harness(ACTUAL, lane_speed=50, lane_width=2.96)
  h.run(180)
  assert h.planner.lanelines_active
  assert h.planner.margin_applied == 0.
  assert h.planner.margin.reason == 'corridor_unqualified'


def test_shadow_can_still_record_laneless_candidates_without_changing_the_path():
  h = Harness(SHADOW, lane_speed=0)
  h.run(180)
  assert not h.planner.lanelines_active
  assert h.planner.margin_applied == 0.
  assert h.planner.margin.value < -.09
  np.testing.assert_array_equal(h.planner.path_xyz[:, 1], np.zeros(33))


def test_preference_changes_do_not_latch_a_native_zero_mode_on():
  h = Harness(ACTUAL, lane_speed=0)
  modes = []
  for settings in (ACTUAL, OFF, SHADOW, ACTUAL):
    h.settings = dict(settings)
    modes.extend(h.run(40))
    assert h.planner.effective_lane_speed == 0
    assert h.planner.margin_applied == 0.
  assert not any(modes)
  assert h.planner.margin.reason == 'native_lane_mode_disabled'
