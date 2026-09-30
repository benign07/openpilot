"""Offline session regressions; no sockets, vehicle or actuator access.

Run with python -m unittest discover -s selfdrive/selfdrived/tests -p test_lx3_engagement.py.
The real StateMachine and SelfdriveD adapter bodies are executed with a small
message facade. Event type membership is read from production EVENTS, not copied.
Native IPC, Panda firmware and physical CAN timing need separate qualification.
"""
import ast
import contextlib
import copy
from enum import IntFlag
import io
import math
from pathlib import Path
import runpy
from types import SimpleNamespace as NS
import unittest

ROOT = Path(__file__).resolve().parents[3]
MODULE = runpy.run_path(str(ROOT / 'selfdrive/selfdrived/lx3_engagement.py'))
Lx3Engagement = MODULE['Lx3Engagement']
Mode = MODULE['EngagementMode']
permissions = MODULE['lx3_control_permissions']
pandas_ready = MODULE['lx3_pandas_ready']


def load_definitions(path, env, names):
  nodes = [n for n in ast.walk(ast.parse(path.read_text(encoding='utf-8')))
           if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
  exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)


SAFETY_ENV = dict(IntFlag=IntFlag)
load_definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/values.py', SAFETY_ENV, {'HyundaiSafetyFlags'})
SafetyFlags = SAFETY_ENV['HyundaiSafetyFlags']
LX3_SAFETY_PARAM = 190 | SafetyFlags.LX3_ENGAGEMENT_GUARD.value


EVENT_TYPES = {}
event_tree = ast.parse((ROOT / 'selfdrive/selfdrived/events.py').read_text(encoding='utf-8'))
for node in event_tree.body:
  if isinstance(node, (ast.Assign, ast.AnnAssign)) and isinstance(node.value, ast.Dict):
    for name, value in zip(node.value.keys, node.value.values):
      if isinstance(name, ast.Attribute) and isinstance(value, ast.Dict):
        EVENT_TYPES[name.attr] = {key.attr for key in value.keys if isinstance(key, ast.Attribute)}


class Events:
  def __init__(self, *names):
    self.events = list(names)

  def add(self, name):
    self.events.append(name)

  def contains(self, event_type):
    return any(event_type in EVENT_TYPES.get(name, ()) for name in self.events)


ET = NS(**{name: name for names in EVENT_TYPES.values() for name in names})
State = NS(disabled='disabled', preEnabled='preEnabled', enabled='enabled',
           softDisabling='softDisabling', overriding='overriding')
ENV = dict(Events=Events, ET=ET, State=State, DT_CTRL=.01, SOFT_DISABLE_TIME=3,
           ACTIVE_STATES=(State.enabled, State.softDisabling, State.overriding),
           ENABLED_STATES=(State.preEnabled, State.enabled, State.softDisabling, State.overriding),
           EventName=NS(**{name: name for name in EVENT_TYPES}), EngagementMode=Mode,
           time=NS(monotonic=lambda: 0.0),
           lx3_pandas_ready=pandas_ready, car=NS(CarParams=NS(SteerControlType=NS(angle='angle'))))
load_definitions(ROOT / 'selfdrive/selfdrived/state.py', ENV, {'StateMachine'})
load_definitions(ROOT / 'selfdrive/selfdrived/selfdrived.py', ENV, {'update_lx3_state'})


def button(name, pressed=False):
  return NS(type=name, pressed=pressed)


class SubMaster(dict):
  valid_streams = True

  def all_checks(self, _):
    return self.valid_streams


class TestLx3Session(unittest.TestCase):
  def setUp(self):
    self.now = 0.0
    ENV['time'] = NS(monotonic=lambda: self.now)
    self.panda = NS(safetyModel='hyundaiCanfd', safetyParam=LX3_SAFETY_PARAM, alternativeExperience=0,
                    controlsAllowed=True, safetyRxChecksInvalid=False, faults=[])
    self.ctx = NS(lx3_engagement=Lx3Engagement(), car_state_fresh=True,
                  CP=NS(openpilotLongitudinalControl=True, steerControlType='angle', alternativeExperience=0,
                        safetyConfigs=[NS(safetyModel='hyundaiCanfd', safetyParam=LX3_SAFETY_PARAM)]),
                  sm=SubMaster(pandaStates=[self.panda]), state_machine=ENV['StateMachine'](),
                  enabled=False, active=False)
    self.cs = NS(canValid=True, steerFaultTemporary=False, steerFaultPermanent=False, buttonEvents=[])

  def step(self, *buttons, events=()):
    self.ctx.events = Events(*events)
    self.cs.buttonEvents = list(buttons)
    with contextlib.redirect_stdout(io.StringIO()):
      ENV['update_lx3_state'](self.ctx, self.cs)
    return permissions(self.ctx.lx3_engagement.mode, self.ctx.enabled, self.ctx.active, True, True, True)

  def test_lfa_uses_normal_enabled_state_without_longitudinal(self):
    self.assertEqual(self.step(button('lfaButton')), (True, False))
    self.assertEqual(self.ctx.state_machine.state, State.enabled)
    self.assertTrue(self.ctx.enabled)

  def test_scc_from_off_enables_both(self):
    self.assertEqual(self.step(button('mainCruise')), (True, True))

  def test_scc_upgrades_lateral(self):
    self.step(button('lfaButton'))
    self.assertEqual(self.step(button('mainCruise')), (True, True))

  def test_scc_off_disables_both(self):
    self.step(button('mainCruise'))
    self.assertEqual(self.step(button('mainCruise')), (False, False))

  def test_lfa_off_ends_session(self):
    for start in ('lfaButton', 'mainCruise'):
      self.setUp()
      self.step(button(start))
      self.assertEqual(self.step(button('lfaButton')), (False, False))

  def test_res_set_reenable_both_after_lfa_off(self):
    for resume in ('accelCruise', 'decelCruise'):
      self.setUp()
      self.step(button('lfaButton'))
      self.step(button('lfaButton'))
      self.assertEqual(self.step(button(resume)), (True, True))

  def test_cancel_dominates_simultaneous_enable(self):
    self.assertEqual(self.step(button('lfaButton'), button('cancel', True), button('mainCruise')), (False, False))

  def test_press_and_hold_does_not_engage(self):
    for _ in range(100):
      self.assertEqual(self.step(button('lfaButton', True)), (False, False))
    self.assertEqual(self.step(button('lfaButton')), (True, False))

  def test_no_entry_is_not_latched(self):
    self.assertEqual(self.step(button('lfaButton'), events=('seatbeltNotLatched',)), (False, False))
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.step(button('lfaButton')), (True, False))

  def test_upgrade_cannot_bypass_no_entry(self):
    self.step(button('lfaButton'))
    self.assertEqual(self.step(button('mainCruise'), events=('seatbeltNotLatched',)), (False, False))

  def test_automatic_or_stock_enable_is_ignored(self):
    self.assertEqual(self.step(events=('buttonEnable', 'pcmEnable')), (False, False))

  def test_enabled_state_without_driver_intent_is_reset(self):
    self.ctx.enabled = self.ctx.active = True
    self.ctx.state_machine.state = State.enabled
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.ctx.state_machine.state, State.disabled)

  def test_stock_available_flicker_does_not_cancel_session(self):
    self.step(button('lfaButton'))
    self.assertEqual(self.step(events=('wrongCarMode', 'pcmDisable')), (True, False))

  def test_pedal_disables_and_requires_new_request(self):
    self.step(button('mainCruise'))
    self.assertEqual(self.step(events=('pedalPressed',)), (False, False))
    self.assertEqual(self.step(), (False, False))

  def test_soft_disable_uses_existing_state_machine(self):
    self.step(button('lfaButton'))
    self.step(events=('seatbeltNotLatched',))
    self.assertEqual(self.ctx.state_machine.state, State.softDisabling)
    for _ in range(301):
      self.step(events=('seatbeltNotLatched',))
    self.assertEqual(self.ctx.state_machine.state, State.disabled)
    self.assertEqual(self.step(), (False, False))

  def test_steer_fault_disables_immediately(self):
    self.step(button('lfaButton'))
    self.cs.steerFaultTemporary = True
    self.assertEqual(self.step(), (False, False))
    self.cs.steerFaultTemporary = False
    self.assertEqual(self.step(), (False, False))

  def test_stale_car_state_cannot_replay_release(self):
    self.step(button('lfaButton'))
    self.ctx.car_state_fresh = False
    self.assertEqual(self.step(button('mainCruise')), (False, False))
    self.ctx.car_state_fresh = True
    self.assertEqual(self.step(), (False, False))

  def test_invalid_can_disables(self):
    self.step(button('lfaButton'))
    self.cs.canValid = False
    self.assertEqual(self.step(), (False, False))

  def test_unavailable_or_mismatched_panda_denies_entry(self):
    for field, value in (('controlsAllowed', False), ('safetyRxChecksInvalid', True),
                         ('safetyParam', 0), ('safetyModel', 'noOutput'),
                         ('alternativeExperience', 1), ('faults', ['relayMalfunction'])):
      self.setUp()
      setattr(self.panda, field, value)
      self.assertEqual(self.step(button('lfaButton')), (False, False), field)
    for pandas in ([], [self.panda, self.panda]):
      self.setUp()
      self.ctx.sm['pandaStates'] = pandas
      self.assertEqual(self.step(button('mainCruise')), (False, False))

  def test_stale_panda_denies_entry(self):
    self.ctx.sm.valid_streams = False
    self.assertEqual(self.step(button('lfaButton')), (False, False))

  def test_delayed_panda_ack_waits_without_actuation(self):
    self.panda.controlsAllowed = False
    self.assertEqual(self.step(button('lfaButton')), (False, False))
    self.now = .1
    self.assertEqual(self.step(), (False, False))
    self.panda.controlsAllowed = True
    self.now = .2
    self.assertEqual(self.step(), (True, False))

  def test_late_ack_cannot_resurrect_expired_request(self):
    self.panda.controlsAllowed = False
    self.step(button('lfaButton'))
    self.now = .501
    self.panda.controlsAllowed = True
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.step(button('lfaButton')), (True, False))

  def test_cancel_while_waiting_clears_request(self):
    self.panda.controlsAllowed = False
    self.step(button('lfaButton'))
    self.step(button('cancel', True))
    self.panda.controlsAllowed = True
    self.assertEqual(self.step(), (False, False))

  def test_fault_while_waiting_clears_request(self):
    self.panda.controlsAllowed = False
    self.step(button('lfaButton'))
    self.step(events=('seatbeltNotLatched',))
    self.panda.controlsAllowed = True
    self.assertEqual(self.step(), (False, False))

  def test_unsupported_stock_long_denies_entry(self):
    self.ctx.CP.openpilotLongitudinalControl = False
    self.assertEqual(self.step(button('mainCruise')), (False, False))

  def test_output_has_no_always_lateral_fallback(self):
    for mode in Mode:
      self.assertEqual(permissions(mode, False, False, True, True, True), (False, False))
      for valid, gear, steer in ((False, True, True), (True, False, True), (True, True, False)):
        self.assertEqual(permissions(mode, True, True, valid, gear, steer), (False, False))


class Packer:
  def make_can_msg(self, name, bus, values, **kwargs):
    return name, bus, dict(values)


class TestLx3CanOwnership(unittest.TestCase):
  def setUp(self):
    self.env = dict(copy=copy, math=math, HyundaiFlags=NS(CAMERA_SCC=NS(value=1)),
                    Params=lambda: NS(get_int=lambda _: 0), _get_desire_and_lane_changing=lambda _: (0, 0))
    load_definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', self.env,
                     {'create_ccnc_messages', 'create_steering_messages_camera_scc'})
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', flags=1)
    self.can = NS(CAM=2, ECAN=0)
    self.cc = NS(enabled=False, latActive=False)

  def test_no_periodic_virtual_buttons_even_with_stock_icon_off(self):
    cs = NS(modelV2=None, lfahda_cluster={'HDA_LFA_SymSta': 0, 'HDA_CntrlModSta': 0},
            cruise_buttons_msg={'LFA_BTN': 0}, cruise_btns_msg_canfd='CRUISE_BUTTONS_ALT')
    for frame in (2, 4, 6, 8, 12, 14, 202, 204):
      for enabled in (True, False):
        self.cc.enabled = enabled
        result = self.env['create_ccnc_messages'](self.cp, Packer(), self.can, frame, self.cc, cs,
                  NS(leadVisible=False, leadDistance=0), 0, False, False, 0, False, 0)
        self.assertEqual(result, [])

  def test_other_models_keep_existing_button_behavior(self):
    self.cp.carFingerprint = 'OTHER_MODEL'
    cs = NS(modelV2=None, lfahda_cluster=None, cruise_buttons_msg={'LFA_BTN': 0},
            cruise_btns_msg_canfd='CRUISE_BUTTONS_ALT')
    result = self.env['create_ccnc_messages'](self.cp, Packer(), self.can, 2, self.cc, cs,
              NS(leadVisible=False, leadDistance=0), 0, False, False, 0, False, 0)
    self.assertEqual(result[0][2]['LFA_BTN'], 1)

  def test_other_models_keep_camera_scc_start_and_resume_requests(self):
    self.cp.carFingerprint = 'OTHER_MODEL'
    self.cc.enabled = True
    for main, mode, field, expected in ((False, 0, 'ADAPTIVE_CRUISE_MAIN_BTN', 1),
                                       (True, 0, 'CRUISE_BUTTONS', 2),
                                       (True, 4, 'CRUISE_BUTTONS', 2)):
      cs = NS(modelV2=None, lfahda_cluster=None, cruise_buttons_msg={'LFA_BTN': 0},
              cruise_btns_msg_canfd='CRUISE_BUTTONS_ALT', MainMode_ACC=main,
              ACCMode=mode, out=NS(vEgo=10))
      result = self.env['create_ccnc_messages'](self.cp, Packer(), self.can, 14, self.cc, cs,
                NS(leadVisible=False, leadDistance=0), 0, False, False, 0, False, 0)
      self.assertEqual(result[0][1], self.can.CAM)
      self.assertEqual(result[0][2][field], expected)

  def test_other_models_keep_feedback_and_torque_steering_paths(self):
    self.cp.carFingerprint = 'OTHER_MODEL'
    cs = NS(adrv_0x161=None, mdps={'STEERING_COL_TORQUE': 10},
            steer_touch_2af={'TOUCH_DETECT': 0}, lfa={'STEER_REQ': 1}, lfa_alt=None)
    for active in (False, True):
      result = self.env['create_steering_messages_camera_scc'](50, Packer(), self.cp, self.can, self.cc,
                 active, 12, cs, 0, 0, False)
      self.assertEqual([(x[0], x[1]) for x in result], [('MDPS', 2), ('STEER_TOUCH_2AF', 2), ('LFA', 0)])
      self.assertEqual(result[0][2]['LKA_ACTIVE'], 1)
      self.assertEqual(result[0][2]['STEERING_COL_TORQUE'], 10)
      self.assertEqual(result[1][2]['TOUCH_DETECT'], 0)
      self.assertEqual(result[2][2]['STEER_REQ'], int(active))
      self.assertEqual(result[2][2]['TORQUE_REQUEST'], 12)

  def steering(self, active=False, alert=0):
    self.cc.latActive = active
    cs = NS(adrv_0x161={'ALERTS_1': alert}, mdps={'STEERING_COL_TORQUE': 10},
            steer_touch_2af={'TOUCH_DETECT': 0}, lfa=None,
            lfa_alt={'COUNTER': 5, 'LKAS_ANGLE_ACTIVE': 2, 'LKAS_ANGLE_CMD': 3, 'LKAS_ANGLE_MAX_TORQUE': 40})
    before = copy.deepcopy(cs)
    result = self.env['create_steering_messages_camera_scc'](0, Packer(), self.cp, self.can, self.cc,
               active, 0, cs, 12, 60, True)
    self.assertEqual(vars(cs), vars(before))
    return result

  def test_no_fabricated_mdps_or_touch_feedback(self):
    self.assertEqual([x[0] for x in self.steering(True)], ['LFA_ALT'])

  def test_inactive_angle_has_no_op_torque_authority(self):
    value = self.steering(False)[0][2]
    self.assertEqual(value['LKAS_ANGLE_ACTIVE'], 1)
    self.assertEqual(value['LKAS_ANGLE_MAX_TORQUE'], 0)

  def test_active_angle_uses_normal_control_output(self):
    value = self.steering(True)[0][2]
    self.assertEqual((value['LKAS_ANGLE_ACTIVE'], value['LKAS_ANGLE_MAX_TORQUE']), (2, 60))

  def test_oem_emergency_branch_not_silently_removed(self):
    # This inherited handoff is a documented bench/Panda release blocker.
    for alert in (11, 12, 13, 14, 15, 21, 22, 23, 24, 25, 26):
      for active in (False, True):
        value = self.steering(active, alert=alert)[0][2]
        self.assertEqual((value['LKAS_ANGLE_ACTIVE'], value['LKAS_ANGLE_MAX_TORQUE']), (2, 40))
        self.assertEqual(value['LKAS_ANGLE_CMD'], 3)

  def test_lx3_candidate_cannot_become_an_active_port(self):
    path = ROOT / 'opendbc_repo/opendbc/car/hyundai/interface.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    guard = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                 any(isinstance(x, ast.Attribute) and x.attr == 'dashcamOnly' for x in ast.walk(n)))
    for name, expected in (('lx3', True), ('other', False)):
      ret = NS(dashcamOnly=False, safetyConfigs=[NS(safetyParam=190)])
      exec(compile(ast.Module(body=[guard], type_ignores=[]), str(path), 'exec'),
           dict(candidate=name, CAR=NS(HYUNDAI_PALISADE_LX3_HEV='lx3'), ret=ret, HyundaiSafetyFlags=SafetyFlags))
      self.assertEqual(ret.dashcamOnly, expected)
      self.assertEqual(ret.safetyConfigs[-1].safetyParam, LX3_SAFETY_PARAM if expected else 190)

  def test_legacy_panda_cannot_acknowledge_guarded_candidate(self):
    config = NS(safetyModel='hyundaiCanfd', safetyParam=LX3_SAFETY_PARAM)
    panda = NS(safetyModel='hyundaiCanfd', safetyParam=190, alternativeExperience=0,
               controlsAllowed=True, safetyRxChecksInvalid=False, faults=[])
    self.assertFalse(pandas_ready([panda], [config], 0))


class TestLx3CameraHealth(unittest.TestCase):
  def setUp(self):
    self.fault = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_state.py'))['lateral_fault']
    self.ok = dict(FAULT_LSS=0, FAULT_LFA=0, FAULT_DAS=0)

  def test_each_camera_fault_blocks_lateral(self):
    for name in self.ok:
      for value in (1, 2, 7):
        self.assertTrue(self.fault(dict(self.ok, **{name: value}), 1_000_000_000, 1_010_000_000))
    self.assertFalse(self.fault(self.ok, 1_000_000_000, 1_010_000_000))

  def test_missing_or_stale_health_is_not_healthy(self):
    for received, now in ((0, 1), (1_000_000_000, 1_250_000_001), (2, 1)):
      self.assertTrue(self.fault(self.ok, received, now))
    self.assertTrue(self.fault(None, 1, 2))
    self.assertTrue(self.fault({}, 1, 2))

  def test_invalid_checksum_cannot_refresh_healthy_camera(self):
    clock = runpy.run_path(str(ROOT / 'selfdrive/carrot/tests/test_lx3_can_time.py'))
    env = clock['ENV']
    parser = env['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    parser._add_message('CCNC_0x162')
    data = bytearray(32)
    data[:2] = env['hkg_can_fd_checksum'](0x162, None, data).to_bytes(2, 'little')
    parser.update([[1_000_000_000, [(0x162, bytes(data), 2)]]])
    values = parser.vl['CCNC_0x162']
    stamp = parser.ts_nanos['CCNC_0x162']['FAULT_LSS']
    self.assertFalse(self.fault(values, stamp, 1_000_000_000))
    data[4] ^= 1
    parser.update([[1_300_000_000, [(0x162, bytes(data), 2)]]])
    self.assertEqual(parser.ts_nanos['CCNC_0x162']['FAULT_LSS'], stamp)
    self.assertTrue(self.fault(values, stamp, parser._last_update_nanos))


if __name__ == '__main__':
  unittest.main()
