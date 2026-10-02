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


class TestLx3PedalPolicy(unittest.TestCase):
  def test_startup_setting_is_shared_with_panda(self):
    for setting, flag in ((False, 1), (True, 0)):
      self.assertEqual(MODULE['lx3_alternative_experience'](setting), flag)
      self.assertEqual(MODULE['lx3_disengage_on_gas'](flag), setting)

  def test_unrelated_safety_flags_do_not_change_gas_policy(self):
    self.assertTrue(MODULE['lx3_disengage_on_gas'](16))
    self.assertFalse(MODULE['lx3_disengage_on_gas'](17))


class TestLx3AngleReentry(unittest.TestCase):
  def test_first_active_command_keeps_margin_to_native_measured_angle(self):
    source = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
    env = {}
    load_definitions(source, env, {'limit_lx3_angle_reentry'})
    limit = env['limit_lx3_angle_reentry']
    # At low speed the normal 2 degree host tick can exceed the native
    # 21-raw-unit bound if Panda's MDPS is newer and moves 0.3 degrees away.
    for sign in (-1, 1):
      first = limit(sign * 2.0, 0.0, False)
      self.assertAlmostEqual(first, sign * 1.0)
      self.assertLessEqual(abs(round(first * 10) - round(-sign * .3 * 10)), 21)
      self.assertGreater(abs(round(sign * 2.0 * 10) - round(-sign * .3 * 10)), 21)
      self.assertEqual(limit(sign * 2.0, 0.0, True), sign * 2.0)

  def test_oem_emergency_template_does_not_become_host_active_reference(self):
    controller = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
    builder = ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
    builder_env = {}
    load_definitions(builder, builder_env, {'oem_emergency_steering'})
    env = {'hyundaicanfd': NS(oem_emergency_steering=builder_env['oem_emergency_steering'])}
    load_definitions(controller, env, {'lx3_op_owned_angle_active', 'lx3_angle_reentry_next'})
    active = (0xCB, bytes([0, 0, 0, 0x20]), 0)
    cs = NS(adrv_0x161={'ALERTS_1': 0})
    self.assertTrue(env['lx3_op_owned_angle_active']([active], cs))
    self.assertEqual(env['lx3_angle_reentry_next'](2, [active], cs), 1)
    self.assertEqual(env['lx3_angle_reentry_next'](1, [active], cs), 0)
    cs.adrv_0x161['ALERTS_1'] = 11
    self.assertFalse(env['lx3_op_owned_angle_active']([active], cs))
    self.assertEqual(env['lx3_angle_reentry_next'](0, [active], cs), 2)
    cs.adrv_0x161['ALERTS_1'] = 0
    self.assertFalse(env['lx3_op_owned_angle_active']([], cs))


def load_definitions(path, env, names):
  nodes = [n for n in ast.walk(ast.parse(path.read_text(encoding='utf-8')))
           if isinstance(n, (ast.FunctionDef, ast.ClassDef)) and n.name in names]
  exec(compile(ast.Module(body=nodes, type_ignores=[]), str(path), 'exec'), env)


def load_car_state_receipt(env):
  # Run the production data_sample receipt prelude; the remainder of the
  # method needs the full camera/model/IPC stack and is unrelated here.
  path = ROOT / 'selfdrive/selfdrived/selfdrived.py'
  tree = ast.parse(path.read_text(encoding='utf-8'))
  owner = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == 'SelfdriveD')
  method = next(node for node in owner.body if isinstance(node, ast.FunctionDef) and node.name == 'data_sample')
  prelude = []
  for node in method.body:
    prelude.append(node)
    if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'CS'
                                            for target in node.targets):
      break
  assert isinstance(prelude[-1], ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'CS'
                                                      for target in prelude[-1].targets)
  receipt = ast.FunctionDef(name='receipt', args=method.args, body=prelude + [ast.Return(value=ast.Name(id='CS', ctx=ast.Load()))],
                            decorator_list=[], returns=None, type_comment=None)
  module = ast.fix_missing_locations(ast.Module(body=[receipt], type_ignores=[]))
  exec(compile(module, str(path), 'exec'), env)
  return env['receipt']


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
           time=NS(monotonic=lambda: 0.0, monotonic_ns=lambda: 0), cloudlog=NS(event=lambda *a, **kw: None),
           lx3_pandas_ready=pandas_ready, lx3_permission_sample=MODULE['lx3_permission_sample'],
           lx3_permission_matches=MODULE['lx3_permission_matches'], lx3_input_ready=MODULE['lx3_input_ready'],
           car=NS(CarParams=NS(SteerControlType=NS(angle='angle'))))
load_definitions(ROOT / 'selfdrive/selfdrived/state.py', ENV, {'StateMachine'})
load_definitions(ROOT / 'selfdrive/selfdrived/selfdrived.py', ENV, {'update_lx3_state'})


def button(name, pressed=False, counter=40, valid=True):
  return NS(type=name, pressed=pressed, lx3PhysicalCounter=counter, lx3PhysicalValid=valid)


class SubMaster(dict):
  valid_streams = True

  def all_checks(self, _):
    return self.valid_streams


class TestLx3Session(unittest.TestCase):
  """Production host adapter frames, with an explicit companion message facade.

  Acceptance publication below is a transport fixture, not the Panda algorithm.
  Actual C/host joint scheduling is verified separately.
  """
  def setUp(self):
    self.now = 1.0
    ENV['time'] = NS(monotonic=lambda: self.now, monotonic_ns=lambda: int(self.now * 1e9))
    self.panda = NS(safetyModel='hyundaiCanfd', safetyParam=LX3_SAFETY_PARAM, alternativeExperience=0,
                    controlsAllowed=False, safetyRxChecksInvalid=False, faults=[],
                    lx3PermissionVersion=2, lx3TransportEpoch=0x123456789ABCDEF0,
                    lx3RequestedMode=0, lx3AcceptedMode=0, lx3PhysicalCounter=0,
                    lx3RequestGeneration=1, lx3RequestAgeMs=0, lx3ControlsAllowed=False, lx3PermissionPhase=0)
    self.ctx = NS(lx3_engagement=Lx3Engagement(), car_state_fresh=True,
                  CP=NS(openpilotLongitudinalControl=True, steerControlType='angle', alternativeExperience=0,
                        safetyConfigs=[NS(safetyModel='hyundaiCanfd', safetyParam=LX3_SAFETY_PARAM)]),
                  sm=SubMaster(pandaStates=[self.panda]), state_machine=ENV['StateMachine'](), enabled=False, active=False)
    self.ctx.sm.logMonoTime = {'pandaStates': 1_000_000_000}
    self.cs = NS(canValid=True, steerFaultTemporary=False, steerFaultPermanent=False, buttonEvents=[],
                 lx3InputState='ready', lx3PhysicalCounterValid=True, lx3PhysicalCounter=0, lx3InputResetCount=1)
    self.refresh_panda = True

  def test_actual_receipt_path_holds_only_missing_short_samples(self):
    queue = [NS(valid=True, logMonoTime=1_000_000_000, carState=self.cs), None, None,
             NS(valid=True, logMonoTime=1_045_000_000, carState=self.cs)]
    now = [1_000_000_000]
    receipt = load_car_state_receipt(dict(messaging=NS(recv_one=lambda _: queue.pop(0)),
                                         time=NS(monotonic_ns=lambda: now[0])))
    self.ctx.car_state_sock = object()
    self.ctx.CS_prev = self.cs
    self.ctx.car_state_last_valid_ns = 0
    for sample_ns, fresh, missing, last_valid in ((1_000_000_000, True, False, 1_000_000_000),
                                                  (1_020_000_000, False, True, 1_000_000_000),
                                                  (1_040_000_000, False, True, 1_000_000_000),
                                                  (1_045_000_000, True, False, 1_045_000_000)):
      now[0] = sample_ns
      self.assertIs(receipt(self.ctx), self.cs)
      self.assertEqual((self.ctx.car_state_fresh, self.ctx.car_state_missing,
                        self.ctx.car_state_last_valid_ns), (fresh, missing, last_valid))
    # Preserve the existing valid-message behavior; a delayed queued message
    # must still not restart the new missing-sample gap allowance.
    stale = NS(valid=True, logMonoTime=1_000_000_000, carState=self.cs)
    receipt_stale = load_car_state_receipt(dict(messaging=NS(recv_one=lambda _: stale),
                                               time=NS(monotonic_ns=lambda: 1_200_000_000)))
    receipt_stale(self.ctx)
    self.assertTrue(self.ctx.car_state_fresh)
    self.assertFalse(self.ctx.car_state_missing)
    self.assertEqual(self.ctx.car_state_last_valid_ns, 1_000_000_000)

  def step(self, *buttons, events=()):
    self.ctx.events = Events(*events)
    self.cs.buttonEvents = list(buttons)
    if self.refresh_panda:
      self.ctx.sm.logMonoTime['pandaStates'] = int(self.now * 1e9)
    with contextlib.redirect_stdout(io.StringIO()):
      ENV['update_lx3_state'](self.ctx, self.cs)
    return permissions(self.ctx.lx3_engagement.mode, self.ctx.enabled, self.ctx.active, True, True, True)

  def pending(self, mode=Mode.LATERAL, counter=40):
    self.panda.lx3RequestGeneration = self.panda.lx3RequestGeneration % 65535 + 1
    self.panda.lx3PermissionPhase = 1
    self.panda.lx3RequestedMode = int(mode)
    self.panda.lx3AcceptedMode = 0
    self.panda.lx3PhysicalCounter = counter
    self.panda.lx3ControlsAllowed = self.panda.controlsAllowed = False
    self.panda.lx3RequestAgeMs = 0

  def accept(self):
    self.panda.lx3PermissionPhase = 2
    self.panda.lx3AcceptedMode = self.panda.lx3RequestedMode
    self.panda.lx3ControlsAllowed = self.panda.controlsAllowed = True

  def engage(self, name='lfaButton', mode=None, events=(), counter=40):
    mode = mode or (Mode.LATERAL if name == 'lfaButton' else Mode.COMBINED)
    self.pending(mode, counter)
    first = self.step(button(name, counter=counter), events=events)
    self.assertEqual(first, (False, False))
    intent = self.ctx.lx3_engagement
    if intent.pending is not None and not self.ctx.enabled and not events:
      # Mode upgrades publish one normal disable frame before fresh PRE_ENABLE.
      self.assertIn(ET.USER_DISABLE, self.ctx.state_machine.current_alert_types)
      self.step()
    if intent.ack_valid and intent.ack_mode == mode:
      self.assertTrue(self.ctx.enabled)
      self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
      self.assertEqual(intent.ack_generation, self.panda.lx3RequestGeneration)
      self.assertEqual(intent.ack_counter, counter)
      self.accept()
      return self.step(events=events)
    return first

  def test_lfa_uses_normal_pre_enabled_then_enabled_without_acc(self):
    self.assertEqual(self.engage(), (True, False))
    self.assertEqual(self.ctx.state_machine.state, State.enabled)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, 0)

  def test_input_health_blocks_entry_independently_of_healthy_eps(self):
    for state, valid in (('notApplicable', False), ('warmingUp', False), ('requalifying', False),
                         ('integrityFault', False), ('ready', False)):
      with self.subTest(state=state, valid=valid):
        self.setUp()
        self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = state, valid
        self.assertEqual(self.engage(), (False, False))
        self.assertFalse(self.ctx.enabled)
        self.assertIn('lx3InputFault' if state == 'integrityFault' else 'lx3InputNotReady', self.ctx.events.events)
        self.assertNotIn('steerUnavailable', self.ctx.events.events)
        self.assertFalse(self.ctx.lx3_engagement.ack_valid and self.ctx.lx3_engagement.ack_mode != Mode.OFF)

  def delayed_main_off(self, phase=1, press=34, anchor=36, consumed=40, target=44, generation_delta=2,
                       capture_events=(), with_press=True, request_age=0):
    self.engage('mainCruise', counter=6)
    old = self.ctx.lx3_engagement.accepted_generation
    self.cs.lx3PhysicalCounter = press
    if with_press:
      self.step(button('mainCruise', True, counter=press))
    self.cs.lx3PhysicalCounter = consumed
    self.panda.lx3RequestGeneration = old + generation_delta
    self.panda.lx3PhysicalCounter = target if phase == 1 else anchor
    self.panda.lx3PermissionPhase = phase
    self.panda.lx3RequestedMode = 2 if phase == 1 else 0
    self.panda.lx3AcceptedMode = 0
    self.panda.lx3ControlsAllowed = self.panda.controlsAllowed = False
    self.panda.lx3RequestAgeMs = request_age
    self.now += .1
    self.step(events=capture_events)
    return old

  @staticmethod
  def delayed_edges(anchor=36, press=42, release=44):
    return (button('mainCruise', counter=anchor), button('accelCruise', True, counter=press),
            button('accelCruise', counter=release))

  def test_delayed_off_then_short_res_requires_new_pre_enable_and_ack(self):
    old = self.delayed_main_off()
    self.assertIsNotNone(self.ctx.lx3_engagement.replay_base)
    self.assertFalse(self.ctx.enabled)
    self.cs.lx3PhysicalCounter = 44
    self.now += .04
    self.assertEqual(self.step(*self.delayed_edges()), (False, False))
    intent = self.ctx.lx3_engagement
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual((intent.ack_generation, intent.ack_counter, intent.ack_mode), (old + 2, 44, Mode.COMBINED))
    self.assertEqual((intent.mode, intent.accepted_generation), (Mode.OFF, 0))
    self.accept()
    self.assertEqual(self.step(), (True, True))
    self.assertEqual(intent.accepted_generation, old + 2)

  def test_delayed_off_companion_can_precede_the_following_pending(self):
    old = self.delayed_main_off(phase=0, generation_delta=1)
    self.assertIsNotNone(self.ctx.lx3_engagement.replay_base)
    self.pending(Mode.COMBINED, 44)
    self.assertEqual(self.panda.lx3RequestGeneration, old + 2)
    self.cs.lx3PhysicalCounter = 44
    self.step(*self.delayed_edges())
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual(self.ctx.lx3_engagement.ack_counter, 44)

  def test_delayed_off_target_consumes_only_real_off_then_waits_for_res_companion(self):
    self.delayed_main_off(phase=0, generation_delta=1)
    self.cs.lx3PhysicalCounter = 44
    self.step(*self.delayed_edges())
    intent = self.ctx.lx3_engagement
    self.assertFalse(self.ctx.enabled)
    self.assertFalse(intent.ack_valid)
    self.assertEqual((intent.pending, intent.pending_counter), (Mode.COMBINED, 44))
    self.pending(Mode.COMBINED, 44)
    self.step()
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual(intent.ack_mode, Mode.COMBINED)

  def test_same_batch_off_then_res_disables_an_existing_active_session_first(self):
    self.engage('mainCruise', counter=6)
    self.pending(Mode.COMBINED, 44)
    self.step(*self.delayed_edges())
    intent = self.ctx.lx3_engagement
    self.assertEqual(self.ctx.state_machine.state, State.disabled)
    self.assertFalse(self.ctx.active)
    self.assertFalse(intent.ack_valid)
    self.assertEqual(intent.pending_counter, 44)
    self.step()
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual(intent.ack_counter, 44)

  def test_delayed_batch_emission_order_handles_counter_wrap(self):
    self.delayed_main_off(press=246, anchor=248, consumed=252, target=0)
    self.cs.lx3PhysicalCounter = 0
    self.step(*self.delayed_edges(anchor=248, press=254, release=0))
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual(self.ctx.lx3_engagement.ack_counter, 0)

  def test_partial_delayed_batches_do_not_replay_a_release_twice(self):
    self.delayed_main_off()
    self.cs.lx3PhysicalCounter = 42
    self.step(*self.delayed_edges()[:2])
    self.assertFalse(self.ctx.enabled)
    self.assertIsNotNone(self.ctx.lx3_engagement.replay_base)
    self.step()
    self.cs.lx3PhysicalCounter = 44
    self.step(button('accelCruise', counter=44))
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertEqual(self.ctx.lx3_engagement.ack_counter, 44)

  def test_base_cannot_supply_permission_when_target_can_never_arrives(self):
    self.delayed_main_off()
    for delay in (.1, .499, .5, .6):
      self.now = 1.1 + delay
      self.step()
      self.assertFalse(self.ctx.enabled)
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertIsNone(self.ctx.lx3_engagement.replay_base)

  def test_hidden_input_reset_or_cancel_clears_delayed_base(self):
    for modification in ('reset', 'cancel', 'not_ready', 'stale_cs', 'epoch', 'target_generation', 'expired'):
      with self.subTest(modification=modification):
        self.setUp()
        self.delayed_main_off()
        self.assertIsNotNone(self.ctx.lx3_engagement.replay_base)
        events = self.delayed_edges()
        self.cs.lx3PhysicalCounter = 44
        if modification == 'reset': self.cs.lx3InputResetCount += 1
        if modification == 'cancel': events += (button('cancel', True, counter=44),)
        if modification == 'not_ready': self.cs.lx3InputState = 'integrityFault'
        if modification == 'stale_cs': self.ctx.car_state_fresh = False
        if modification == 'epoch': self.panda.lx3TransportEpoch += 1
        if modification == 'target_generation': self.panda.lx3RequestGeneration += 5
        if modification == 'expired': self.now += .5
        self.step(*events)
        self.assertIsNone(self.ctx.lx3_engagement.replay_base)
        self.assertFalse(self.ctx.enabled)
        self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_entry_barriers_prevent_capture_and_clear_a_waiting_base(self):
    for event in ('doorOpen', 'wrongGear', 'pedalPressed', 'steerUnavailable', 'commIssue', 'tooDistracted'):
      for stage in ('capture', 'application'):
        with self.subTest(event=event, stage=stage):
          self.setUp()
          self.delayed_main_off(capture_events=(event,) if stage == 'capture' else ())
          self.cs.lx3PhysicalCounter = 44
          self.step(*self.delayed_edges(), events=(event,) if stage == 'application' else ())
          self.assertIsNone(self.ctx.lx3_engagement.replay_base)
          self.assertFalse(self.ctx.enabled)
          self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_unproven_or_out_of_bounds_delayed_base_is_never_captured(self):
    for change in ('old_producer', 'odd_counter', 'far_counter',
                   'far_generation', 'generation_wrap', 'panda_deadline'):
      with self.subTest(change=change):
        self.setUp()
        if change == 'old_producer': self.cs.lx3InputResetCount = 0
        kwargs = {}
        if change == 'odd_counter': kwargs['target'] = 45
        if change == 'far_counter': kwargs['target'] = 68
        if change == 'far_generation': kwargs['generation_delta'] = 5
        if change == 'generation_wrap': kwargs['generation_delta'] = -1
        if change == 'panda_deadline': kwargs['request_age'] = 500
        self.delayed_main_off(**kwargs)
        self.assertIsNone(self.ctx.lx3_engagement.replay_base)

  def test_replay_derived_candidate_still_needs_matching_native_counter_and_mode(self):
    for target, mode in ((40, 2), (44, 1)):
      self.setUp()
      self.delayed_main_off(target=target)
      self.panda.lx3RequestedMode = mode
      self.cs.lx3PhysicalCounter = 44
      self.step(*self.delayed_edges())
      self.assertFalse(self.ctx.enabled)
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_delayed_main_release_needs_held_press_provenance(self):
    for change in ('anchor', 'missing_press', 'long_press'):
      with self.subTest(change=change):
        self.setUp()
        self.delayed_main_off(with_press=change != 'missing_press', press=0 if change == 'long_press' else 34)
        self.cs.lx3PhysicalCounter = 44
        self.step(*self.delayed_edges(anchor=4 if change == 'anchor' else 36))
        self.assertFalse(self.ctx.enabled)
        self.assertFalse(self.ctx.lx3_engagement.ack_valid)
        self.assertIsNone(self.ctx.lx3_engagement.replay_base)

  def test_old_carstate_producer_cannot_enable(self):
    del self.cs.lx3InputState
    del self.cs.lx3PhysicalCounterValid
    self.assertEqual(self.engage(), (False, False))
    self.assertIn('lx3InputNotReady', self.ctx.events.events)

  def test_physical_cancel_uses_normal_cancel_alert_during_requalification(self):
    self.assertEqual(self.engage(), (True, False))
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'requalifying', False
    self.assertEqual(self.step(button('cancel', pressed=True)), (False, False))
    self.assertEqual(self.ctx.state_machine.current_alert_types, [ET.PERMANENT, ET.USER_DISABLE])
    self.assertIn('buttonCancel', self.ctx.events.events)
    self.assertNotIn('steerUnavailable', self.ctx.events.events)

  def test_input_loss_disables_active_and_recovery_does_not_reengage(self):
    self.assertEqual(self.engage(), (True, False))
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'integrityFault', False
    self.assertEqual(self.step(), (False, False))
    self.assertFalse(self.ctx.enabled)
    self.assertIn(ET.IMMEDIATE_DISABLE, self.ctx.state_machine.current_alert_types)
    self.assertNotIn('steerUnavailable', self.ctx.events.events)
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'ready', True
    self.assertEqual(self.step(), (False, False))
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_input_loss_revokes_pending_ack_and_cannot_renew_on_recovery(self):
    self.pending()
    self.step(button('lfaButton'))
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'requalifying', False
    self.step()
    self.assertFalse(self.ctx.enabled)
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)
    self.cs.lx3InputState, self.cs.lx3PhysicalCounterValid = 'ready', True
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)

  def test_genuine_eps_fault_retains_existing_alert_and_denies_entry(self):
    self.cs.steerFaultTemporary = True
    self.assertEqual(self.engage(), (False, False))
    self.assertIn('steerUnavailable', self.ctx.events.events)
    self.assertNotIn('lx3InputFault', self.ctx.events.events)

  def test_idle_without_permission_is_not_mismatch(self):
    self.assertEqual(self.step(), (False, False))
    self.assertNotIn('controlsMismatch', self.ctx.events.events)

  def test_wrong_firmware_is_reported_on_request(self):
    self.panda.safetyParam = 190
    self.assertEqual(self.engage(), (False, False))
    self.assertIn('controlsMismatch', self.ctx.events.events)

  def test_legacy_default_companion_cannot_grant(self):
    self.panda.controlsAllowed = True
    self.panda.lx3PermissionVersion = 0
    self.assertEqual(self.engage(), (False, False))
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_permission_revoked_in_active_session_is_reported(self):
    self.engage()
    self.panda.lx3ControlsAllowed = False
    self.assertEqual(self.step(), (False, False))
    self.assertIn('controlsMismatch', self.ctx.events.events)

  def test_companion_is_authority_not_independently_sampled_health(self):
    self.engage()
    self.panda.controlsAllowed = False
    self.assertEqual(self.step(), (True, False))

  def test_scc_enables_both_after_its_ack(self):
    self.assertEqual(self.engage('mainCruise'), (True, True))

  def test_scc_upgrade_suspends_then_checks_new_combined_ack(self):
    self.engage()
    self.assertEqual(self.engage('mainCruise'), (True, True))

  def test_upgrade_has_one_normal_disable_transition_and_alert(self):
    self.engage()
    machine = self.ctx.state_machine
    original = machine.update
    calls = []
    def update(events):
      calls.append(list(events.events))
      return original(events)
    machine.update = update
    self.pending(Mode.COMBINED, 42)
    self.assertEqual(self.step(button('mainCruise', counter=42)), (False, False))
    self.assertEqual(len(calls), 1)
    self.assertEqual(machine.state, State.disabled)
    self.assertIn(ET.USER_DISABLE, machine.current_alert_types)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(machine.state, State.preEnabled)

  def test_matching_recent_physical_off_clears_unbound_request(self):
    self.engage(events=('seatbeltNotLatched',))
    self.panda.lx3RequestGeneration += 1
    self.panda.lx3PhysicalCounter = 42
    self.panda.lx3PermissionPhase = 0
    self.panda.lx3RequestedMode = self.panda.lx3AcceptedMode = 0
    self.assertEqual(self.step(button('lfaButton', counter=42)), (False, False))
    self.assertIsNone(self.ctx.lx3_engagement.pending)

  def test_res_set_in_combined_does_not_restart_transaction(self):
    self.engage('mainCruise')
    generation = self.ctx.lx3_engagement.accepted_generation
    for name in ('accelCruise', 'decelCruise'):
      self.assertEqual(self.step(button(name)), (True, True))
      self.assertEqual(self.ctx.lx3_engagement.accepted_generation, generation)
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_main_and_lfa_off_disable(self):
    for start, stop in (('mainCruise', 'mainCruise'), ('mainCruise', 'lfaButton'), ('lfaButton', 'lfaButton')):
      self.setUp()
      self.engage(start)
      self.assertEqual(self.step(button(stop)), (False, False))

  def test_cancel_dominates_all_enables_even_without_metadata(self):
    self.pending(Mode.COMBINED)
    self.assertEqual(self.step(button('lfaButton'), button('cancel', True, valid=False), button('mainCruise')), (False, False))
    self.assertIsNone(self.ctx.lx3_engagement.pending)

  def test_press_and_hold_and_old_metadata_cannot_request(self):
    for b in (button('lfaButton', True), button('lfaButton', valid=False), NS(type='mainCruise', pressed=False)):
      self.assertEqual(self.step(b), (False, False))
      self.assertIsNone(self.ctx.lx3_engagement.pending)

  def test_no_entry_is_rejected_and_not_latched(self):
    self.assertEqual(self.engage(events=('seatbeltNotLatched',)), (False, False))
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.engage(counter=42), (True, False))

  def test_denial_before_companion_rejects_late_pending_without_enable(self):
    self.assertEqual(self.step(button('lfaButton'), events=('tooDistracted',)), (False, False))
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertIsNone(self.ctx.lx3_engagement.pending)
    self.now += .1
    self.pending()
    self.assertEqual(self.step(), (False, False))
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, self.panda.lx3RequestGeneration)
    self.assertFalse(self.ctx.enabled)

  def test_duplicate_denied_gesture_cannot_overwrite_off_ack(self):
    self.step(button('lfaButton'), events=('tooDistracted',))
    self.now += .1
    self.pending()
    self.assertEqual(self.step(button('lfaButton')), (False, False))
    self.assertIsNone(self.ctx.lx3_engagement.pending)
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)
    self.accept()
    self.assertEqual(self.step(), (False, False))

  def test_unbound_rejection_does_not_match_other_or_expired_physical_request(self):
    for delay, counter in ((.1, 42), (.501, 40)):
      self.setUp()
      self.step(button('lfaButton'), events=('tooDistracted',))
      self.now += delay
      self.pending(counter=counter)
      self.step()
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)
      self.assertFalse(self.ctx.enabled)

  def test_unbound_refusal_respects_panda_499_500ms_boundary(self):
    for age, expected in ((499, True), (500, False)):
      self.setUp()
      self.step(button('lfaButton'), events=('tooDistracted',))
      self.now += .1
      self.pending()
      self.panda.lx3RequestAgeMs = age
      self.step()
      self.assertEqual(self.ctx.lx3_engagement.ack_valid, expected)
      self.assertFalse(self.ctx.enabled)
      self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)

  def test_generation_wrap_cannot_accept_bound_old_request(self):
    self.panda.lx3RequestGeneration = 65534
    self.pending()
    self.step(button('lfaButton'))
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, 65535)
    self.accept()
    self.panda.lx3RequestGeneration = 1
    self.assertEqual(self.step(), (False, False))
    self.assertIn('controlsMismatch', self.ctx.events.events)
    self.assertFalse(self.ctx.enabled)

  def test_old_refusal_is_displaced_by_new_generation_after_wrap(self):
    self.panda.lx3RequestGeneration = 65534
    self.engage(events=('tooDistracted',))
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, 65535)
    self.pending(counter=42)
    self.step(button('lfaButton', counter=42))
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, 1)
    self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.LATERAL)
    self.accept()
    self.assertEqual(self.step(), (True, False))

  def test_upgrade_cannot_bypass_no_entry(self):
    self.engage()
    self.assertEqual(self.engage('mainCruise', events=('seatbeltNotLatched',)), (False, False))

  def test_pre_enabled_rechecks_no_entry_and_soft_disable(self):
    for event in ('seatbeltNotLatched', 'commIssue', 'tooDistracted'):
      self.setUp()
      self.pending()
      self.step(button('lfaButton'))
      self.assertTrue(self.ctx.enabled)
      self.assertFalse(self.ctx.active)
      self.assertEqual(self.step(events=(event,)), (False, False))
      self.assertFalse(self.ctx.enabled)
      self.assertEqual(self.ctx.lx3_engagement.ack_mode, Mode.OFF)
      self.accept()  # Old ACK might already be in transit; no resurrection.
      self.assertEqual(self.step(), (False, False))

  def test_other_pre_enable_condition_outlives_completed_panda_transaction(self):
    self.pending()
    self.step(button('lfaButton'), events=('preEnableStandstill',))
    self.accept()
    self.now += .1
    self.assertEqual(self.step(events=('preEnableStandstill',)), (False, False))
    self.assertTrue(self.ctx.enabled)
    self.assertFalse(self.ctx.active)
    self.assertIsNone(self.ctx.lx3_engagement.pending)
    self.assertEqual(self.ctx.lx3_engagement.mode, Mode.LATERAL)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.now += 1
    self.assertEqual(self.step(events=('preEnableStandstill',)), (False, False))
    self.assertEqual(self.step(), (True, False))

  def test_accepted_but_still_pre_enabled_also_rechecks_no_entry(self):
    self.pending()
    self.step(button('lfaButton'), events=('preEnableStandstill',))
    self.accept()
    self.step(events=('preEnableStandstill',))
    self.assertIsNone(self.ctx.lx3_engagement.pending)
    self.assertEqual(self.step(events=('tooDistracted',)), (False, False))
    self.assertFalse(self.ctx.enabled)

  def test_automatic_stock_enable_and_enabled_without_intent_are_reset(self):
    self.assertEqual(self.step(events=('buttonEnable', 'pcmEnable')), (False, False))
    self.ctx.enabled = self.ctx.active = True
    self.ctx.state_machine.state = State.enabled
    self.assertEqual(self.step(), (False, False))

  def test_stock_available_flicker_does_not_cancel(self):
    self.engage()
    self.assertEqual(self.step(events=('wrongCarMode', 'pcmDisable')), (True, False))

  def test_pedal_disables_and_requires_new_physical_request(self):
    self.engage('mainCruise')
    self.assertEqual(self.step(events=('pedalPressed',)), (False, False))
    self.assertEqual(self.step(), (False, False))

  def test_active_soft_disable_preserves_existing_state_machine(self):
    self.engage()
    self.step(events=('seatbeltNotLatched',))
    self.assertEqual(self.ctx.state_machine.state, State.softDisabling)
    for _ in range(301): self.step(events=('seatbeltNotLatched',))
    self.assertEqual(self.ctx.state_machine.state, State.disabled)
    self.assertEqual(self.step(), (False, False))

  def test_steer_fault_and_stale_invalid_car_state_disable(self):
    for field, value in (('steerFaultTemporary', True), ('steerFaultPermanent', True), ('canValid', False)):
      self.setUp()
      self.engage()
      setattr(self.cs, field, value)
      self.assertEqual(self.step(), (False, False))
    self.setUp()
    self.engage()
    self.ctx.car_state_fresh = False
    self.assertEqual(self.step(button('mainCruise')), (False, False))
    self.ctx.car_state_fresh = True
    self.assertEqual(self.step(), (False, False))

  def test_short_missing_carstate_during_pending_waits_without_ack_or_authority(self):
    self.pending(Mode.COMBINED)
    self.step(button('mainCruise'))
    self.assertTrue(self.ctx.enabled)
    self.assertFalse(self.ctx.active)
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.now += .025  # The route's ordinary carState publication gaps reached 25-30 ms.
    self.ctx.car_state_fresh = False
    self.ctx.car_state_missing = True
    self.ctx.car_state_last_valid_ns = int((self.now - .025) * 1e9)
    self.assertEqual(self.step(), (False, False))
    self.assertNotIn('controlsMismatch', self.ctx.events.events)
    self.assertIsNotNone(self.ctx.lx3_engagement.pending)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertFalse(self.ctx.active)
    self.ctx.car_state_fresh = True
    self.ctx.car_state_missing = False
    self.now += .005
    self.assertEqual(self.step(), (False, False))
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.accept()
    self.assertEqual(self.step(), (True, True))

  def test_accepted_session_survives_bounded_missing_publication_without_replaying_buttons(self):
    # Oct 3 route 183: all three mismatches adjoined 22.45-25.84 ms
    # publication gaps while Panda still reported the accepted identity.
    # The logger does not record recv_one(None); explicitly inject that
    # possible socket outcome here, rather than claim an exact IPC replay.
    for name, expected in (('lfaButton', (True, False)), ('mainCruise', (True, True))):
      for gap_ns in (22_451_738, 22_638_937, 25_839_783, 50_000_000):
        with self.subTest(button=name, gap_ns=gap_ns):
          self.setUp()
          self.engage(name)
          identity = (self.ctx.lx3_engagement.mode, self.ctx.lx3_engagement.accepted_generation,
                      self.ctx.lx3_engagement.accepted_counter, self.ctx.lx3_engagement.accepted_epoch)
          stamp = int(self.now * 1e9)
          self.ctx.car_state_fresh = False
          self.ctx.car_state_missing = True
          self.ctx.car_state_last_valid_ns = stamp
          self.now = (stamp + gap_ns) / 1e9
          self.assertEqual(self.step(button('mainCruise')), expected)  # cached release must be ignored
          self.assertNotIn('controlsMismatch', self.ctx.events.events)
          self.assertFalse(self.ctx.lx3_engagement.ack_valid)
          self.assertIsNone(self.ctx.lx3_engagement.pending)
          self.assertEqual((self.ctx.lx3_engagement.mode, self.ctx.lx3_engagement.accepted_generation,
                            self.ctx.lx3_engagement.accepted_counter, self.ctx.lx3_engagement.accepted_epoch), identity)

  def test_accepted_gap_expires_from_publication_without_renewal(self):
    self.engage()
    self.ctx.car_state_fresh = False
    self.ctx.car_state_missing = True
    self.ctx.car_state_last_valid_ns = int(self.now * 1e9)
    base = self.now
    for gap in (.02, .04, .05):
      self.now = base + gap
      self.assertEqual(self.step(), (True, False))
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.now = base + .050001
    self.assertEqual(self.step(), (False, False))
    self.assertIn('controlsMismatch', self.ctx.events.events)
    self.ctx.car_state_fresh = True
    self.ctx.car_state_missing = False
    self.assertEqual(self.step(), (False, False))  # recovery never re-engages by itself

  def test_accepted_short_gap_still_rejects_invalid_input_and_permission_loss(self):
    for fault in ('invalid_received', 'zero_stamp', 'future_stamp', 'panda_stale', 'panda_rx',
                  'panda_stream', 'controls', 'epoch', 'generation', 'counter', 'mode',
                  'can_invalid', 'input_fault', 'steer_fault', 'pedal', 'distracted'):
      with self.subTest(fault=fault):
        self.setUp()
        self.engage()
        self.ctx.car_state_fresh = False
        self.ctx.car_state_missing = True
        self.ctx.car_state_last_valid_ns = int(self.now * 1e9)
        self.now += .025
        if fault == 'invalid_received': self.ctx.car_state_missing = False
        if fault == 'zero_stamp': self.ctx.car_state_last_valid_ns = 0
        if fault == 'future_stamp': self.ctx.car_state_last_valid_ns = int((self.now + .001) * 1e9)
        if fault == 'panda_stale':
          self.refresh_panda = False
          self.ctx.sm.logMonoTime['pandaStates'] = int((self.now - .251) * 1e9)
        if fault == 'panda_rx': self.panda.safetyRxChecksInvalid = True
        if fault == 'panda_stream': self.ctx.sm.valid_streams = False
        if fault == 'controls': self.panda.controlsAllowed = self.panda.lx3ControlsAllowed = False
        if fault == 'epoch': self.panda.lx3TransportEpoch += 1
        if fault == 'generation': self.panda.lx3RequestGeneration += 1
        if fault == 'counter': self.panda.lx3PhysicalCounter += 2
        if fault == 'mode': self.panda.lx3AcceptedMode = self.panda.lx3RequestedMode = 2
        if fault == 'can_invalid': self.cs.canValid = False
        if fault == 'input_fault': self.cs.lx3InputState = 'integrityFault'
        if fault == 'steer_fault': self.cs.steerFaultTemporary = True
        events = ('pedalPressed',) if fault == 'pedal' else ('tooDistracted',) if fault == 'distracted' else ()
        self.assertEqual(self.step(events=events), (False, False))
        self.assertFalse(self.ctx.enabled)
        self.assertFalse(self.ctx.lx3_engagement.ack_valid and self.ctx.lx3_engagement.ack_mode != Mode.OFF)
        self.assertIsNone(self.ctx.lx3_engagement.replay_base)

  def test_short_gap_does_not_start_an_idle_session_from_cached_button(self):
    self.ctx.car_state_fresh = False
    self.ctx.car_state_missing = True
    self.ctx.car_state_last_valid_ns = int((self.now - .025) * 1e9)
    self.pending()
    self.assertEqual(self.step(button('lfaButton')), (False, False))
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertIsNone(self.ctx.lx3_engagement.pending)

  def test_actual_receipt_missing_then_recovery_preserves_only_existing_session(self):
    self.engage()
    stamp = int(self.now * 1e9)
    queue = [NS(valid=True, logMonoTime=stamp, carState=self.cs), None,
             NS(valid=True, logMonoTime=stamp + 30_000_000, carState=self.cs),
             NS(valid=False, logMonoTime=stamp + 40_000_000, carState=self.cs)]
    receipt = load_car_state_receipt(dict(messaging=NS(recv_one=lambda _: queue.pop(0))))
    self.ctx.car_state_sock = object()
    self.ctx.CS_prev = self.cs
    for age, expected in ((0, (True, False)), (.025, (True, False)),
                           (.030, (True, False)), (.040, (False, False))):
      self.now = stamp / 1e9 + age
      self.assertIs(receipt(self.ctx), self.cs)
      self.assertEqual(self.step(), expected)
    self.assertIn('controlsMismatch', self.ctx.events.events)

  def test_missing_sample_never_promotes_accepted_pre_enable(self):
    self.pending(Mode.COMBINED)
    self.step(button('mainCruise'), events=('preEnableStandstill',))
    self.accept()
    self.step(events=('preEnableStandstill',))
    self.assertTrue(self.ctx.enabled)
    self.assertFalse(self.ctx.active)
    self.ctx.car_state_fresh = False
    self.ctx.car_state_missing = True
    self.ctx.car_state_last_valid_ns = int(self.now * 1e9)
    self.now += .025
    self.assertEqual(self.step(), (False, False))
    self.assertFalse(self.ctx.enabled)

  def test_pending_acceptance_waits_through_two_missing_samples_then_commits_once(self):
    self.pending(Mode.COMBINED)
    self.step(button('mainCruise'))
    self.accept()
    self.ctx.car_state_fresh = False
    self.ctx.car_state_missing = True
    self.ctx.car_state_last_valid_ns = int(self.now * 1e9)
    for gap in (.02, .049):
      self.now = 1.0 + gap
      self.assertEqual(self.step(), (False, False))
      self.assertFalse(self.ctx.lx3_engagement.ack_valid)
      self.assertIsNotNone(self.ctx.lx3_engagement.pending)
    self.ctx.car_state_fresh = True
    self.ctx.car_state_missing = False
    self.now = 1.05
    self.assertEqual(self.step(), (True, True))
    self.assertIsNone(self.ctx.lx3_engagement.pending)
    accepted_generation = self.ctx.lx3_engagement.accepted_generation
    self.now = 1.06
    self.assertEqual(self.step(), (True, True))
    self.assertEqual(self.ctx.lx3_engagement.accepted_generation, accepted_generation)

  def test_missing_carstate_never_masks_prolonged_or_invalid_input(self):
    for missing, gap, panda_fault, entry_barrier, changed_generation in (
      (True, .051, False, False, False), (False, .025, False, False, False),
      (True, .025, True, False, False), (True, .025, False, True, False),
      (True, .025, False, False, True)):
      with self.subTest(missing=missing, gap=gap, panda_fault=panda_fault,
                        entry_barrier=entry_barrier, changed_generation=changed_generation):
        self.setUp()
        self.pending()
        self.step(button('lfaButton'))
        self.now += gap
        self.ctx.car_state_fresh = False
        self.ctx.car_state_missing = missing
        self.ctx.car_state_last_valid_ns = int((self.now - gap) * 1e9)
        self.panda.safetyRxChecksInvalid = panda_fault
        if changed_generation:
          self.panda.lx3RequestGeneration += 1
        self.assertEqual(self.step(events=('tooDistracted',) if entry_barrier else ()), (False, False))
        self.assertIn('tooDistracted' if entry_barrier else 'controlsMismatch', self.ctx.events.events)
        self.assertIsNone(self.ctx.lx3_engagement.pending)
        self.assertFalse(self.ctx.lx3_engagement.ack_valid and self.ctx.lx3_engagement.ack_mode != Mode.OFF)

  def test_captured_override_torque_yields_lateral_on_second_rising_sample(self):
    # Execute the production CarrotControls class without a vehicle or sockets.
    class Params:
      def get_int(self, name):
        return {'LatSuspendAngleDeg': 300, 'LaneChangeNeedTorque': 0}[name]
    env = dict(Params=Params, DT_CTRL=.01, math=math)
    load_definitions(ROOT / 'selfdrive/carrot/carrot_controls.py', env, {'CarrotControls'})
    controls = env['CarrotControls'](NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'))
    cs = NS(steeringPressed=False, steeringAngleDeg=1.0, steeringTorque=0,
            leftBlinker=False, rightBlinker=False)
    self.assertTrue(controls.lat_suspend_control(cs, True, 1))
    cs.steeringTorque = -170  # First measured pre-override sample in the route.
    self.assertTrue(controls.lat_suspend_control(cs, True, 2))
    self.assertTrue(controls.lat_suspend_control(cs, True, 2))  # Duplicate controlsd tick is not a second sample.
    cs.steeringTorque = -236  # The next sample remains above 150, before >250.
    self.assertFalse(controls.lat_suspend_control(cs, True, 3))
    cs.steeringTorque = 0
    for sample in range(4, 53):
      self.assertFalse(controls.lat_suspend_control(cs, True, sample))
    self.assertTrue(controls.lat_suspend_control(cs, True, 53))
    controls.CP.carFingerprint = 'OTHER'
    cs.steeringTorque = 170
    self.assertTrue(controls.lat_suspend_control(cs, True, 54))

  def test_config_fault_extra_panda_and_stock_long_deny(self):
    for field, value in (('safetyRxChecksInvalid', True), ('safetyParam', 0), ('safetyModel', 'noOutput'),
                         ('alternativeExperience', 1), ('faults', ['relayMalfunction'])):
      self.setUp()
      setattr(self.panda, field, value)
      self.assertEqual(self.engage(), (False, False), field)
    for pandas in ([], [self.panda, self.panda]):
      self.setUp()
      self.ctx.sm['pandaStates'] = pandas
      self.assertEqual(self.engage('mainCruise'), (False, False))
    self.setUp()
    self.ctx.CP.openpilotLongitudinalControl = False
    self.assertEqual(self.engage('mainCruise'), (False, False))

  def test_invalid_stale_or_future_panda_timestamp_denies(self):
    self.ctx.sm.valid_streams = False
    self.assertEqual(self.engage(), (False, False))
    for stamp in (0, 749_000_000, 1_001_000_000):
      self.setUp()
      self.refresh_panda = False
      self.ctx.sm.logMonoTime['pandaStates'] = stamp
      self.assertEqual(self.engage(), (False, False))

  def test_can_before_companion_waits_disabled_without_ack(self):
    self.assertEqual(self.step(button('lfaButton')), (False, False))
    self.assertFalse(self.ctx.enabled)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.now += .1
    self.assertEqual(self.step(), (False, False))
    self.pending()
    self.now += .1
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.ctx.state_machine.state, State.preEnabled)
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.accept()
    self.now += .1
    self.assertEqual(self.step(), (True, False))

  def test_companion_before_can_does_not_create_intent(self):
    self.pending()
    self.assertEqual(self.step(), (False, False))
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.step(button('lfaButton')), (False, False))
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)

  def test_wrong_counter_cannot_bind_pending(self):
    self.pending(counter=42)
    self.step(button('lfaButton', counter=40))
    self.assertFalse(self.ctx.enabled)
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)

  def test_changed_generation_counter_or_mode_cannot_activate(self):
    for field in ('lx3RequestGeneration', 'lx3PhysicalCounter', 'lx3RequestedMode'):
      self.setUp()
      self.pending()
      self.step(button('lfaButton'))
      self.accept()
      setattr(self.panda, field, getattr(self.panda, field) + 1)
      self.assertEqual(self.step(), (False, False))
      self.assertFalse(self.ctx.enabled)

  def test_late_ack_cannot_resurrect_and_fresh_request_can_retry(self):
    self.pending()
    self.step(button('lfaButton'))
    self.accept()
    self.now += .501
    self.assertEqual(self.step(), (False, False))
    self.assertEqual(self.engage(counter=42), (True, False))

  def test_new_request_after_expiry_is_not_mistaken_for_second_toggle(self):
    self.pending()
    self.step(button('lfaButton'))
    self.now += .501
    self.assertEqual(self.engage(counter=42), (True, False))

  def test_rapid_second_toggle_and_ordered_batch_are_off(self):
    self.pending()
    self.step(button('lfaButton'))
    self.assertEqual(self.step(button('lfaButton', counter=42)), (False, False))
    self.assertEqual(self.ctx.state_machine.state, State.disabled)
    self.accept()
    self.assertEqual(self.step(), (False, False))
    self.setUp()
    self.pending()
    self.assertEqual(self.step(button('lfaButton', counter=40), button('lfaButton', counter=42)), (False, False))
    self.assertIsNone(self.ctx.lx3_engagement.pending)

  def test_res_set_during_pending_does_not_change_generation_or_deadline(self):
    self.pending()
    self.step(button('lfaButton'))
    intent = self.ctx.lx3_engagement
    before = intent.pending_generation, intent.pending_since
    self.now += .1
    self.step(button('accelCruise', counter=42))
    self.assertEqual((intent.pending_generation, intent.pending_since), before)

  def test_rejection_retention_and_active_ack_lifetime(self):
    self.engage(events=('seatbeltNotLatched',))
    generation = self.ctx.lx3_engagement.ack_generation
    self.now += .11
    self.step()
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, generation)
    self.assertTrue(self.ctx.lx3_engagement.ack_valid)
    self.engage(counter=42)
    self.now += .01
    self.step()
    self.assertFalse(self.ctx.lx3_engagement.ack_valid)
    self.assertEqual(self.ctx.lx3_engagement.ack_generation, 0)

  def test_cancel_waiting_and_fault_after_ack_never_resume(self):
    for events, buttons in (((), (button('cancel', True),)), (('steerUnavailable',), ())):
      self.setUp()
      self.pending()
      self.step(button('lfaButton'))
      self.assertEqual(self.step(*buttons, events=events), (False, False))
      self.accept()
      self.assertEqual(self.step(), (False, False))

  def test_output_has_no_always_lateral_fallback(self):
    for mode in Mode:
      self.assertEqual(permissions(mode, False, False, True, True, True), (False, False))
      self.assertEqual(permissions(mode, True, False, True, True, True), (False, False))
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
                     {'create_ccnc_messages', 'create_steering_messages_camera_scc', 'create_acc_control_scc2',
                      'oem_emergency_steering'})
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', flags=1)
    self.can = NS(CAM=2, ECAN=0)
    self.cc = NS(enabled=False, latActive=False)

  def acc(self, enabled, gas=False, soft_hold=1, carrot=0, guarded=True):
    cs = NS(scc_control={'COUNTER':3, 'InfoDisplay':0}, softHoldActive=soft_hold, paddle_button_prev=0,
            out=NS(gasPressed=gas, vEgo=10, aEgo=0))
    jerk = NS(carrot_cruise=carrot, carrot_cruise_accel=1.5, jerk_u=2, jerk_l=2)
    hud = NS(leadDistanceBars=2, leadVisible=False)
    return self.env['create_acc_control_scc2'](Packer(), self.can, enabled, 1, 1, False, False,
                                               60, hud, jerk, cs, lx3_guard=guarded)[2]

  def test_lateral_only_cannot_become_acc_via_soft_hold(self):
    for carrot in (0, 1, 2):
      msg = self.acc(False, carrot=carrot)
      self.assertEqual((msg['ACCMode'], msg['aReqRaw'], msg['aReqValue'], msg['StopReq']), (0, 0, 0, 0))

  def test_gas_override_is_zero_accel_and_no_stop_request(self):
    for carrot in (0, 1, 2):
      msg = self.acc(True, gas=True, carrot=carrot)
      self.assertEqual((msg['ACCMode'], msg['aReqRaw'], msg['aReqValue'], msg['StopReq']), (2, 0, 0, 0))

  def test_other_models_retain_soft_hold_authority(self):
    msg = self.acc(False, guarded=False)
    self.assertEqual((msg['ACCMode'], msg['aReqRaw'], msg['StopReq']), (1, 1, 1))

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

  def test_active_lx3_requires_exact_guarded_angle_long_profile(self):
    path = ROOT / 'opendbc_repo/opendbc/car/hyundai/interface.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    guard = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                 any(isinstance(x, ast.Attribute) and x.attr == 'dashcamOnly' for x in ast.walk(n)))
    for name, param, angle, longitudinal, expected in (
        ('lx3', 190, True, True, False), ('other', 190, True, True, False),
        ('lx3', 186, True, False, True), ('lx3', 190, False, True, True),
        ('lx3', 190, True, False, True), ('lx3', 158, True, True, True)):
      with self.subTest(name=name, param=param, angle=angle, longitudinal=longitudinal):
        ret = NS(dashcamOnly=False, safetyConfigs=[NS(safetyParam=param)],
                 steerControlType='angle' if angle else 'torque', openpilotLongitudinalControl=longitudinal)
        exec(compile(ast.Module(body=[guard], type_ignores=[]), str(path), 'exec'),
             dict(candidate=name, CAR=NS(HYUNDAI_PALISADE_LX3_HEV='lx3'), ret=ret,
                  HyundaiSafetyFlags=SafetyFlags, SteerControlType=NS(angle='angle')))
        self.assertEqual(ret.dashcamOnly, expected)
        self.assertEqual(ret.safetyConfigs[-1].safetyParam,
                         (param | SafetyFlags.LX3_ENGAGEMENT_GUARD.value) if name == 'lx3' else param)

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
    self.assertIn(0x162, parser.addresses)  # Required health is registered at startup.
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
