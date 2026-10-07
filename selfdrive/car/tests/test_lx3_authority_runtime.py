"""Real CarInterface/Carrot/Events/StateMachine -> native C -> real builder.

Linux runtime dependencies must be built. Mock only physical input, time and
message delivery; don't assign native permission or replace control decisions.
No vehicle, USB transport, model inference or closed-loop dynamics is simulated.
"""
import binascii
from collections import defaultdict
import ctypes as ct
import json
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import patch

from cereal import car, log, custom
import cereal.messaging as messaging
from opendbc.can.packer import CANPacker
from opendbc.car import Bus
from opendbc.car.hyundai.interface import CarInterface
from opendbc.car.hyundai.carcontroller import apply_steer_angle_limits_physics
from opendbc.car.hyundai.values import CAR, DBC
from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix
from openpilot.selfdrive.car.car_specific import CarSpecificEvents
from openpilot.selfdrive.car.cruise import VCruiseCarrot
from openpilot.selfdrive.car.lx3_authority import copy_status, populate_car_state, configure_control
from openpilot.selfdrive.car.lx3_engagement import Lx3Engagement
from openpilot.selfdrive.selfdrived.events import ET
from openpilot.selfdrive.selfdrived.state import StateMachine

ROOT = Path(__file__).resolve().parents[3]
EventName = log.OnroadEvent.EventName


class Status(ct.LittleEndianStructure):
  _pack_ = 1
  _fields_ = [(n, t) for t, names in (
    (ct.c_uint8, 'version profile allowed armed'),
    (ct.c_uint16, 'reason pendingKey pendingGeneration lateralGeneration longitudinalGeneration pendingAgeMs'),
    (ct.c_uint64, 'epoch'), (ct.c_uint32, 'sequence'), (ct.c_uint16, 'config longPressMs'),
    (ct.c_uint32, 'oemLateralPassthrough oemLongitudinalPassthrough'),
    (ct.c_uint16, 'heartbeatAgeMs inputReady lateralRevision longitudinalRevision oemEmergency longPendingKey longPendingGeneration longPendingAgeMs'),
    (ct.c_uint8, 'pendingAxes longPendingAxes')
  ) for n in names.split()]

  def message(self):
    values = {n: getattr(self, n) for n, _ in self._fields_}
    for n in ('inputReady', 'oemEmergency'): values[n] = bool(values[n])
    return values


class Messages(dict):
  def __init__(self):
    super().__init__(carControl=car.CarControl.new_message(), pandaStates=[])
    self.alive = defaultdict(bool); self.valid = defaultdict(bool); self.logMonoTime = defaultdict(int)


class TestLx3Runtime(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.build = tempfile.TemporaryDirectory()
    so = Path(cls.build.name) / 'lx3.so'
    cmd = ['cc', '-shared', '-fPIC', '-std=gnu11', '-Wall', '-Wextra', '-Werror', '-Wno-pointer-to-int-cast',
           '-I' + str(ROOT / 'opendbc_repo/opendbc/safety'), '-I' + str(ROOT / 'opendbc_repo/opendbc/safety/board'),
           str(Path(__file__).with_name('lx3_runtime_native.c')), '-lm', '-o', str(so)]
    subprocess.run(cmd, check=True)
    cls.native = ct.CDLL(str(so))
    cls.native.fixture_status.argtypes = [ct.POINTER(Status)]
    cls.native.fixture_rx.argtypes = [ct.c_uint, ct.c_char_p, ct.c_uint]
    cls.native.fixture_tx.argtypes = [ct.c_uint, ct.c_char_p, ct.c_uint, ct.c_uint]
    assert ct.sizeof(Status) == 58

  @classmethod
  def tearDownClass(cls):
    cls.build.cleanup()

  def setUp(self):
    self.prefix = OpenpilotPrefix(); self.prefix.__enter__(); self.addCleanup(self.prefix.__exit__, None, None, None)
    self.params = Params()
    values = {'HyundaiCameraSCC': 1, 'CanfdHDA2': 1, 'AutoCruiseControl': 2, 'AutoEngage': 2,
              'LfaButtonMode': 0, 'CancelButtonMode': 0, 'CruiseButtonLongDelay': 40, 'CruiseSpeedUnit': 10,
              'CruiseSpeedUnitBasic': 1, 'CruiseButtonMode': 2, 'AutoGasTokSpeed': 30, 'AutoGasCancelSpeed': 30,
              'LongitudinalPersonalityMax': 4, 'MaxAngleFrames': 89, 'UseLaneLineSpeed': 40, 'HDPuse': 0}
    for k, v in values.items(): self.params.put_int(k, v)
    self.params.put_bool('ControlsReady', True); self.params.put_bool('AlwaysLateral', True)
    fp = {i: {} for i in range(8)}
    fp[0] = {0x105:32, 0x10B:16, 0x175:24, 0xA0:24, 0xEA:24, 0x1AA:16, 69:24}
    fp[1] = {0x110:32}  # Real alternate HDA2 fingerprint, not a forced safety parameter.
    fp[2] = {0xCB:24, 0x12A:16, 0x1A0:32, 0x161:32}
    self.params.put('FingerPrints', str(fp))
    self.CP = CarInterface.get_params(CAR.HYUNDAI_PALISADE_LX3_HEV, fp, [], True, False, False)
    self.assertEqual(self.CP.safetyConfigs[-1].safetyParam, 2238)
    self.CI = CarInterface(self.CP); self.packer = CANPacker(DBC[self.CP.carFingerprint][Bus.pt])
    self.cruise = VCruiseCarrot(self.CP); self.car_events = CarSpecificEvents(self.CP)
    self.machine = StateMachine(); self.handshake = Lx3Engagement(); self.sm = Messages()
    self.prev = car.CarState.new_message(); self.CC = self.sm['carControl']; self.CS = self.prev
    self.now = 2_000_000_000; self.frame = 0; self.counters = defaultdict(int)
    self.enabled = False; self.trace = []; self.details = []; self.native.fixture_init()
    self.gear = next(k for k,v in self.CI.CS.shifter_values.items() if v == 'D')
    self.step(0, ticks=220)
    self.assertFalse(self.enabled)
    self.assertFalse(self.CS.latEnabled)  # AutoEngage preference cannot grant at boot.

  def tearDown(self):
    # Include state transitions, not just the final assertion, in CI evidence.
    previous = None
    for row in self.details:
      state = {k:v for k,v in row.items() if k != 'frame'}
      if state != previous:
        print('TRACE', self._testMethodName, json.dumps(row, sort_keys=True))
        previous = state

  def msg(self, name, values, bus=0):
    address, raw, _ = self.packer.make_can_msg(name, bus, values)
    data = bytearray(raw)
    self.counters[address] = (self.counters[address] + (2 if address == 0x10B else 1)) & 255
    data[2] = self.counters[address]
    if len(data) in (8,16,24,32):
      crc = binascii.crc_hqx(data[2:] + address.to_bytes(2,'little'), 0) ^ {8:0x5F29,16:0x041D,24:0x819D,32:0x9F5B}[len(data)]
      data[:2] = crc.to_bytes(2,'little')
    return address, bytes(data), bus

  def status(self):
    s = Status(); self.native.fixture_status(ct.byref(s)); return s

  def step(self, key=0, ticks=1, brake=False, gas=0, extra=(), angle=0, speed=80):
    for _ in range(ticks):
      self.frame += 1; self.now += 10_000_000; self.native.fixture_time(self.now // 1000)
      frames = [self.msg('ACCELERATOR_ALT', {'ACCELERATOR_PEDAL': gas}), self.msg('TCS', {'DriverBraking': int(brake)}),
                self.msg('WHEEL_SPEEDS', {f'WHEEL_SPEED_{i}':speed for i in range(1,5)}),
                self.msg('MDPS', {'STEERING_COL_TORQUE':0, 'STEERING_OUT_TORQUE':0, 'STEERING_ANGLE_2':-angle}),
                self.msg('CRUISE_BUTTONS_ALT', {}), self.msg('GEAR', {'GEAR':self.gear}),
                self.msg('LFA_ALT', {}, 2), self.msg('LFA', {}, 2), self.msg('SCC_CONTROL', {}, 2),
                self.msg('ADRV_0x161', {}, 2)]
      if self.frame % 4 == 0:
        p = list(self.msg('CRUISE_BUTTONS_ALT2', {})); d = bytearray(p[1]); d[10] = key
        d[:2] = (binascii.crc_hqx(d[2:]+b'\x0b\x01',0)^0x041D).to_bytes(2,'little'); p[1]=bytes(d); frames.append(tuple(p))
      for addr,data,bus in frames:
        if bus == 0 and addr in (0x105,0x175,0xA0,0xEA,0x1AA,0x10B): self.native.fixture_rx(addr,data,len(data))
      CS = self.CI.update([(self.now,frames)])
      copy_status(CS,self.sm,self.now)
      with patch('openpilot.selfdrive.car.cruise.time.monotonic_ns', return_value=self.now):
        self.cruise.update_v_cruise(CS,self.sm,True)
      CS.latEnabled = self.cruise._lat_enabled; CS.activateCruise = self.cruise._activate_cruise
      CS.vCruise = float(self.cruise.v_cruise_kph); CS.softHoldActive = self.cruise._soft_hold_active
      populate_car_state(CS,self.sm,self.cruise,self.params,self.now)
      events = self.car_events.update(CS,self.prev,self.CC)
      if brake or (gas and self.params.get_bool('DisengageOnAccelerator')): events.add(EventName.pedalPressed)
      for event in extra: events.add(event)
      self.handshake.update(events,CS,self.enabled,self.now)
      self.enabled,_ = self.machine.update(events)
      CC = car.CarControl.new_message(enabled=self.enabled)
      CC.latActive, CC.longActive = configure_control(CC,CS,events.to_msg(),self.enabled,
        self.params.get_bool('AlwaysLateral'),True,True,self.now,self.handshake.request,self.handshake.refusing)
      if self.frame % 10 == 0:
        a = CC.lx3Authority
        self.native.fixture_state(a.intent,a.decisionKey,a.pendingGeneration,int(a.autoResume),a.observedLongRevision,a.config,int(a.refuseLong))
        ps = log.PandaState.new_message(); ps.lx3Authority=self.status().message()
        self.sm['pandaStates']=[ps]; self.sm.alive['pandaStates']=self.sm.valid['pandaStates']=True
        self.sm.logMonoTime['pandaStates']=self.now
      self.trace.append((self.frame,self.enabled,self.status().allowed,self.handshake.request,tuple(events.names)))
      s=self.status()
      self.details.append({'frame':self.frame,'enabled':self.enabled,'native':s.allowed,'armed':s.armed,'reason':s.reason,
        'hostIntent':CC.lx3Authority.intent,'hostLat':CS.latEnabled,'latKey':CS.lx3Authority.lateralDecisionKey,
        'longKey':CS.lx3Authority.longitudinalDecisionKey,'pending':s.pendingKey,'generation':s.pendingGeneration,
        'request':self.handshake.request,'refuse':self.handshake.refusing,'auto':CS.lx3Authority.autoResume,
        'activate':CS.activateCruise,'blocked':self.cruise.lx3_auto_blocked,'carrotLog':self.cruise.log,
        'gear':str(CS.gearShifter),'events':list(events.names),'mode':self.cruise._lfa_button_mode})
      self.CC=CC; self.CS=CS; self.prev=CS.as_reader(); self.sm['carControl']=CC
      self.CI.CS.softHoldActive = CS.softHoldActive  # same bridge as card
    return self.CS

  def main(self):
    self.step(8,ticks=8); self.step(0,ticks=70)

  def lfa(self):
    self.step(128,ticks=8); self.step(0,ticks=40)

  def test_main_and_lfa_only(self):
    self.lfa(); self.assertEqual(self.status().allowed,1); self.assertFalse(self.enabled); self.assertTrue(self.CC.latActive)
    self.lfa(); self.assertEqual(self.status().allowed,0); self.assertFalse(self.CS.latEnabled)
    self.main(); self.assertTrue(self.enabled); self.assertEqual(self.status().allowed,3)
    self.assertTrue(all(not en or allow & 2 for _,en,allow,_,_ in self.trace))

  def test_real_ipc_preserves_independent_native_authority(self):
    self.lfa()
    publisher=messaging.pub_sock('pandaStates')
    subscriber=messaging.sub_sock('pandaStates',timeout=0)
    message=messaging.new_message('pandaStates',1)
    message.valid=True
    message.pandaStates[0].lx3Authority=self.status().message()
    received=None
    for _ in range(50):
      publisher.send(message.to_bytes())
      received=messaging.recv_one_or_none(subscriber)
      if received is not None:
        break
      time.sleep(.01)
    self.assertIsNotNone(received)
    a=received.pandaStates[0].lx3Authority
    self.assertEqual(a.allowed,1)
    self.assertGreater(a.lateralGeneration,0)
    self.assertEqual(a.longitudinalGeneration,0)
    self.assertEqual(a.epoch,self.status().epoch)

  def test_main_before_ready_does_not_latch_or_engage_later(self):
    self.CI.CS.controls_ready_count = 0
    self.main()
    self.assertFalse(self.CI.CS.main_enabled)
    self.assertFalse(self.enabled)
    self.step(ticks=220)
    self.assertFalse(self.enabled)
    self.assertEqual(self.status().allowed, 0)
    self.main()
    self.assertTrue(self.enabled)
    self.assertEqual(self.status().allowed, 3)

  def test_brake_short_auto_resume_and_res(self):
    self.params.put_bool('AlwaysLateral',False); self.main(); self.assertEqual(self.status().allowed,3)
    self.step(brake=True,ticks=8); self.assertFalse(self.enabled); self.assertEqual(self.status().allowed,0)
    self.step(ticks=60); self.assertTrue(self.enabled); self.assertEqual(self.status().allowed,3)
    self.params.put_int('AutoCruiseControl',0); self.step(brake=True,ticks=20); self.step(ticks=20)
    self.step(1,ticks=8); self.step(ticks=35)
    self.assertTrue(self.enabled); self.assertEqual(self.status().allowed,3)

  def test_brake_again_while_auto_request_pending_retains_arming(self):
    self.main()
    self.step(brake=True,ticks=8)
    self.step(ticks=1)
    self.step(brake=True,ticks=8)
    self.assertTrue(self.status().armed & 2)
    self.assertFalse(self.handshake.refusing)
    self.step(ticks=100)
    self.assertTrue(self.enabled)
    self.assertEqual(self.status().allowed,3)

  def test_lfa_brake_and_rejected_press_need_one_press(self):
    self.params.put_bool('AlwaysLateral',False); self.lfa(); self.step(brake=True,ticks=15)
    self.assertEqual(self.status().allowed,0); self.assertFalse(self.CS.latEnabled)
    self.step(ticks=20); self.lfa(); self.assertEqual(self.status().allowed,1)
    self.step(brake=True,ticks=20); self.step(128,brake=True,ticks=8); self.step(brake=True,ticks=20)
    self.assertFalse(self.CS.latEnabled); self.step(ticks=20); self.lfa(); self.assertEqual(self.status().allowed,1)

  def test_auto_without_physical_arm_and_remote_cannot_enable(self):
    self.step(brake=True,ticks=8); self.step(ticks=250)
    self.assertFalse(self.enabled); self.assertEqual(self.status().allowed,0)
    self.sm['carrotMan']=custom.CarrotMan.new_message(carrotCmdIndex=1,carrotCmd='CRUISE',carrotArg='ON')
    self.sm.alive['carrotMan']=True; self.step(ticks=250)
    self.assertFalse(self.enabled); self.assertEqual(self.status().allowed,0)
    self.assertTrue(all(EventName.controlsMismatch not in e for *_,e in self.trace))

  def test_dm_warning_and_lockout_remove_lateral(self):
    self.lfa(); self.step(ticks=20,extra=(EventName.driverDistracted3,))
    self.assertFalse(self.CC.latActive); self.assertEqual(self.status().allowed,0)
    self.step(ticks=70); self.lfa(); self.assertEqual(self.status().allowed,1)
    self.step(ticks=20,extra=(EventName.tooDistracted,)); self.assertEqual(self.status().allowed,0)

  def test_no_entry_between_request_and_grant_cancels_pending_session(self):
    self.step(8,ticks=8); self.step(ticks=32)
    self.assertTrue(self.handshake.request)
    self.step(ticks=1,extra=(EventName.seatbeltNotLatched,))
    self.assertTrue(self.handshake.refusing)
    self.step(ticks=50)
    self.assertFalse(self.enabled)
    self.assertEqual(self.status().allowed,0)
    self.assertFalse(self.status().armed & 2)
    self.step(2,ticks=8); self.step(ticks=40)
    self.assertTrue(self.enabled)

  def test_pending_lfa_does_not_revive_after_short_dm_fault(self):
    self.step(128,ticks=8); self.step(ticks=4)
    self.step(ticks=2,extra=(EventName.driverDistracted3,))
    self.step(ticks=40)
    self.assertFalse(self.CS.latEnabled)
    self.assertEqual(self.status().allowed,0)
    self.lfa()
    self.assertEqual(self.status().allowed,1)

  def test_rejected_soft_hold_does_not_turn_next_set_into_cancel(self):
    self.main()
    self.step(brake=True,speed=0,ticks=300)
    self.assertEqual(self.CS.softHoldActive,1)
    self.step(speed=0,ticks=1)
    self.assertTrue(self.handshake.request or self.handshake.deadline)
    self.step(speed=0,ticks=2,extra=(EventName.seatbeltNotLatched,))
    self.step(speed=0,ticks=70)
    self.assertFalse(self.enabled)
    self.assertEqual(self.CS.softHoldActive,0)
    self.step(2,speed=0,ticks=8); self.step(speed=0,ticks=40)
    self.assertTrue(self.enabled)

  def physical_during_pending_hold(self, button):
    self.main(); self.step(brake=True,speed=0,ticks=298)
    # Start the physical press while braking, then release it during the next
    # pending auto window (before the 10 Hz grant). A SET released *after* a
    # successful automatic hold correctly cancels that hold in stock Carrot.
    self.step(button,brake=True,speed=0,ticks=8)
    self.step(button,speed=0,ticks=1)
    self.assertTrue(self.handshake.deadline)
    self.assertFalse(self.enabled)
    self.step(speed=0,ticks=3)
    self.assertFalse(self.enabled)
    self.step(speed=0,ticks=40)
    self.assertTrue(self.enabled)
    self.assertEqual(self.CS.softHoldActive,0)
    self.assertIsNone(self.cruise.lx3_auto_pending)

  def test_physical_set_supersedes_pending_hold(self):
    self.physical_during_pending_hold(2)

  def test_physical_res_supersedes_pending_hold(self):
    self.physical_during_pending_hold(1)

  def test_internal_gas_pause_does_not_disarm_pending_auto_session(self):
    self.main(); self.step(brake=True,speed=0,ticks=300)
    self.step(speed=0,ticks=1)
    self.step(gas=0.25,speed=0,ticks=8); self.step(speed=0,ticks=20)
    self.assertFalse(self.handshake.refusing)
    self.assertTrue(self.status().armed & 2)
    self.step(brake=True,speed=0,ticks=100); self.step(speed=0,ticks=70)
    self.assertTrue(self.enabled)

  def test_actual_controller_commands_are_admitted(self):
    self.main(); self.assertTrue(self.enabled)
    counts=defaultdict(int)
    for _ in range(25):
      self.step()
      _,msgs=self.CI.apply(self.CC.as_reader(),self.now,None)
      for addr,data,bus in msgs:
        if bus==0 and addr in (0xCB,0x12A,0x1A0):
          gen=self.CC.lx3Authority.longitudinalGeneration if addr==0x1A0 else self.CC.lx3Authority.lateralGeneration
          self.assertTrue(self.native.fixture_tx(addr,data,len(data),gen),f'rejected {addr:x}, native={self.status().message()}')
          counts[addr]+=1
    self.assertTrue(counts[0xCB] and counts[0x1A0],counts)

  def test_initial_large_angle_waits_then_real_controller_recovers(self):
    self.main()
    for angle in (200,200,40,20,20):
      self.step(angle=angle)
      out,msgs=self.CI.apply(self.CC.as_reader(),self.now,None)
      self.assertEqual(out.lx3AngleLimited, angle >= 200)
      for addr,data,bus in msgs:
        if bus == 0 and addr == 0xCB:
          self.assertTrue(self.native.fixture_tx(addr,data,len(data),self.CC.lx3Authority.lateralGeneration))
          self.assertEqual(data[6] == 0, angle >= 200)
      self.assertEqual(self.status().allowed,3)

  def test_shrinking_angle_bound_converges_at_existing_rate(self):
    args=(100,100,30,100,True,2.97,16.4,175)
    legacy=apply_steer_angle_limits_physics(*args)
    corrected=apply_steer_angle_limits_physics(*args,limit_target_first=True)
    self.assertGreater(abs(legacy-100),2)
    self.assertLess(abs(corrected-100),2)
    self.assertLess(corrected,100)
    last=corrected
    for _ in range(2000):
      nxt=apply_steer_angle_limits_physics(100,last,30,last,True,2.97,16.4,175,limit_target_first=True)
      self.assertLessEqual(abs(nxt-last),2)
      self.assertLessEqual(nxt,last)
      last=nxt
    self.assertAlmostEqual(last,legacy,places=6)


if __name__=='__main__': unittest.main()
