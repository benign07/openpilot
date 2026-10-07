"""Production decoder/decision/serializer checks, runnable without vehicle IPC.

Schemas are copied solely because Windows Git checkouts store symlinks as text.
The serializer, decision helpers and switch decoder are the actual source files.
"""
import binascii
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import capnp

ROOT = Path(__file__).resolve().parents[3]


def module(name, path):
  spec = importlib.util.spec_from_file_location(name, ROOT / path)
  mod = importlib.util.module_from_spec(spec)
  sys.modules[name] = mod
  spec.loader.exec_module(mod)
  return mod


buttons = module('lx3_physical_test_source', 'opendbc_repo/opendbc/car/hyundai/lx3_buttons.py')
authority = module('lx3_decision_test_source', 'selfdrive/car/lx3_authority.py')


class TestLx3Authority(unittest.TestCase):
  @classmethod
  def setUpClass(cls):
    cls.temp = tempfile.TemporaryDirectory()
    d = Path(cls.temp.name)
    for name in ('log.capnp', 'custom.capnp', 'deprecated.capnp'):
      shutil.copyfile(ROOT / 'cereal' / name, d / name)
    shutil.copytree(ROOT / 'cereal/include', d / 'include')
    shutil.copyfile(ROOT / 'opendbc_repo/opendbc/car/car.capnp', d / 'car.capnp')
    capnp.remove_import_hook()
    cls.log = capnp.load(str(d / 'log.capnp'))
    cls.car = capnp.load(str(d / 'car.capnp'))
    with patch.dict(sys.modules, {'cereal': SimpleNamespace(log=cls.log)}):
      cls.serializer = module('lx3_serializer_test_source', 'selfdrive/pandad/pandad_api_impl.py')

  @classmethod
  def tearDownClass(cls):
    cls.temp.cleanup()

  def setUp(self):
    self.now = 10_000_000_000
    self.counter = 248
    self.decoder = buttons.PhysicalButtons()
    for _ in range(3): self.feed(0)

  def feed(self, key, step=2, corrupt=False, bus=0):
    self.now += 40_000_000
    self.counter = (self.counter + step) & 255
    d = bytearray(16); d[2] = self.counter; d[10] = key
    d[:2] = (binascii.crc_hqx(d[2:] + b'\x0b\x01', 0) ^ 0x041D).to_bytes(2, 'little')
    if corrupt: d[10] ^= 1
    self.decoder.update([(self.now, [(0x10B, d, bus)])])
    return self.decoder.events

  def state(self, allowed=0):
    CS = self.car.CarState.new_message(latEnabled=True, gearShifter='drive')
    CS.lx3Authority = {'version': 4, 'profile': 1, 'epoch': 123456789, 'allowed': allowed,
      'inputReady': True, 'buttonHealthy': True, 'lateralGeneration': 4 if allowed & 1 else 0,
      'longitudinalGeneration': 5 if allowed & 2 else 0, 'config': 5, 'longPressMs': 700}
    return CS

  def control(self, CS, enabled=False, events=(), fresh=True):
    CC = self.car.CarControl.new_message(enabled=enabled)
    allowed = authority.configure_control(CC, CS, events, enabled, True, True, fresh, self.now)
    return CC, allowed

  def test_decoder_crc_echo_and_recovery(self):
    self.feed(128)
    events = self.feed(0)
    self.assertEqual([(e.button, e.pressed) for e in events], [(128, False)])
    self.assertEqual(events[0].held_ns, 40_000_000)
    self.feed(128, corrupt=True)
    self.assertFalse(self.decoder.stream_healthy(self.now))
    self.assertEqual(self.feed(0, bus=128), [])
    self.feed(0); self.feed(0); self.feed(0)
    self.assertTrue(self.decoder.healthy(self.now))

  def test_resync_does_not_mean_bad_stream(self):
    self.feed(1); self.feed(2)
    self.assertFalse(self.decoder.ready)
    self.assertTrue(self.decoder.stream_healthy(self.now))
    self.assertEqual(self.decoder.events, [])
    self.assertTrue(any(e.button == 4 and e.pressed for e in self.feed(4)))

  def test_batched_duration_and_counter_wrap(self):
    packets=[]
    for key in [128] * 140 + [0]:
      self.counter=(self.counter+2)&255
      d=bytearray(16); d[2]=self.counter; d[10]=key
      d[:2]=(binascii.crc_hqx(d[2:]+b'\x0b\x01',0)^0x041D).to_bytes(2,'little')
      packets.append((0x10B,bytes(d),0))
    events=self.decoder.update([(self.now+40_000_000,packets)])
    release=[e for e in events if e.button==128 and not e.pressed]
    self.assertEqual(len(release),1)
    self.assertEqual(release[0].held_ns,5_600_000_000)

  def test_pending_axis_masks_and_unrelated_lfa_off(self):
    CS=self.state(); a=CS.lx3Authority
    a.pendingKey,a.pendingGeneration,a.pendingAxes=0x800A,8,1
    a.longPendingKey,a.longPendingGeneration,a.longPendingAxes=0x010C,9,2
    a.lateralDecisionKey=a.longitudinalDecisionKey=0x010C
    a.lateralDecisionTime=a.longitudinalDecisionTime=self.now
    CC,_=self.control(CS,enabled=True)
    self.assertEqual((CC.lx3Authority.intent,CC.lx3Authority.pendingGeneration),(2,9))
    fault=self.log.OnroadEvent.new_message(name='driverDistracted3',warning=True)
    CS=self.state(1)
    self.assertEqual(self.control(CS,events=[fault])[0].lx3Authority.intent,0)

  def test_main_tail_and_separate_res(self):
    self.feed(8)
    self.feed(0); first_neutral = self.counter
    self.feed(1)
    self.assertTrue(any(e.button == 1 and not e.pressed for e in self.feed(0)))
    events = []
    for _ in range(5): events.extend(self.feed(0))
    main = [e for e in events if e.button == 8 and not e.pressed]
    self.assertEqual(len(main), 1)
    self.assertEqual(main[0].counter, first_neutral)

  def test_permission_is_not_intent(self):
    CS = self.state()
    CC, active = self.control(CS, enabled=True)
    self.assertEqual(CC.lx3Authority.intent, 3)
    self.assertEqual(active, (False, False))
    self.assertEqual(CC.lx3Authority.pendingGeneration, 0)
    self.assertFalse(authority.monitor_lateral(CS.lx3Authority))

  def test_mixed_host_native_protocol_cannot_supply_permission(self):
    CS = self.state(3)
    self.assertTrue(authority.verified(CS.lx3Authority))
    with patch.object(authority, 'VERSION', 3):
      self.assertFalse(authority.verified(CS.lx3Authority))
      self.assertEqual(self.control(CS, enabled=True)[1], (False, False))
    CS.lx3Authority.version = 3
    self.assertFalse(authority.verified(CS.lx3Authority))
    self.assertEqual(self.control(CS, enabled=True)[1], (False, False))

  def test_transient_freshness_failure_stops_output_without_latching_lfa_off(self):
    CS = self.state(1)
    CC, active = self.control(CS, fresh=False)
    self.assertEqual(active, (False, False))
    self.assertEqual(CC.lx3Authority.intent, 0)
    self.assertFalse(CC.lx3Authority.lateralRefused)
    self.assertEqual(self.control(CS)[1], (True, False))
    fault = self.log.OnroadEvent.new_message(name='driverDistracted3', warning=True)
    CC, active = self.control(CS, events=[fault])
    self.assertFalse(active[0])
    self.assertTrue(CC.lx3Authority.lateralRefused)  # A real fault is still latched.

  def test_independent_citations_in_two_heartbeats(self):
    CS = self.state()
    a = CS.lx3Authority
    a.pendingKey, a.pendingGeneration = 0x800A, 8
    a.pendingAxes = 1
    a.lateralDecisionKey, a.lateralDecisionTime = 0x800A, self.now
    a.longitudinalDecisionKey, a.longitudinalDecisionTime = 0x020C, self.now
    CC, _ = self.control(CS, enabled=True)
    self.assertEqual((CC.lx3Authority.intent, CC.lx3Authority.decisionKey), (1, 0x800A))
    a.allowed, a.lateralGeneration = 1, 8
    a.pendingKey, a.pendingGeneration = 0x020C, 9
    a.pendingAxes = 2
    CC, _ = self.control(CS, enabled=True)
    self.assertEqual((CC.lx3Authority.intent, CC.lx3Authority.decisionKey), (3, 0x020C))
    a.longitudinalDecisionTime = self.now - 600_000_001
    self.assertEqual(self.control(CS, enabled=True)[0].lx3Authority.decisionKey, 0)

  def test_lateral_only_monitoring_and_faults(self):
    CS = self.state(1)
    wrong_mode = self.log.OnroadEvent.new_message(name='wrongCarMode', noEntry=True, userDisable=True)
    CC, active = self.control(CS, events=[wrong_mode])
    self.assertFalse(CC.enabled)
    self.assertEqual(active, (True, False))
    self.assertTrue(authority.monitor_lateral(CS.lx3Authority))
    fault = self.log.OnroadEvent.new_message(name='tooDistracted', noEntry=True)
    CC, active = self.control(CS, events=[fault])
    self.assertEqual(CC.lx3Authority.intent, 0)
    self.assertEqual(active, (False, False))
    self.assertEqual(self.control(CS, fresh=False)[0].lx3Authority.intent, 0)

  def test_pedals_and_oem_emergency(self):
    CS = self.state(3); CS.brakePressed = True
    CC, active = self.control(CS, enabled=True)
    self.assertEqual((CC.lx3Authority.intent, active), (1, (True, False)))
    CS.brakePressed = False; CS.lx3Authority.oemEmergency = True
    CC, active = self.control(CS, enabled=True)
    self.assertEqual((CC.lx3Authority.intent, active), (3, (False, True)))

  def test_actual_serializer_identity_and_legacy_defaults(self):
    CS = self.state(3)
    msgs = [(0xCB, bytes(24), 0), (0x12A, bytes(16), 0), (0x1A0, bytes(32), 0), (0xEA, bytes(24), 2)]
    raw = self.serializer.can_list_to_can_capnp(msgs, 'sendcan', lx3_authority=CS.lx3Authority)
    with self.log.Event.from_bytes(raw) as event:
      self.assertEqual([m.lx3Identity.generation for m in event.sendcan], [4, 4, 5, 0])
      self.assertEqual([m.lx3Identity.valid for m in event.sendcan], [True, True, True, False])
      self.assertEqual([(m.address, m.dat, m.src) for m in event.sendcan], msgs)
    raw = self.serializer.can_list_to_can_capnp(msgs, 'sendcan')
    with self.log.Event.from_bytes(raw) as event:
      self.assertFalse(any(m.lx3Identity.valid for m in event.sendcan))
    self.assertEqual(len(self.serializer.can_capnp_to_list([raw], 'sendcan')[0][1]), 4)


if __name__ == '__main__':
  unittest.main()
