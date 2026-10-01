"""Actual messaging health logic for change-triggered onroadEvents, no sockets."""
from types import SimpleNamespace
import unittest

from cereal import messaging
from cereal.services import SERVICE_LIST
from openpilot.selfdrive.selfdrived.lx3_engagement import lx3_control_inputs_valid


class TestLx3EventHealth(unittest.TestCase):
  def setUp(self):
    self.cs = SimpleNamespace(lx3InputState='ready', lx3PhysicalCounterValid=True)
    self.sm = messaging.SubMaster.__new__(messaging.SubMaster)
    sm = self.sm
    sm.services = ['selfdriveState', 'carState', 'modelV2', 'onroadEvents']
    sm.frame = -1
    sm.simulation = False
    sm.static_freq_services = set(sm.services)
    sm.ignore_average_freq = []
    sm.ignore_alive = []
    sm.ignore_valid = []
    for name in ('seen', 'updated', 'alive', 'freq_ok', 'valid'):
      setattr(sm, name, dict.fromkeys(sm.services, False))
    for name in ('recv_time', 'recv_frame', 'logMonoTime'):
      setattr(sm, name, dict.fromkeys(sm.services, 0))
    sm.data = {}
    sm.freq_tracker = {s: messaging.FrequencyTracker(SERVICE_LIST[s].frequency, 100., s == 'selfdriveState')
                       for s in sm.services}
    self.clock = 10.

  def advance(self, seconds, burst=(), events=True):
    start = self.clock
    for index in range(round(seconds * 100)):
      t = start + (index + 1) / 100.
      names = ['selfdriveState', 'carState']
      if index % 5 == 0:
        names.append('modelV2')
      if events and (index % 100 == 0 or index in burst):
        names.append('onroadEvents')
      messages = []
      for name in names:
        message = messaging.new_message(name, 0 if name == 'onroadEvents' else None)
        message.valid = True
        message.logMonoTime = round(t * 1e9)
        messages.append(message.as_reader())
      self.sm.update_msgs(t, messages)
    self.clock = start + seconds

  def healthy(self):
    self.advance(12.)
    self.assertTrue(self.sm.all_checks())

  def test_event_burst_does_not_drop_healthy_control(self):
    self.healthy()
    self.advance(.4, burst=(8, 10, 12, 28))
    self.assertFalse(self.sm.freq_ok['onroadEvents'])
    self.assertFalse(self.sm.all_checks())  # Previous controlsd predicate.
    self.assertTrue(lx3_control_inputs_valid(self.sm, self.cs))

  def test_missing_and_invalid_events_still_block_control(self):
    self.healthy()
    self.sm.valid['onroadEvents'] = False
    self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))
    self.sm.valid['onroadEvents'] = True
    self.advance(10.1, events=False)
    self.assertFalse(self.sm.alive['onroadEvents'])
    self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))

  def test_never_received_events_block_control(self):
    self.advance(12., events=False)
    self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))

  def test_periodic_input_health_remains_required(self):
    self.healthy()
    for service in ('selfdriveState', 'carState', 'modelV2'):
      for health in ('alive', 'valid', 'freq_ok'):
        with self.subTest(service=service, health=health):
          values = getattr(self.sm, health)
          values[service] = False
          self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))
          values[service] = True

  def test_physical_input_readiness_remains_required(self):
    self.healthy()
    self.cs.lx3InputState = 'fault'
    self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))
    self.cs.lx3InputState = 'ready'
    self.cs.lx3PhysicalCounterValid = False
    self.assertFalse(lx3_control_inputs_valid(self.sm, self.cs))


if __name__ == '__main__':
  unittest.main()
