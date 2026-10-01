"""Actual Linux Params, CarInterface and generated DBC configuration.

Synthetic fingerprint addresses, no vehicle I/O. Only the exact guarded profile is active.
"""
import tempfile
import unittest
from unittest.mock import patch

from openpilot.common.params import Params
from opendbc.car.hyundai.interface import CarInterface
from opendbc.car.hyundai.carstate import CarState
from opendbc.car.hyundai.carcontroller import CarController
from opendbc.car.hyundai.values import CAR, DBC
from opendbc.car import Bus
from opendbc.car.hyundai.hyundaicanfd import hkg_can_fd_checksum
from cereal import car, log


class TestLx3RuntimeConfiguration(unittest.TestCase):
  def setUp(self):
    directory = tempfile.TemporaryDirectory()
    self.addCleanup(directory.cleanup)
    self.params = Params(directory.name)
    self.params.put_int('CanfdHDA2', 2)
    for module in ('interface', 'hyundaicanfd', 'carcontroller', 'carstate'):
      patcher = patch(f'opendbc.car.hyundai.{module}.Params', return_value=self.params)
      patcher.start()
      self.addCleanup(patcher.stop)

  def configuration(self, mode=1):
    fingerprint = {0: {0x105: 32, 0xEA: 24, 0x10B: 16, 0x1AA: 16, 0x69: 8},
                   1: {0x110: 32, 0x362: 32, 0x210: 32},
                   2: {0xCB: 24, 0x12A: 16, 0x161: 32, 0x162: 32, 0x1A0: 32}}
    self.params.put_int('HyundaiCameraSCC', mode)
    return CarInterface.get_params(CAR.HYUNDAI_PALISADE_LX3_HEV, fingerprint, [], True, False, False)

  def test_last_verified_device_profile_selects_guarded_angle_configuration(self):
    cp = self.configuration()
    self.assertFalse(cp.dashcamOnly)
    self.assertTrue(cp.openpilotLongitudinalControl)
    self.assertEqual(str(cp.steerControlType), 'angle')
    self.assertEqual(cp.safetyConfigs[-1].safetyParam, 1214)

  def test_stock_long_parameter_is_kept_as_unsupported_guarded_configuration(self):
    cp = self.configuration(3)
    self.assertTrue(cp.dashcamOnly)
    self.assertFalse(cp.openpilotLongitudinalControl)
    self.assertEqual(cp.safetyConfigs[-1].safetyParam, 1210)

  def test_actual_generated_parser_and_controller_use_current_display_definition(self):
    cp = self.configuration()
    parser = CarState.get_can_parsers_canfd(None, cp)[Bus.cam]
    controller = CarController(DBC[cp.carFingerprint], cp)
    self.assertIsNotNone(controller.lx3_cluster)
    self.assertEqual((controller.CAN.ECAN, controller.CAN.ACAN, controller.CAN.CAM), (0, 1, 2))
    for name in controller.lx3_cluster.SOURCES:
      message = parser.dbc.name_to_msg[name]
      self.assertEqual(message.sigs['COUNTER'].type, 0)
      self.assertIsNotNone(message.sigs['CHECKSUM'].calc_checksum)
      self.assertTrue(any(s.startswith('RAW_UNMAPPED_') for s in message.sigs))
      self.assertEqual(set(message.sigs), set(controller.packer.dbc.name_to_msg[name].sigs))
    for name in ('LFA', 'LFA_ALT', 'SCC_CONTROL'):
      message = parser.dbc.name_to_msg[name]
      self.assertIsNotNone(message.sigs['CHECKSUM'].calc_checksum)
      self.assertEqual(message.sigs['COUNTER'].type, 0)
      self.assertEqual(set(message.sigs), set(controller.packer.dbc.name_to_msg[name].sigs))
    self.assertIn('RAW_UNMAPPED_79', parser.dbc.name_to_msg['LFA'].sigs)
    pt_parser = CarState.get_can_parsers_canfd(None, cp)[Bus.pt]
    for name in ('MDPS', 'TCS'):
      message = pt_parser.dbc.name_to_msg[name]
      self.assertIsNotNone(message.sigs['CHECKSUM'].calc_checksum)
      self.assertEqual(message.sigs['COUNTER'].type, 0)

  def test_actual_hud_schema_serializes_nearest_valid_display_lead(self):
    from openpilot.selfdrive.carrot.tests.test_lx3_hud_lead import TestLx3HudLead
    cp = self.configuration()
    self.assertEqual(cp.carFingerprint, CAR.HYUNDAI_PALISADE_LX3_HEV)
    radar = log.RadarState.new_message(
      leadOne={'status': True, 'dRel': 40, 'yRel': 0.2, 'vRel': -1, 'radar': True, 'dPath': 0.1},
      leadTwo={'status': True, 'dRel': 12, 'yRel': -0.3, 'vRel': -3, 'radar': False, 'dPath': -0.8})
    command = car.CarControl.new_message()
    TestLx3HudLead().hud(radar.leadOne, radar.leadTwo, target=command.hudControl)
    with car.CarControl.from_bytes(command.to_bytes()) as reader:
      hud = reader.hudControl
      self.assertTrue(hud.leadVisible)
      self.assertEqual((hud.leadDistance, hud.leadRelSpeed, hud.leadRadar), (12, -3, 0))
      self.assertAlmostEqual(hud.leadDPath, -0.8)
      self.assertFalse(reader.latActive)
      self.assertFalse(reader.longActive)

  def test_real_carstate_producer_serializes_input_health_without_eps_fault(self):
    cp = self.configuration()
    state = CarState(cp)
    parsers = state.get_can_parsers_canfd(cp)
    ns = 1_000_000_000
    def frame(address, length, counter=0, raw=0):
      data = bytearray(length)
      data[2] = counter
      if address == 0x10B:
        data[10] = raw
      data[:2] = hkg_can_fd_checksum(address, None, data).to_bytes(2, 'little')
      return bytes(data)
    schedule = ((0, 0, 'warmingUp'), (2, 0, 'warmingUp'), (4, 0, 'ready'),
                (6, 4, 'requalifying'), (8, 0, 'requalifying'), (10, 1, 'requalifying'),
                (12, 0, 'requalifying'), (14, 0, 'requalifying'), (16, 0, 'ready'))
    for counter, raw, expected in schedule:
      ns += 40_000_000
      parsers[Bus.pt].update([[ns, [(0x10B, frame(0x10B, 16, counter, raw), 0),
                                  (0xEA, frame(0xEA, 24), 0)]]])
      parsers[Bus.cam].update([[ns, [(0x162, frame(0x162, 32), 2)]]])
      result = state.update_canfd(parsers)
      # Serialize the actual CarState producer through the Capnp schema.
      message = car.CarState.new_message(**result.to_dict())
      with car.CarState.from_bytes(message.to_bytes()) as reader:
        self.assertEqual(str(reader.lx3InputState), expected)
        self.assertEqual(reader.lx3PhysicalCounter, counter)
        self.assertEqual(reader.lx3PhysicalCounterValid, expected == 'ready')
        self.assertEqual(reader.lx3InputResetCount, state.lx3_button_intent.input.reset_count)
        self.assertGreater(reader.lx3InputResetCount, 0)
        self.assertFalse(reader.steerFaultTemporary)
        self.assertFalse(reader.steerFaultPermanent)
        self.assertFalse(any(str(event.type) in ('mainCruise', 'lfaButton', 'accelCruise') and not event.pressed
                             for event in reader.buttonEvents))

  def test_required_camera_health_survives_optional_startup_discovery(self):
    # Noon rlog had healthy raw CCNC while the old optional cache was None.
    # Run the real monitor/update path, including count 122 cache assignment;
    # no ControlsReady or unrelated missing parser can imply native authority.
    cp = self.configuration()
    state = CarState(cp)
    parsers = state.get_can_parsers_canfd(cp)
    ns = 1_000_000_000
    counter = 0

    def frame(address, length, corrupt=False):
      data = bytearray(length)
      if address == 0x10B:
        data[2] = counter
      data[:2] = hkg_can_fd_checksum(address, None, data).to_bytes(2, 'little')
      if corrupt:
        data[0] ^= 1
      return bytes(data)

    def tick(camera=True, corrupt=False):
      nonlocal ns, counter
      ns += 40_000_000
      counter = (counter + 2) & 0xff
      parsers[Bus.pt].update([[ns, [(0x10B, frame(0x10B, 16), 0),
                                  (0xEA, frame(0xEA, 24), 0)]]])
      parsers[Bus.cam].update([[ns, [(0x162, frame(0x162, 32, corrupt), 2)] if camera else []]])
      return state.update(parsers)

    self.params.put_bool('ControlsReady', False)
    self.assertIsNone(state.ccnc_0x162)
    self.assertTrue(tick(camera=False).steerFaultTemporary)
    for _ in range(3):
      result = tick()
      self.assertFalse(result.steerFaultTemporary)
      self.assertEqual(state.controls_ready_count, 0)
    self.params.put_bool('ControlsReady', True)
    for expected_count in range(1, 124):
      result = tick()
      self.assertFalse(result.steerFaultTemporary)
      self.assertEqual(state.controls_ready_count, expected_count)
    received = parsers[Bus.cam].ts_nanos['CCNC_0x162']['FAULT_LSS']
    # Invalid CAN must never extend the last-good frame's health lifetime.
    for _ in range(6):
      self.assertFalse(tick(corrupt=True).steerFaultTemporary)
      self.assertEqual(parsers[Bus.cam].ts_nanos['CCNC_0x162']['FAULT_LSS'], received)
    self.assertTrue(tick(corrupt=True).steerFaultTemporary)
    self.assertIsNone(state.ccnc_0x162)
    self.assertFalse(tick().steerFaultTemporary)
    self.assertIsNotNone(state.ccnc_0x162)


if __name__ == '__main__':
  unittest.main()
