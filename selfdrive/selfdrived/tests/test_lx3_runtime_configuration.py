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


class TestLx3RuntimeConfiguration(unittest.TestCase):
  def setUp(self):
    directory = tempfile.TemporaryDirectory()
    self.addCleanup(directory.cleanup)
    self.params = Params(directory.name)
    self.params.put_int('CanfdHDA2', 2)
    for module in ('interface', 'hyundaicanfd', 'carcontroller'):
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


if __name__ == '__main__':
  unittest.main()
