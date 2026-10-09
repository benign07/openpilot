"""LX3 observed capability paths must construct the real modern interface.

Synthetic input tests exercise decoding/startup only; they do not establish the
LX3 power-flow enum's physical meaning or vehicle control qualification.
"""
from contextlib import ExitStack
import tempfile
import unittest
from unittest.mock import patch

from openpilot.common.params import Params
from opendbc.can import CANPacker
from opendbc.can.dbc import DBC as DbcDefinition, SignalType
from opendbc.car import Bus, gen_empty_fingerprint, structs
from opendbc.car.hyundai.carstate import CarState, EV_MODE_STATUS_TIMEOUT_NS, _get_ev_mode_state
from opendbc.car.hyundai.interface import CarInterface
from opendbc.car.hyundai.radar_interface import RadarInterface
from opendbc.car.hyundai.values import CANFD_CAR, CAR, DBC, HyundaiExtFlags, HyundaiSafetyFlags


class TestLx3ModernStartup(unittest.TestCase):
  def setUp(self):
    self.stack = ExitStack()
    self.addCleanup(self.stack.close)
    directory = self.stack.enter_context(tempfile.TemporaryDirectory())
    self.params = Params(directory)
    self.params.put_int('HyundaiCameraSCC', 1)
    self.params.put_int('CanfdHDA2', 1)
    for module in ('opendbc.car.interfaces', 'opendbc.car.hyundai.interface',
                   'opendbc.car.hyundai.carstate', 'opendbc.car.hyundai.carcontroller',
                   'opendbc.car.hyundai.radar_interface', 'opendbc.car.hyundai.hyundaicanfd'):
      self.stack.enter_context(patch(module + '.Params', return_value=self.params))

  def interface(self, candidate=CAR.HYUNDAI_PALISADE_LX3_HEV, *, status_bus=0, status_length=32, hybrid_status=True):
    fingerprint = gen_empty_fingerprint()
    fingerprint[0].update({0x105: 32, 0x10B: 16, 0x45: 24, 0xEA: 24})
    fingerprint[1][0x110] = 32
    fingerprint[2][0xCB] = 24
    if hybrid_status:
      fingerprint[0][0xFA] = 32
    if status_bus is not None:
      fingerprint[status_bus][0x230] = status_length
    self.params.put('FingerPrints', repr(fingerprint))
    cp = CarInterface.get_params(candidate, fingerprint, [], False, False, False)
    return CarInterface(cp)

  def test_observed_lx3_capability_constructs_real_interface(self):
    ci = self.interface()
    self.assertFalse(ci.CP.extFlags & HyundaiExtFlags.EV_MODE_STATUS_230)
    self.assertTrue(ci.CP.safetyConfigs[-1].safetyParam & HyundaiSafetyFlags.LX3_AUTHORITY)
    self.assertIsNotNone(ci.CC)
    parser = ci.can_parsers[Bus.pt]
    self.assertEqual(parser.bus, 0)
    self.assertNotIn(0x230, parser.addresses)
    self.assertEqual(_get_ev_mode_state(parser), (False, False))
    # Construction/one empty update cannot supply a physical authority grant.
    self.assertEqual(ci.update([]).lx3Authority.allowed, 0)

  def test_display_capability_preserves_safety_and_rejects_other_bus_or_shape(self):
    normal = self.interface(CAR.HYUNDAI_SANTAFE_MX5_HEV)
    for options in ({'status_bus': None}, {'status_bus': 1, 'status_length': 16},
                    {'status_length': 16}, {'hybrid_status': False}):
      with self.subTest(options=options):
        ci = self.interface(CAR.HYUNDAI_SANTAFE_MX5_HEV, **options)
        self.assertFalse(ci.CP.extFlags & HyundaiExtFlags.EV_MODE_STATUS_230)
        self.assertNotIn(0x230, ci.can_parsers[Bus.pt].addresses)
        self.assertEqual([s.to_dict() for s in ci.CP.safetyConfigs],
                         [s.to_dict() for s in normal.CP.safetyConfigs])
        self.assertEqual(ci.CP.flags, normal.CP.flags)
        self.assertEqual(ci.CP.openpilotLongitudinalControl, normal.CP.openpilotLongitudinalControl)

  def test_stock_display_stays_optional_and_crc_checked(self):
    ci = self.interface(CAR.HYUNDAI_SANTAFE_MX5_HEV)
    self.assertTrue(ci.CP.extFlags & HyundaiExtFlags.EV_MODE_STATUS_230)
    name = DBC[ci.CP.carFingerprint][Bus.pt]
    definition = DbcDefinition(name).addr_to_msg[0x230]
    self.assertEqual(definition.sigs['CHECKSUM'].type, SignalType.HKG_CAN_FD_CHECKSUM)
    parser = ci.can_parsers[Bus.pt]
    self.assertTrue(parser.message_states[0x230].ignore_alive)
    self.assertTrue(parser.message_states[0x230].ignore_counter)
    packer = CANPacker(name)
    for counter, mode in enumerate((1, 3, 6)):
      frame = packer.make_can_msg('HCU_STATUS_230', 0,
                                  {'COUNTER': counter, 'HYBRID_POWER_FLOW_MODE': mode})
      timestamp = 1_000_000_000 + counter * 100_000_000
      self.assertEqual(parser.update([(timestamp, [frame])]), {0x230})
      self.assertEqual(_get_ev_mode_state(parser), (mode in (1, 2, 6), True))
    value_before = parser.ts_nanos['HCU_STATUS_230'].copy()
    address, data, bus = frame
    corrupt = bytearray(data)
    corrupt[-1] ^= 1
    self.assertEqual(parser.update([(timestamp + 100_000_000, [(address, bytes(corrupt), bus)])]), set())
    self.assertEqual(parser.ts_nanos['HCU_STATUS_230'], value_before)
    parser.update([(timestamp + EV_MODE_STATUS_TIMEOUT_NS + 1, [])])
    self.assertEqual(_get_ev_mode_state(parser), (False, False))

  def test_all_canfd_platforms_advertise_only_decodable_status(self):
    # Exercise the capability/parser layer independently of unrelated upstream
    # torque metadata (K5 DL3 24 HEV currently lacks a stock torque-data entry).
    for candidate in CANFD_CAR:
      with self.subTest(candidate=candidate):
        fingerprint = gen_empty_fingerprint()
        fingerprint[0].update({0x105: 32, 0xFA: 32, 0x230: 32, 0x10B: 16})
        fingerprint[1][0x110] = 32
        fingerprint[2][0xCB] = 24
        cp = structs.CarParams()
        cp.carFingerprint = candidate
        cp.flags = int(candidate.config.flags)
        cp = CarInterface._get_params(cp, candidate, fingerprint, [], False, False, False)
        parsers = CarState.get_can_parsers_canfd(None, cp)
        definition = DbcDefinition(DBC[candidate][Bus.pt]).name_to_msg.get('HCU_STATUS_230')
        self.assertEqual(bool(cp.extFlags & HyundaiExtFlags.EV_MODE_STATUS_230),
                         definition is not None)
        self.assertEqual(0x230 in parsers[Bus.pt].addresses, definition is not None)

  def test_lx3_observed_corner_capability_constructs_radar(self):
    # The observed ACAN also contains all twenty 32-byte corner-radar frames.
    # Exercise radard's separate constructor, with the display feature absent.
    ci = self.interface()
    cp = ci.CP
    cp.extFlags |= HyundaiExtFlags.CORNER_RADAR_OBJECTS_235.value
    for corner in (0, 1):
      for tracks in (0, 1):
        with self.subTest(corner=corner, tracks=tracks):
          self.params.put_int('EnableCornerRadar', corner)
          self.params.put_int('EnableRadarTracks', tracks)
          radar = RadarInterface(cp)
          self.assertEqual(radar.rcp_corner_objects is not None, bool(corner))
          self.assertEqual(radar.rcp_tracks is not None, bool(tracks))
          self.assertEqual(radar.rcp_scc.bus, 2)
          if corner:
            self.assertEqual(radar.rcp_corner_objects.bus, 1)
            self.assertEqual(radar.rcp_corner_objects.addresses, set(range(0x235, 0x249)))
          # Empty input only exercises registration/update, never track meaning.
          radar.update([])


if __name__ == '__main__':
  unittest.main()
