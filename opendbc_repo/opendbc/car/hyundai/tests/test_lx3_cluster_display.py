import copy
import unittest
from collections import defaultdict
from types import SimpleNamespace as NS
from unittest.mock import patch

from opendbc.can import CANPacker
from opendbc.car.hyundai import hyundaicanfd
from opendbc.car.hyundai.values import HyundaiFlags


class RecordingPacker:
  def __init__(self):
    self.packer = CANPacker('hyundai_canfd_lx3_hev_generated')
    self.values = {}

  def make_can_msg(self, name, bus, values, **kwargs):
    self.values[name] = dict(values)
    return self.packer.make_can_msg(name, bus, values, **kwargs)


class TestLx3ClusterDisplay(unittest.TestCase):
  def setUp(self):
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', flags=HyundaiFlags.CAMERA_SCC)
    self.cc = NS(enabled=True, latActive=True)
    self.hud = NS(activeCarrot=0, setSpeed=25, leadDistanceBars=2, leadVisible=True, leadDistance=30,
                  leadRadar=1, leadRelSpeed=0, leftLaneVisible=True, rightLaneVisible=True,
                  leftLaneDepart=False, rightLaneDepart=False)
    self.cs = NS(modelV2=None, lfahda_cluster=None, cruise_buttons_msg=None,
                 adrv_0x161=defaultdict(int, LANELINE_LEFT=8, LANELINE_RIGHT=2,
                                      LANELINE_CURVATURE=9, LANELINE_CURVATURE_DIRECTION=1),
                 adrv_0x200=None, adrv_0x1ea=None,
                 ccnc_0x162=defaultdict(int, FF_DETECT=4, FF_DISTANCE=15, FAULT_LSS=1),
                 is_metric=True, paddle_button_prev=0, softHoldActive=0, trailer_connected=False,
                 out=NS(cruiseState=NS(available=True), latEnabled=True, steeringAngleDeg=0,
                        leftLaneLine=10, rightLaneLine=11, leftBlindspot=False, rightBlindspot=False,
                        leftBlinker=False, rightBlinker=False))

  def messages(self):
    packer = RecordingPacker()
    with patch.object(hyundaicanfd, 'Params') as params:
      params.return_value.get_int.return_value = 0
      params.return_value.get.return_value = b'0'
      result = hyundaicanfd.create_ccnc_messages(self.cp, packer, NS(ECAN=0, CAM=2),
                                                0, self.cc, self.cs, self.hud, 0, 0, 0, 0, False, 0)
    self.packed = result
    return packer.values

  def test_cluster_wire_checksums_valid_after_display_edits(self):
    self.messages()
    self.assertEqual({address for address, _, _ in self.packed}, {0x161, 0x162})
    for address, data, bus in self.packed:
      self.assertEqual(bus, 0)
      self.assertEqual(int.from_bytes(data[:2], 'little'), hyundaicanfd.hkg_can_fd_checksum(address, None, data))

  def test_checksum_repair_changes_only_two_checksum_bytes(self):
    packer = CANPacker('hyundai_canfd_lx3_hev_generated')
    values = {'CHECKSUM': 0x1234, 'COUNTER': 33, 'FF_DETECT': 4, 'FF_DISTANCE': 25.1, 'FAULT_LSS': 1}
    original = packer.make_can_msg('CCNC_0x162', 0, values)
    repaired = hyundaicanfd._make_ccnc_cluster_msg(packer, 'CCNC_0x162', 0, values, True)
    self.assertEqual(repaired[1][2:], original[1][2:])
    self.assertEqual(repaired[0], original[0])
    self.assertEqual(repaired[2], original[2])
    self.assertEqual(hyundaicanfd._make_ccnc_cluster_msg(packer, 'CCNC_0x162', 0, values, False), original)

  def test_stock_lane_glyphs_preserved_when_active_and_inactive(self):
    for active in (True, False):
      self.cc.latActive = active
      values = self.messages()['ADRV_0x161']
      self.assertEqual((values['LANELINE_LEFT'], values['LANELINE_RIGHT']), (8, 2))

  def test_oem_curve_preserved_independent_of_steering_and_engagement(self):
    self.cs.adrv_0x1ea = defaultdict(int, LANELINE_CURVATURE=7, LANELINE_CURVATURE_DIRECTION=0)
    for active in (True, False):
      for angle in (-30, 0, 30):
        self.cc.latActive = active
        self.cs.out.steeringAngleDeg = angle
        values = self.messages()
        self.assertEqual((values['ADRV_0x161']['LANELINE_CURVATURE'], values['ADRV_0x161']['LANELINE_CURVATURE_DIRECTION']), (9, 1))
        self.assertEqual((values['ADRV_0x1ea']['LANELINE_CURVATURE'], values['ADRV_0x1ea']['LANELINE_CURVATURE_DIRECTION']), (7, 0))

  def test_lead_type_does_not_toggle_at_relative_speed_boundary(self):
    for radar in (0, 1, 2):
      for speed in (-0.11, -0.1, -0.09, 0.0):
        self.hud.leadRadar, self.hud.leadRelSpeed = radar, speed
        self.assertEqual(self.messages()['CCNC_0x162']['FF_DETECT'], 4)

  def test_unengaged_lead_is_gray(self):
    self.cc.enabled = False
    self.assertEqual(self.messages()['CCNC_0x162']['FF_DETECT'], 3)

  def test_lost_lead_clears_stale_copied_icon(self):
    self.hud.leadVisible = False
    values = self.messages()['CCNC_0x162']
    self.assertEqual((values['FF_DETECT'], values['FF_DISTANCE']), (0, 0))

  def test_invalid_or_unrepresentable_distance_clears_icon(self):
    for distance in (0, -1, float('nan'), float('inf'), 204.8):
      self.hud.leadDistance = distance
      messages = self.messages()
      values = messages['CCNC_0x162']
      self.assertEqual((values['FF_DETECT'], values['FF_DISTANCE']), (0, 0))
      self.assertEqual(messages['ADRV_0x161']['TARGET'], 0)
      self.assertEqual(messages['ADRV_0x161']['TARGET_DISTANCE'], 0)

  def test_fault_and_input_values_preserved(self):
    original = copy.deepcopy(self.cs.ccnc_0x162)
    self.assertEqual(self.messages()['CCNC_0x162']['FAULT_LSS'], 1)
    self.assertEqual(self.cs.ccnc_0x162, original)

  def test_other_platform_display_unchanged(self):
    self.cp.carFingerprint = 'OTHER'
    self.hud.leadRadar = 0
    values = self.messages()
    self.assertEqual(values['CCNC_0x162']['FF_DETECT'], 13)
    self.assertEqual(values['ADRV_0x161']['LANELINE_LEFT'], 2)


if __name__ == '__main__':
  unittest.main()
