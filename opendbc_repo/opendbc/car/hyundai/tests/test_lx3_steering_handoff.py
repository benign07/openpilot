import unittest
from types import SimpleNamespace as NS

from opendbc.can import CANPacker
from opendbc.car.hyundai import hyundaicanfd
from opendbc.car.hyundai.carcontroller import lx3_camera_steering_handoff


class TestLx3SteeringHandoff(unittest.TestCase):
  def setUp(self):
    self.packer = CANPacker('hyundai_canfd_lx3_hev_generated')
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV')
    self.can = NS(ECAN=0, CAM=2)
    self.cs = NS(mdps=None, steer_touch_2af=None, lfa_alt={'COUNTER': 42, 'LKAS_ANGLE_ACTIVE': 2},
                 lfa=None, adrv_0x161=None)
    self.companion = (0x12A, bytes(16), 0)

  def messages(self, active):
    cc = NS(latActive=active)
    steering = hyundaicanfd.create_steering_messages_camera_scc(
      0, self.packer, self.cp, self.can, cc, active, 0, self.cs, 3.0, 25 if active else 0, True)
    self.assertEqual([msg[0] for msg in steering], [0xCB])
    return steering + [self.companion]

  def test_inactive_session_preserves_oem_camera_steering(self):
    selected, owned = lx3_camera_steering_handoff(self.messages(False), False, False)
    self.assertFalse(owned)
    self.assertEqual(selected, [self.companion])

  def test_driver_yield_sends_one_neutral_then_stops_replacing_oem(self):
    active, owned = lx3_camera_steering_handoff(self.messages(True), True, False)
    self.assertTrue(owned)
    self.assertEqual([msg[0] for msg in active], [0xCB, 0x12A])
    self.assertEqual((active[0][1][3] >> 4) & 3, 2)

    neutral, owned = lx3_camera_steering_handoff(self.messages(False), False, owned)
    self.assertFalse(owned)
    self.assertEqual([msg[0] for msg in neutral], [0xCB, 0x12A])
    self.assertEqual((neutral[0][1][3] >> 4) & 3, 1)
    self.assertEqual(neutral[0][1][6], 0)

    for _ in range(53):  # Longer than the faulting 0.53-second neutral stream.
      selected, owned = lx3_camera_steering_handoff(self.messages(False), False, owned)
      self.assertFalse(owned)
      self.assertEqual(selected, [self.companion])

    resumed, owned = lx3_camera_steering_handoff(self.messages(True), True, owned)
    self.assertTrue(owned)
    self.assertEqual([msg[0] for msg in resumed], [0xCB, 0x12A])

  def test_missing_camera_template_cannot_claim_host_ownership(self):
    selected, owned = lx3_camera_steering_handoff([self.companion], True, False)
    self.assertEqual(selected, [self.companion])
    self.assertFalse(owned)


if __name__ == '__main__':
  unittest.main()
