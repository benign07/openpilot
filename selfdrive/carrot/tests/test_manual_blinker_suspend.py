import unittest
from types import SimpleNamespace
from unittest.mock import patch

from openpilot.selfdrive.carrot.carrot_controls import CarrotControls


class TestManualBlinkerSuspend(unittest.TestCase):
  def setUp(self):
    self.params = {'LaneChangeNeedTorque': -1, 'LatSuspendAngleDeg': 300}
    with patch('openpilot.selfdrive.carrot.carrot_controls.Params') as params:
      params.return_value.get_int.side_effect = self.params.__getitem__
      self.control = CarrotControls(SimpleNamespace(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'))
    self.cs = SimpleNamespace(leftBlinker=False, rightBlinker=False, steeringPressed=False, steeringAngleDeg=0)

  def update(self, active=True):
    return self.control.lat_suspend_control(self.cs, active)

  def test_each_manual_blinker_yields_without_driver_torque(self):
    for side in ('leftBlinker', 'rightBlinker'):
      self.assertTrue(self.update())
      setattr(self.cs, side, True)
      self.assertFalse(self.update())
      self.assertFalse(self.cs.steeringPressed)
      setattr(self.cs, side, False)
      self.update(False)

  def test_no_resume_during_blinker_gap_or_driver_input(self):
    self.cs.leftBlinker = True
    self.assertFalse(self.update())
    self.cs.leftBlinker = False
    for _ in range(20):
      self.assertFalse(self.update())
    self.cs.steeringPressed = True
    for _ in range(100):
      self.assertFalse(self.update())
    self.cs.steeringPressed = False
    self.cs.steeringAngleDeg = 25
    for _ in range(100):
      self.assertFalse(self.update())
    self.cs.steeringAngleDeg = 0
    for _ in range(49):
      self.assertFalse(self.update())
    self.assertTrue(self.update())

  def test_hazards_do_not_start_a_manual_lane_change(self):
    self.cs.leftBlinker = self.cs.rightBlinker = True
    self.assertTrue(self.update())

  def test_hazards_do_not_resume_an_existing_handoff(self):
    self.cs.leftBlinker = True
    self.assertFalse(self.update())
    self.cs.rightBlinker = True
    for _ in range(100):
      self.assertFalse(self.update())

  def test_auto_lane_change_modes_unchanged(self):
    self.cs.leftBlinker = True
    for setting in (0, 1):
      self.params['LaneChangeNeedTorque'] = setting
      self.assertTrue(self.update())

  def test_other_platforms_unchanged(self):
    self.control.CP.carFingerprint = 'OTHER'
    self.cs.leftBlinker = True
    self.assertTrue(self.update())

  def test_upstream_disengagement_is_never_overridden(self):
    self.cs.leftBlinker = True
    self.assertFalse(self.update())
    self.assertFalse(self.update(False))
    self.assertFalse(self.control.manual_blinker_suspended)
    self.assertFalse(self.update())

  def test_existing_large_angle_suspend_remains(self):
    self.cs.steeringPressed = True
    self.cs.steeringAngleDeg = 310
    for _ in range(110):
      result = self.update()
    self.assertFalse(result)
    self.assertTrue(self.control.lat_suspend_active)

  def test_handoff_reaches_angle_actuator_without_disabling_oem_emergency(self):
    from opendbc.car.hyundai import hyundaicanfd

    class Packer:
      def make_can_msg(self, name, bus, values, **kwargs):
        return name, dict(values), bus

    self.cs.leftBlinker = True
    cc = SimpleNamespace(latActive=self.update())
    cs = SimpleNamespace(adrv_0x161={'ALERTS_1': 0}, mdps=None, steer_touch_2af=None, lfa=None,
                         lfa_alt={'LKAS_ANGLE_ACTIVE': 2, 'LKAS_ANGLE_CMD': 5, 'LKAS_ANGLE_MAX_TORQUE': 150})
    bus = SimpleNamespace(ECAN=0, CAM=2)
    for emergency in (False, True):
      cs.adrv_0x161['ALERTS_1'] = 21 if emergency else 0
      result = hyundaicanfd.create_steering_messages_camera_scc(0, Packer(), self.control.CP, bus,
                                                               cc, cc.latActive, 100, cs, 10, 200, True)
      values = result[0][1]
      self.assertEqual(values['LKAS_ANGLE_ACTIVE'], 2 if emergency else 1)
      self.assertEqual(values['LKAS_ANGLE_MAX_TORQUE'], 150 if emergency else 0)


if __name__ == '__main__':
  unittest.main()
