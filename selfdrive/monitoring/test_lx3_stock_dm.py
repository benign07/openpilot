"""Stock Carrot DisableDM contract on the LX3 authority profile. No new monitor."""
from types import SimpleNamespace
import unittest

from cereal import car, log
import cereal.messaging as messaging
from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix

EventName = log.OnroadEvent.EventName


class TestStockDmCompatibility(unittest.TestCase):
  def setUp(self):
    self.prefix = OpenpilotPrefix()
    self.prefix.__enter__()
    self.addCleanup(self.prefix.__exit__, None, None, None)
    self.params = Params()
    self.CP = car.CarParams.new_message(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', brand='hyundai',
                                      steerControlType='angle', openpilotLongitudinalControl=True)
    self.CP.init('safetyConfigs', 1)[0].safetyParam = 2238
    self.CP.lateralTuning.init('pid')

  def test_original_process_selection_and_driver_view(self):
    from openpilot.system.manager.process_config import enable_dm, enable_webrtc, managed_processes
    self.assertNotIn('wheel_monitord', managed_processes)
    for fingerprint in ('HYUNDAI_PALISADE_LX3_HEV', 'KIA_EV9'):
      self.CP.carFingerprint = fingerprint
      for mode in (0, 1, 2):
        self.params.put_int('DisableDM', mode)
        for started in (False, True):
          for preview in (False, True):
            with self.subTest(car=fingerprint, mode=mode, started=started, preview=preview):
              self.params.put_bool('IsDriverViewEnabled', preview)
              self.assertEqual(enable_dm(started, self.params, self.CP), mode == 0 and (started or preview))
              self.assertEqual(enable_webrtc(started, self.params, self.CP), mode == 2)

  def selfdrived(self, mode):
    from openpilot.selfdrive.selfdrived.selfdrived import SelfdriveD
    self.params.put_int('DisableDM', mode)
    self.params.put_int('LongitudinalPersonality', 1)
    sd = SelfdriveD(self.CP)
    sd.initialized = True
    for s in sd.sm.services:
      sd.sm.alive[s] = sd.sm.valid[s] = sd.sm.freq_ok[s] = True
    sd.sm.data['controlsState'] = messaging.new_message('controlsState').controlsState
    sd.sm['controlsState'].lateralControlState.init('angleState')
    return sd

  def test_disabled_camera_does_not_add_lx3_monitoring_prerequisite(self):
    sd = self.selfdrived(1)
    self.assertNotIn('driverCameraState', sd.camera_packets)
    self.assertIn('driverMonitoringState', sd.sm.ignore_alive)
    sd.sm.alive['driverMonitoringState'] = sd.sm.valid['driverMonitoringState'] = False
    sd.sm.data['driverMonitoringState'] = messaging.new_message('driverMonitoringState').driverMonitoringState
    sd.sm['driverMonitoringState'].activePolicy = log.DriverMonitoringState.MonitoringPolicy.wheeltouch
    sd.sm['driverMonitoringState'].alertLevel = log.DriverMonitoringState.AlertLevel.three
    sd.sm['driverMonitoringState'].lockout = True
    # Other missing vehicle inputs remain real faults; assert only DM behavior.
    sd.update_events(car.CarState.new_message())
    for event in (EventName.lx3MonitoringRequired, EventName.driverUnresponsive3, EventName.tooDistracted):
      self.assertNotIn(event, sd.events.names)
    self.assertFalse(self.params.get_bool('DriverTooDistracted'))

  def test_enabled_camera_keeps_original_alert_and_lockout(self):
    sd = self.selfdrived(0)
    self.assertIn('driverCameraState', sd.camera_packets)
    sd.sm.data['driverMonitoringState'] = messaging.new_message('driverMonitoringState').driverMonitoringState
    sd.sm['driverMonitoringState'].activePolicy = log.DriverMonitoringState.MonitoringPolicy.vision
    sd.sm['driverMonitoringState'].alertLevel = log.DriverMonitoringState.AlertLevel.three
    sd.sm['driverMonitoringState'].lockout = True
    sd.update_events(car.CarState.new_message())
    self.assertIn(EventName.driverDistracted3, sd.events.names)
    self.assertIn(EventName.tooDistracted, sd.events.names)
    self.assertTrue(self.params.get_bool('DriverTooDistracted'))

  def test_actual_controls_publish_matches_original_disabled_behavior(self):
    from openpilot.selfdrive.controls.controlsd import Controls
    controls = Controls.__new__(Controls)
    controls.CP = self.CP
    controls.params = self.params
    controls.calibrated_pose = None
    controls.curvature = controls.desired_curvature = 0.
    controls.lanefull_mode_enabled = False
    controls.LoC = SimpleNamespace(long_control_state=0, pid=SimpleNamespace(p=0., i=0., f=0.))
    controls.sm = messaging.SubMaster(['carState', 'longitudinalPlan', 'modelV2', 'carrotMan', 'selfdriveState',
                                      'radarState', 'driverAssistance', 'carOutput', 'driverMonitoringState'])
    dm = messaging.new_message('driverMonitoringState').driverMonitoringState
    dm.alertLevel = log.DriverMonitoringState.AlertLevel.three
    controls.sm.data['driverMonitoringState'] = dm
    sent = {}
    controls.pm = SimpleNamespace(send=lambda key, message: sent.__setitem__(key, message.to_bytes()))
    for mode, expected in ((0, True), (1, False), (2, False)):
      self.params.put_int('DisableDM', mode)
      controls.publish(car.CarControl.new_message(), log.ControlsState.LateralAngleState.new_message())
      self.assertEqual(messaging.log_from_bytes(sent['controlsState']).controlsState.forceDecel, expected)


if __name__ == '__main__':
  unittest.main()
