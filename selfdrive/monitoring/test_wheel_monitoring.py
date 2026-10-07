"""Actual stock policy, event handling, process selection and IPC; no vehicle."""
import subprocess
import sys
import time
from types import SimpleNamespace
import unittest

from cereal import car, log
import cereal.messaging as messaging
from openpilot.common.params import Params
from openpilot.common.prefix import OpenpilotPrefix
from openpilot.selfdrive.monitoring.lx3_monitoring import monitoring_enabled
from openpilot.selfdrive.monitoring.policy import DriverMonitoring
from openpilot.selfdrive.monitoring.wheel_monitord import WheelMonitoring, inputs_valid

AlertLevel = log.DriverMonitoringState.AlertLevel
Policy = log.DriverMonitoringState.MonitoringPolicy
EventName = log.OnroadEvent.EventName


def lx3_params():
  CP = car.CarParams.new_message(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', brand='hyundai',
                                steerControlType='angle', openpilotLongitudinalControl=True)
  CP.init('safetyConfigs', 1)[0].safetyParam = 2238
  CP.lateralTuning.init('pid')
  return CP


class TestWheelMonitoring(unittest.TestCase):
  def setUp(self):
    self.prefix = OpenpilotPrefix()
    self.prefix.__enter__()
    self.addCleanup(self.prefix.__exit__, None, None, None)
    self.params = Params()
    self.CP = lx3_params()
    self.CS = car.CarState.new_message(canValid=True, latEnabled=True, gearShifter='drive')
    self.SS = log.SelfdriveState.new_message()
    self.monitor = WheelMonitoring()
    self.now = 100.
    self.CS.lx3Authority = dict(version=4, profile=1, epoch=1, inputReady=True, allowed=1, lateralGeneration=1,
                                statusMonoTime=int(self.now * 1e9))
    self.monitor.update(self.CS, self.SS, self.now, True)

  def advance(self, seconds, dt=.05, valid=True):
    for _ in range(round(seconds / dt)):
      self.now += dt
      if self.CS.lx3Authority.version:
        self.CS.lx3Authority.statusMonoTime = int(self.now * 1e9)
      msg = self.monitor.update(self.CS, self.SS, self.now, valid)
    return msg

  def test_stock_deadlines_in_real_elapsed_time(self):
    for dt in (.05, .2):
      with self.subTest(dt=dt):
        self.monitor = WheelMonitoring()
        self.monitor.update(self.CS, self.SS, self.now, True)
        for duration, expected in ((14.8, AlertLevel.none), (.4, AlertLevel.one),
                                   (8.6, AlertLevel.one), (.4, AlertLevel.two),
                                   (5.6, AlertLevel.two), (.4, AlertLevel.three)):
          packet = self.advance(duration, dt)
          self.assertTrue(packet.valid)
          self.assertEqual(packet.driverMonitoringState.alertLevel, expected)
          self.assertEqual(packet.driverMonitoringState.activePolicy, Policy.wheeltouch)
          self.assertFalse(packet.driverMonitoringState.visionPolicyState.faceDetected)

  def test_real_interaction_resets_before_terminal_only(self):
    for field in ('steeringPressed', 'gasPressed'):
      self.advance(24.1)
      self.assertEqual(self.monitor.dm.alert_level, AlertLevel.two)
      setattr(self.CS, field, True)
      self.advance(.05)
      self.assertEqual(self.monitor.dm.awareness, 1.)
      setattr(self.CS, field, False)
    self.advance(30.1)
    self.CS.steeringPressed = True
    self.advance(.05)
    self.assertEqual(self.monitor.dm.alert_level, AlertLevel.three)
    self.assertLessEqual(self.monitor.dm.awareness, 0.)

  def test_new_touch_does_not_rewrite_time_before_observation(self):
    self.advance(29.95)
    self.CS.steeringPressed = True
    self.advance(.2, dt=.2)
    self.assertEqual(self.monitor.dm.alert_level, AlertLevel.three)
    self.assertEqual(self.monitor.dm.terminal_alert_cnt, 1)

  def test_standstill_and_manual_driving_do_not_lock_out(self):
    self.CS.standstill = True
    self.advance(60.)
    self.assertEqual(self.monitor.dm.alert_level, AlertLevel.none)
    self.assertGreater(self.monitor.dm.awareness, 0.)
    self.monitor = WheelMonitoring()
    self.CS.standstill = False
    self.CS.latEnabled = False
    self.CS.lx3Authority.allowed = 0
    self.CS.lx3Authority.lateralGeneration = 0
    self.monitor.update(self.CS, self.SS, self.now, True)
    self.advance(60.)
    self.assertEqual(self.monitor.dm.alert_level, AlertLevel.none)
    self.assertEqual(self.monitor.dm.awareness, 1.)
    self.assertFalse(self.monitor.dm.always_on)

  def test_fresh_permission_off_overrides_still_armed_host_latch(self):
    self.advance(24.1)
    self.CS.lx3Authority.allowed = 0
    self.CS.lx3Authority.lateralGeneration = 0
    self.CS.lx3Authority.armed = 3
    self.advance(60.)
    self.assertTrue(self.CS.latEnabled)
    self.assertEqual(self.monitor.dm.awareness, 1.)
    self.assertEqual(self.monitor.dm.terminal_alert_cnt, 0)

  def test_unverified_pending_lfa_never_starts_monitoring_session(self):
    self.monitor = WheelMonitoring()
    self.CS.lx3Authority = {}
    self.monitor.update(self.CS, self.SS, self.now, True)
    self.advance(60.)
    self.assertEqual(self.monitor.dm.awareness, 1.)

  def test_unknown_native_status_keeps_lfa_only_awareness(self):
    self.advance(24.1)
    before = self.monitor.dm.awareness
    self.CS.lx3Authority = {}
    self.assertFalse(self.SS.enabled)
    self.advance(.1)
    self.assertLess(self.monitor.dm.awareness, before)
    self.CS.latEnabled = False
    self.advance(.05)
    self.assertEqual(self.monitor.dm.awareness, 1.)

  def test_invalid_input_or_long_scheduler_gap_never_forgives(self):
    self.advance(24.1)
    before = self.monitor.dm.awareness
    self.CS.latEnabled = False  # Unknown input must not count as disengagement.
    packet = self.advance(.1, valid=False)
    self.assertFalse(packet.valid)
    self.assertEqual(self.monitor.dm.awareness, before)
    self.CS.latEnabled = True
    self.advance(.05)
    self.assertEqual(self.monitor.dm.awareness, before)
    packet = self.advance(.5, dt=.5)
    self.assertFalse(packet.valid)
    self.assertEqual(self.monitor.dm.awareness, before)
    self.assertFalse(self.monitor.update(self.CS, self.SS, self.now - 1., True).valid)

  def test_no_elapsed_time_cannot_recover_attention(self):
    self.advance(24.1)
    before = self.monitor.dm.awareness
    self.CS.steeringPressed = True
    for _ in range(100):
      self.monitor.update(self.CS, self.SS, self.now, True)
    self.assertEqual(self.monitor.dm.awareness, before)

  def test_terminal_count_and_duration_lockout(self):
    for i in range(3):
      self.advance(30.1)
      self.assertEqual(self.monitor.dm.terminal_alert_cnt, i + 1)
      self.CS.latEnabled = False
      self.CS.lx3Authority.allowed = 0
      self.CS.lx3Authority.lateralGeneration = 0
      self.advance(.05)
      self.CS.latEnabled = True
      self.CS.lx3Authority.allowed = 1
      self.CS.lx3Authority.lateralGeneration = 1
    self.assertTrue(self.monitor.dm.too_distracted)
    self.monitor = WheelMonitoring()
    self.monitor.update(self.CS, self.SS, self.now, True)
    self.advance(60.2)
    self.assertTrue(self.monitor.dm.too_distracted)

  def test_process_matrix_other_cars_and_missing_profile_unchanged(self):
    from openpilot.system.manager.process_config import enable_dm, enable_wheel_monitoring
    for fingerprint, profile in (('HYUNDAI_PALISADE_LX3_HEV', 2238), ('HYUNDAI_PALISADE_LX3_HEV', 190),
                                 ('KIA_EV9', 2238)):
      self.CP.carFingerprint = fingerprint
      self.CP.safetyConfigs[0].safetyParam = profile
      for mode in (0, 1, 2):
        for started in (False, True):
          with self.subTest(car=fingerprint, profile=profile, mode=mode, started=started):
            self.params.put_int('DisableDM', mode)
            self.assertEqual(enable_dm(started, self.params, self.CP), started and mode == 0)
            wheel = started and mode == 1 and fingerprint == 'HYUNDAI_PALISADE_LX3_HEV' and profile == 2238
            self.assertEqual(enable_wheel_monitoring(started, self.params, self.CP), wheel)
            self.assertEqual(monitoring_enabled(self.CP, mode), mode == 0 or (wheel and started) or
                             (not started and mode == 1 and fingerprint == 'HYUNDAI_PALISADE_LX3_HEV' and profile == 2238))

  def test_input_validity_requires_fresh_valid_physical_data(self):
    sm = messaging.SubMaster(['carState', 'selfdriveState'])
    sm.data['carState'] = self.CS
    sm.data['selfdriveState'] = self.SS
    for s in sm.services:
      sm.alive[s] = sm.valid[s] = True
      sm.logMonoTime[s] = int(self.now * 1e9)
    self.assertTrue(inputs_valid(sm, self.now))
    sm.alive['carState'] = False  # 100 ms generic threshold is not the monitor's 250 ms bound.
    self.assertTrue(inputs_valid(sm, self.now + .2))
    self.assertFalse(inputs_valid(sm, self.now + .251))
    self.assertFalse(inputs_valid(sm, self.now - .001))
    self.CS.canValid = False
    self.assertFalse(inputs_valid(sm, self.now))
    self.CS.canValid = True
    self.CS.canTimeout = True
    self.assertFalse(inputs_valid(sm, self.now))
    self.CS.canTimeout = False
    sm.valid['carState'] = False
    self.assertFalse(inputs_valid(sm, self.now))

  def selfdrived(self, mode=1):
    from openpilot.selfdrive.selfdrived.selfdrived import SelfdriveD
    self.params.put_int('DisableDM', mode)
    self.params.put_int('LongitudinalPersonality', 1)
    sd = SelfdriveD(self.CP)
    sd.initialized = True
    for s in sd.sm.services:
      sd.sm.alive[s] = sd.sm.valid[s] = sd.sm.freq_ok[s] = True
    sd.sm.data['controlsState'] = messaging.new_message('controlsState').controlsState
    sd.sm['controlsState'].lateralControlState.init('angleState')
    sd.sm.data['liveCalibration'] = messaging.new_message('liveCalibration').liveCalibration
    sd.sm['liveCalibration'].calStatus = log.LiveCalibrationData.Status.calibrated
    sd.sm.data['deviceState'] = messaging.new_message('deviceState').deviceState
    sd.sm['deviceState'].freeSpacePercent = 50
    # CAN events are separate fixtures: this test asserts actual monitoring
    # events without pretending that all other startup/vehicle inputs pass.
    self.CS.canValid = False
    return sd

  def test_actual_selfdrived_events_lockout_and_rate_path(self):
    sd = self.selfdrived()
    self.advance(30.1)
    sd.sm.data['driverMonitoringState'] = self.monitor.dm.get_state_packet().driverMonitoringState
    sd.update_events(self.CS)
    self.assertIn(EventName.driverUnresponsive3, sd.events.names)
    self.assertNotIn(EventName.lx3MonitoringRequired, sd.events.names)
    self.assertNotIn('driverMonitoringState', sd.sm.ignore_average_freq)
    self.monitor.dm.too_distracted = True
    sd.sm.data['driverMonitoringState'] = self.monitor.dm.get_state_packet().driverMonitoringState
    sd.update_events(self.CS)
    self.assertIn(EventName.tooDistracted, sd.events.names)
    self.assertTrue(self.params.get_bool('DriverTooDistracted'))
    self.assertTrue(DriverMonitoring().too_distracted)

  def test_missing_camera_no_silent_fallback_and_mode_change_latched(self):
    sd = self.selfdrived(mode=0)
    sd.sm.alive['driverMonitoringState'] = False
    sd.update_events(self.CS)
    self.assertIn(EventName.lx3MonitoringRequired, sd.events.names)
    # A mode change is not an escape from a monitoring fault mid-session.
    self.params.put_int('DisableDM', 1)
    sd.sm.alive['driverMonitoringState'] = True
    sd.sm.data['driverMonitoringState'] = self.monitor.dm.get_state_packet().driverMonitoringState
    sd.update_events(self.CS)
    self.assertTrue(sd.dm_mode_changed)
    self.assertIn(EventName.lx3MonitoringRequired, sd.events.names)
    self.params.put_int('DisableDM', 0)
    sd.update_events(self.CS)
    self.assertIn(EventName.lx3MonitoringRequired, sd.events.names)

  def test_wheel_mode_rejects_wrong_policy_and_invalid_data(self):
    sd = self.selfdrived()
    sd.sm.data['driverMonitoringState'] = messaging.new_message('driverMonitoringState').driverMonitoringState
    sd.sm['driverMonitoringState'].activePolicy = Policy.vision
    sd.update_events(self.CS)
    self.assertIn(EventName.lx3MonitoringRequired, sd.events.names)
    sd.sm.data['driverMonitoringState'] = self.monitor.dm.get_state_packet().driverMonitoringState
    sd.sm.valid['driverMonitoringState'] = False
    sd.update_events(self.CS)
    self.assertIn(EventName.lx3MonitoringRequired, sd.events.names)

  def test_actual_controls_publish_forces_decel_for_wheel_red(self):
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
    sent = {}
    controls.pm = SimpleNamespace(send=lambda key, message: sent.__setitem__(key, message.to_bytes()))
    self.advance(30.1)
    controls.sm.data['driverMonitoringState'] = self.monitor.dm.get_state_packet().driverMonitoringState
    for mode, expected in ((1, True), (0, True), (2, False)):
      self.params.put_int('DisableDM', mode)
      controls.publish(car.CarControl.new_message(), log.ControlsState.LateralAngleState.new_message())
      result = messaging.log_from_bytes(sent['controlsState'])
      self.assertEqual(result.controlsState.forceDecel, expected)

  def test_daemon_uses_real_ipc_without_driver_camera(self):
    self.params.put_int('DisableDM', 1)
    self.params.put('CarParams', self.CP.to_bytes())
    publisher = messaging.PubMaster(['carState', 'selfdriveState'])
    subscriber = messaging.SubMaster(['driverMonitoringState'])
    proc = subprocess.Popen([sys.executable, '-m', 'openpilot.selfdrive.monitoring.wheel_monitord'])
    try:
      deadline = time.monotonic() + 12.
      got_valid = False
      while time.monotonic() < deadline:
        for name, value in (('carState', self.CS), ('selfdriveState', self.SS)):
          msg = messaging.new_message(name, valid=True)
          setattr(msg, name, value)
          publisher.send(name, msg)
        subscriber.update(25)
        if subscriber.valid['driverMonitoringState']:
          got_valid = True
          break
      self.assertTrue(got_valid, 'Wheel daemon did not publish valid real IPC without driverStateV2')
      self.assertEqual(subscriber['driverMonitoringState'].activePolicy, Policy.wheeltouch)
      self.assertFalse(subscriber['driverMonitoringState'].visionPolicyState.faceDetected)
      # Removing actual vehicle input must invalidate the live publisher.
      deadline = time.monotonic() + 2.
      while time.monotonic() < deadline and subscriber.valid['driverMonitoringState']:
        subscriber.update(100)
      self.assertFalse(subscriber.valid['driverMonitoringState'])
      self.assertIsNone(proc.poll())
    finally:
      proc.terminate()
      proc.wait(timeout=5)


if __name__ == '__main__':
  unittest.main()
