"""Run the existing wheel-touch policy from actual vehicle inputs, without vision.

No driverStateV2 is generated. Output identifies the wheel-touch policy and
reports faceDetected=False. This observes interaction, not gaze or attention.
"""
import time

from cereal import car, log
import cereal.messaging as messaging
from openpilot.common.params import Params
from openpilot.common.realtime import DT_DMON, Ratekeeper
from openpilot.selfdrive.car.lx3_authority import LAT, STATUS_MAX_NS, verified
from openpilot.selfdrive.monitoring.lx3_monitoring import uses_wheel_monitoring
from openpilot.selfdrive.monitoring.policy import DriverMonitoring

MAX_INPUT_AGE = 0.25
MAX_STEP_GAP = 0.25


class WheelMonitoring:
  def __init__(self):
    # AlwaysOnDM assumes vision can recognize attentive manual driving. A
    # wheel-only monitor cannot; it monitors granted assistance sessions only.
    self.dm = DriverMonitoring(always_on=False)
    self.dm._set_policy(log.DriverMonitoringState.MonitoringPolicy.wheeltouch)
    self.last_time = None
    self.remainder = 0.
    self.previous = None
    self.lateral_active = False

  def update(self, CS, SS, now, valid):
    elapsed = 0. if self.last_time is None else now - self.last_time
    self.last_time = now
    if not valid or not 0. <= elapsed <= MAX_STEP_GAP:
      # Never count a missing input as disengagement or invent interaction.
      # Publishing invalid removes permission through the normal host path.
      self.remainder = 0.
      self.previous = None
      return self.dm.get_state_packet(valid=False)

    a = CS.lx3Authority
    if verified(a) and 0 <= now * 1e9 - a.statusMonoTime <= STATUS_MAX_NS:
      self.lateral_active = bool(a.allowed & LAT)
    elif not CS.latEnabled:
      self.lateral_active = False
    # Unknown status preserves a previously observed grant, never creates one.
    state = {
      'driver_engaged': CS.steeringPressed or CS.gasPressed,
      'op_engaged': SS.enabled or self.lateral_active,
      'standstill': CS.standstill,
      'wrong_gear': str(CS.gearShifter) not in ('drive', 'sport', 'manumatic', 'eco', 'low'),
    }
    if self.previous is None:
      self.previous = state
      elapsed = 0.
    self.remainder += elapsed
    steps = int((self.remainder + 1e-9) / DT_DMON)
    self.remainder = max(0., self.remainder - steps * DT_DMON)
    for i in range(steps):
      # A newly observed touch must not retroactively erase a terminal alert
      # that was reached during a short scheduler delay.
      self.dm._update_events(**(state if i == steps - 1 else self.previous))
    self.previous = state
    return self.dm.get_state_packet()


def inputs_valid(sm, now):
  # Own bounded freshness check: SubMaster's 100 Hz alive threshold is 100 ms.
  # Brief delivery jitter must not reset the policy; hard loss remains invalid.
  return (all(sm.valid[s] and 0. < sm.logMonoTime[s] and 0. <= now - sm.logMonoTime[s] / 1e9 <= MAX_INPUT_AGE
              for s in ('carState', 'selfdriveState')) and
          sm['carState'].canValid and not sm['carState'].canTimeout)


def main():
  params = Params()
  CP = messaging.log_from_bytes(params.get('CarParams', block=True), car.CarParams)
  if not uses_wheel_monitoring(CP, params.get_int('DisableDM')):
    return
  sm = messaging.SubMaster(['carState', 'selfdriveState'])
  pm = messaging.PubMaster(['driverMonitoringState'])
  monitor = WheelMonitoring()
  rk = Ratekeeper(1 / DT_DMON, print_delay_threshold=None)
  mode_changed = False
  while True:
    sm.update(0)
    now = time.monotonic()
    mode_changed |= params.get_int('DisableDM') != 1
    dat = monitor.update(sm['carState'], sm['selfdriveState'], now, not mode_changed and inputs_valid(sm, now))
    pm.send('driverMonitoringState', dat)
    rk.keep_time()


if __name__ == '__main__':
  main()
