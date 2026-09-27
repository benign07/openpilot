from openpilot.common.realtime import DT_CTRL
from openpilot.common.params import Params

class CarrotControls:
  def __init__(self, CP):
    self.CP = CP
    self.params = Params()
    self.lat_suspend_active = False
    self.lat_suspend_enter_t = 0.0
    self.lat_suspend_hold_t = 0.0
    self.manual_blinker_suspended = False
    self.manual_blinker_release_t = 0.0

  def lat_suspend_control(self, CS, latActive):
    suspend_angle = float(self.params.get_int("LatSuspendAngleDeg"))
    resume_angle  = 15
    delay_sec     = 1.0
    hold_sec      = 0.5

    # Negative LaneChangeNeedTorque disables blinker-triggered model lane
    # changes. On LX3, also yield centering to the driver's manual maneuver.
    manual_lane_change = (self.CP.carFingerprint == "HYUNDAI_PALISADE_LX3_HEV" and
                          self.params.get_int("LaneChangeNeedTorque") < 0)
    if not latActive or not manual_lane_change:
      self.manual_blinker_suspended = False
      self.manual_blinker_release_t = 0.0
    elif CS.leftBlinker != CS.rightBlinker:
      self.manual_blinker_suspended = True
      self.manual_blinker_release_t = 0.0
    elif self.manual_blinker_suspended:
      if CS.leftBlinker or CS.rightBlinker or CS.steeringPressed or abs(CS.steeringAngleDeg) >= resume_angle:
        self.manual_blinker_release_t = 0.0
      else:
        self.manual_blinker_release_t += DT_CTRL
        if self.manual_blinker_release_t >= hold_sec:
          self.manual_blinker_suspended = False
          self.manual_blinker_release_t = 0.0

    # 1) enter condition timer
    enter_cond = CS.steeringPressed and abs(CS.steeringAngleDeg) > suspend_angle
    if not self.lat_suspend_active:
      if enter_cond:
        self.lat_suspend_enter_t += DT_CTRL
        if self.lat_suspend_enter_t >= delay_sec:
          self.lat_suspend_active = True
          self.lat_suspend_hold_t = 0.0
      else:
        self.lat_suspend_enter_t = 0.0

    # 2) while suspended: enforce minimum hold time + hysteresis exit
    if self.lat_suspend_active:
      self.lat_suspend_hold_t += DT_CTRL

      exit_cond = (abs(CS.steeringAngleDeg) < resume_angle) and (not CS.steeringPressed)
      if (self.lat_suspend_hold_t >= hold_sec) and exit_cond:
        self.lat_suspend_active = False
        self.lat_suspend_enter_t = 0.0

    if self.lat_suspend_active or self.manual_blinker_suspended:
      latActive = False
    return latActive
