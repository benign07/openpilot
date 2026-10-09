from openpilot.common.realtime import DT_CTRL
from openpilot.common.params import Params
from openpilot.selfdrive.carrot.manual_steering import ManualSteeringPause
from opendbc.car.hyundai.lx3_buttons import uses_lx3_authority

class CarrotControls:
  def __init__(self, CP):
    self.CP = CP
    self.params = Params()
    self.lat_suspend_active = False
    self.lat_suspend_enter_t = 0.0
    self.lat_suspend_hold_t = 0.0
    self.manual_steering = ManualSteeringPause(DT_CTRL)
    self.manual_params_frame = 0
    self.manual_steering_enabled = False
    self.manual_lane_change_off = False
    self.manual_supported = uses_lx3_authority(CP)

  def lat_suspend_control(self, CS, latActive):
    if self.manual_params_frame % 100 == 0:
      self.manual_steering_enabled = self.params.get_bool("ManualSteerWithBlinker")
      self.manual_lane_change_off = self.params.get_int("LaneChangeNeedTorque") < 0
    self.manual_params_frame += 1
    manual_pause = self.manual_steering.update(
      enabled=self.manual_supported and self.manual_steering_enabled, lane_change_off=self.manual_lane_change_off,
      left_blinker=CS.leftBlinker, right_blinker=CS.rightBlinker,
      steering_pressed=CS.steeringPressed, steering_angle=CS.steeringAngleDeg,
      can_valid=CS.canValid, can_timeout=CS.canTimeout, lateral_active=latActive,
    )
    suspend_angle = float(self.params.get_int("LatSuspendAngleDeg"))
    resume_angle  = 15
    delay_sec     = 1.0
    hold_sec      = 0.5

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

    if self.lat_suspend_active or (manual_pause and self.manual_steering.scale <= 0.0):
      latActive = False
    return latActive
