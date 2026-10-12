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
    self.manual_grant_identity = None

  def lat_suspend_control(self, CS, latActive, explicit_resume_supported=True):
    if self.manual_params_frame % 100 == 0:
      self.manual_steering_enabled = self.params.get_bool("ManualSteerWithBlinker")
      self.manual_lane_change_off = self.params.get_int("LaneChangeNeedTorque") < 0
    self.manual_params_frame += 1
    explicit_resume = False
    if self.manual_supported and explicit_resume_supported:
      auth = CS.lx3Authority
      identity = (int(auth.epoch), int(auth.lateralGeneration), int(auth.longitudinalGeneration))
      previous = self.manual_grant_identity
      # A normal accepted physical request, not a latActive rising edge (which
      # can also be caused by speed/fault recovery). A reboot cannot resume it.
      if previous is not None and previous[0] == identity[0] and identity[0] != 0:
        new_lat = identity[1] != previous[1] and identity[1] != 0 and auth.lateralDecisionKey != 0
        new_long = (identity[2] != previous[2] and identity[2] != 0 and auth.longitudinalDecisionKey != 0
                    and (int(auth.allowed) & 2) and (int(auth.armed) & 2)
                    and not (CS.leftBlinker or CS.rightBlinker))
        # RES/SET can refresh both native generations. While a signal remains
        # on it is not proof that the driver has finished a manual lane change.
        explicit_resume = bool(latActive and not (CS.leftBlinker or CS.rightBlinker)
                               and (int(auth.allowed) & 1) and (int(auth.armed) & 1) and (new_lat or new_long))
      self.manual_grant_identity = identity
    manual_pause = self.manual_steering.update(
      enabled=self.manual_supported and self.manual_steering_enabled, lane_change_off=self.manual_lane_change_off,
      left_blinker=CS.leftBlinker, right_blinker=CS.rightBlinker,
      steering_pressed=CS.steeringPressed, steering_angle=CS.steeringAngleDeg,
      can_valid=CS.canValid, can_timeout=CS.canTimeout, lateral_active=latActive,
      explicit_resume=explicit_resume,
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
