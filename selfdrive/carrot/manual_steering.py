"""Opt-in actuator suspension; never grants or revokes control permission."""
import math


class ManualSteeringPause:
  FADE_SECONDS = 1.0
  CLEAR_SECONDS = 0.5
  RESUME_SECONDS = 0.5
  RESUME_ANGLE_DEG = 15.0

  def __init__(self, dt, *, quick_recovery=False):
    self.dt = dt
    self.quick_recovery = quick_recovery
    self.resume_seconds = .2 if quick_recovery else self.RESUME_SECONDS
    self.paused = False
    self.clear_seconds = 0.0
    self.scale = 1.0
    self.resuming = False
    self.curve_quiet_seconds = 0.0

  def update(self, *, enabled, lane_change_off, left_blinker, right_blinker,
             steering_pressed, steering_angle, can_valid, can_timeout, lateral_active=True, explicit_resume=False,
             speed=0.0, steering_rate=float('nan')):
    signalling = left_blinker or right_blinker
    if self.quick_recovery and not lateral_active and not self.paused:
      # A real permission loss ends the old handback. A later activation uses
      # the normal initial ramp, not a stale .2s resume flag.
      self.resuming=False
      self.scale=1.0
    quiet_curve = (self.quick_recovery and can_valid and not can_timeout and lateral_active and
                   math.isfinite(speed) and speed>=8.0 and
                   math.isfinite(steering_rate) and abs(steering_rate)<=12.0 and
                   math.isfinite(steering_angle) and abs(steering_angle)<85.0)
    self.curve_quiet_seconds = self.curve_quiet_seconds+self.dt if quiet_curve else 0.0
    angle_clear = (math.isfinite(steering_angle) and (abs(steering_angle)<self.RESUME_ANGLE_DEG or
                   (quiet_curve and self.curve_quiet_seconds>=.2)))
    if (explicit_resume and not signalling and self.paused and lateral_active and can_valid and not can_timeout
        and math.isfinite(steering_angle) and (not self.quick_recovery or (not steering_pressed and angle_clear))):
      self.paused = False
      self.clear_seconds = 0.0
      self.resuming = True
    # Hazard lights alone do not start a manual lane-change handoff. If a
    # handoff already started, keep it until both signals have cleared.
    if enabled and lane_change_off and bool(left_blinker) != bool(right_blinker):
      self.paused = True
      self.resuming = False
    if self.quick_recovery and self.resuming and steering_pressed:
      self.paused = True
      self.resuming = False
      self.clear_seconds = 0.0

    # Retain a started handoff across setting/mode changes and signal changes.
    # Neither disabling this option nor switching sides can abruptly hand back.
    if self.paused:
      # Only an already permitted actuator may fade. A cancellation, fault,
      # invalid input, or an inactive start never delays a normal stop and
      # cannot turn a later re-grant into assistance during this handoff.
      if not lateral_active or not can_valid or can_timeout:
        self.scale = 0.0
      else:
        self.scale = max(0.0, self.scale - self.dt / self.FADE_SECONDS)
      clear = (not signalling and not steering_pressed and can_valid and not can_timeout and
               angle_clear)
      self.clear_seconds = self.clear_seconds + self.dt if clear else 0.0
      if clear and self.clear_seconds >= (0.0 if self.quick_recovery else self.CLEAR_SECONDS):
        self.paused = False
        self.clear_seconds = 0.0
        # Automatic return must use the same gradual envelope as a newly
        # accepted physical resume. Preserve the actual partial/zero level.
        self.resuming = True
        if self.quick_recovery and lateral_active:
          self.scale = min(1.0, self.scale + self.dt / self.resume_seconds)
          self.resuming = self.scale < 1.0
    elif self.resuming:
      if not lateral_active or not can_valid or can_timeout:
        self.scale = 0.0
      else:
        self.scale = min(1.0, self.scale + self.dt / self.resume_seconds)
        self.resuming = self.scale < 1.0
    return self.paused
