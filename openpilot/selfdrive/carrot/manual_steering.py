"""Opt-in actuator suspension; never grants or revokes control permission."""
import math


class ManualSteeringPause:
  FADE_SECONDS = 1.0
  RESUME_SECONDS = 1.0
  RESUME_ANGLE_DEG = 15.0

  def __init__(self, dt):
    self.dt = dt
    self.paused = False
    self.clear_seconds = 0.0
    self.scale = 1.0

  def update(self, *, enabled, lane_change_off, left_blinker, right_blinker,
             steering_pressed, steering_angle, can_valid, can_timeout, lateral_active=True):
    signalling = left_blinker or right_blinker
    # Hazard lights alone do not start a manual lane-change handoff. If a
    # handoff already started, keep it until both signals have cleared.
    if enabled and lane_change_off and bool(left_blinker) != bool(right_blinker):
      self.paused = True

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
               math.isfinite(steering_angle) and abs(steering_angle) < self.RESUME_ANGLE_DEG)
      self.clear_seconds = self.clear_seconds + self.dt if clear else 0.0
      if self.clear_seconds >= self.RESUME_SECONDS:
        self.paused = False
        self.clear_seconds = 0.0
        self.scale = 1.0
    return self.paused
