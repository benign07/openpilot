"""Additional ceiling only: never raises stock authority or grants control."""
from math import isfinite

LX3_DRIVER_RELEASE_RATIO = 0.80
LX3_DRIVER_YIELD_RATIO = 0.90


class ManualSteeringCeiling:
  RESUME_SECONDS = 0.5

  def __init__(self, dt=0.01, *, resume_seconds=RESUME_SECONDS, recovery_maximum=None):
    self.previous = 0.0
    self.anchor = 0.0
    self.fading = False
    self.recovering = False
    self.dt = dt
    if not isfinite(resume_seconds) or resume_seconds <= 0:
      raise ValueError('Positive recovery duration required')
    self.resume_seconds = resume_seconds
    if recovery_maximum is not None and (not isfinite(recovery_maximum) or recovery_maximum <= 0):
      raise ValueError('Positive recovery ceiling required')
    self.recovery_maximum = recovery_maximum
    self.recovery_anchor = self.recovery_elapsed = 0.0

  def update(self, baseline, scale, active, resuming=False):
    scale = max(0.0, min(1.0, scale)) if isfinite(scale) else 0.0
    if not active:
      result = 0.0
      self.anchor = 0.0
      self.recovering = False
    elif resuming or (self.recovering and scale >= 1.0):
      # A partial fade can have an anchor below today's baseline. Continue
      # limiting from actual output even after the host's ramp flag clears.
      if self.recovery_maximum is None:
        result = min(baseline * scale, self.previous + baseline * self.dt / self.resume_seconds)
        self.recovering = result < baseline
      else:
        # One linear handback envelope, capped by the independently recovering
        # stock ceiling. Multiplying two ramps would extend the requested time.
        if not self.recovering:
          self.recovery_anchor = self.previous
          self.recovery_elapsed = 0.0
        self.recovery_elapsed += self.dt
        progress = min(1.0, self.recovery_elapsed / self.resume_seconds)
        target = self.recovery_anchor + (self.recovery_maximum-self.recovery_anchor)*progress
        result = min(baseline, target)
        self.recovering = progress < 1.0 or result < baseline
    elif scale < 1.0:
      self.recovering = False
      if not self.fading:
        self.anchor = self.previous
      result = min(baseline, self.previous, self.anchor * scale)
    else:
      result = baseline
      self.recovering = False
    self.fading = scale < 1.0 and not (resuming or self.recovering)
    self.previous = max(0.0, result)
    return self.previous
