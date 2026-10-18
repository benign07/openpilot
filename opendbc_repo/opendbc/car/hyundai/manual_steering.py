"""Additional ceiling only: never raises stock authority or grants control."""
from math import isfinite


class ManualSteeringCeiling:
  RESUME_SECONDS = 0.5

  def __init__(self, dt=0.01):
    self.previous = 0.0
    self.anchor = 0.0
    self.fading = False
    self.recovering = False
    self.dt = dt

  def update(self, baseline, scale, active, resuming=False):
    scale = max(0.0, min(1.0, scale)) if isfinite(scale) else 0.0
    if not active:
      result = 0.0
      self.anchor = 0.0
      self.recovering = False
    elif resuming or (self.recovering and scale >= 1.0):
      # A partial fade can have an anchor below today's baseline. Continue
      # limiting from actual output even after the host's ramp flag clears.
      result = min(baseline * scale, self.previous + baseline * self.dt / self.RESUME_SECONDS)
      self.recovering = result < baseline
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
