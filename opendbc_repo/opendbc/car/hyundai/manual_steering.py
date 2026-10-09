"""Additional ceiling only: never raises stock authority or grants control."""
from math import isfinite


class ManualSteeringCeiling:
  def __init__(self):
    self.previous = 0.0
    self.anchor = 0.0
    self.fading = False

  def update(self, baseline, scale, active):
    scale = max(0.0, min(1.0, scale)) if isfinite(scale) else 0.0
    if not active:
      result = 0.0
      self.anchor = 0.0
    elif scale < 1.0:
      if not self.fading:
        self.anchor = self.previous
      # Capture the last output, not a recovering stock maximum. A stronger
      # driver handover may reduce it faster; never undo that reduction.
      result = min(baseline, self.previous, self.anchor * scale)
    else:
      result = baseline
    self.fading = scale < 1.0
    self.previous = max(0.0, result)
    return self.previous
