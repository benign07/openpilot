"""Optional routine notification pulses through the existing CCNC VIBRATE bit.

No steering torque/angle synthesis. Physical haptic response is not qualified.
Critical acoustic alerts are independent and are not silenced here.
"""
class HapticNotifications:
  ROUTINE = frozenset(('engage', 'longEngaged', 'trafficSignGreen', 'trafficSignChanged',
                       'laneChange', 'stopping', 'autoHold', 'engage2', 'audioTurn', 'reverseGear'))

  def __init__(self):
    self.previous = 'none'
    self.end_frame = -1
    self.last_frame = -1000

  def update(self, alert, *, eligible, frame):
    name = str(alert)
    if not eligible:
      self.previous = 'none'
      self.end_frame = -1
      return False
    if name != self.previous and name in self.ROUTINE and frame - self.last_frame >= 100:
      self.end_frame = frame + 20  # 0.2 s at the existing 100 Hz controller rate
      self.last_frame = frame
    elif name not in self.ROUTINE:
      self.end_frame = -1
    self.previous = name
    return frame < self.end_frame
