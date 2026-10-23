"""Optional routine notification pulses through the existing CCNC VIBRATE bit.

No steering torque/angle synthesis. Physical haptic response is not qualified.
Critical acoustic alerts are independent and are not silenced here.
The requested pulse follows the observed ~0.8 s OEM duration, but this
observation does not establish that duration alone makes the motor respond.
"""
class HapticNotifications:
  PULSE_FRAMES = 80  # 0.8 s at the existing 100 Hz controller rate
  COOLDOWN_FRAMES = 100
  ROUTINE = frozenset(('engage', 'longEngaged', 'trafficSignGreen', 'trafficSignChanged',
                       'laneChange', 'stopping', 'autoHold', 'engage2', 'audioTurn', 'reverseGear'))

  def __init__(self):
    self.previous = 'none'
    self.end_frame = -1
    self.last_frame = -1000
    self.last_update_frame = -1

  def update(self, alert, *, eligible, frame):
    name = str(alert)
    if frame <= self.last_update_frame:
      # An out-of-order/restarted clock must not replay an old notification.
      self.previous = name
      self.end_frame = -1
      self.last_frame = frame
      self.last_update_frame = frame
      return False
    self.last_update_frame = frame
    if not eligible:
      # Consume the event while disabled; eligibility recovery is not a new
      # audible event and must not replay an already held alert.
      self.previous = name
      self.end_frame = -1
      return False
    if (name != self.previous and name in self.ROUTINE and frame >= self.end_frame and
        frame - self.last_frame >= self.COOLDOWN_FRAMES):
      self.end_frame = frame + self.PULSE_FRAMES
      self.last_frame = frame
    elif name != 'none' and name not in self.ROUTINE:
      self.end_frame = -1
    self.previous = name
    return frame < self.end_frame
