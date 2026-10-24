"""Optional routine notification pulses through the existing CCNC VIBRATE bit.

No steering torque/angle synthesis. Physical haptic response is not qualified.
Critical acoustic alerts are independent and are not silenced here.
The owner confirmed an engage vibration on the original LX3 on 2026-10-24.
Other events and the optional split patterns still require vehicle validation.
VIBRATE is binary: these settings do not claim to control motor strength.
"""

# (parameter, default, event names, Korean menu title). Defaults preserve the
# previous event set; the existing master switch remains the opt-in gate.
HAPTIC_EVENT_OPTIONS = (
  ('C3xLiteHapticEngage', True, ('engage', 'engage2', 'longEngaged'), '인게이지·제어 시작'),
  ('C3xLiteHapticTraffic', True, ('trafficSignGreen', 'trafficSignChanged'), '신호 변경 알림'),
  ('C3xLiteHapticLaneChange', True, ('laneChange',), '차선 변경 알림'),
  ('C3xLiteHapticStopping', True, ('stopping', 'autoHold'), '정지·오토홀드 알림'),
  ('C3xLiteHapticNavigation', True, ('audioTurn',), '길 안내 알림'),
  ('C3xLiteHapticReverse', True, ('reverseGear',), '후진 알림'),
)
HAPTIC_PATTERN_KEY = 'C3xLiteHapticPattern'


def haptic_param_declarations():
  return ''.join('    {"%s", {PERSISTENT, BOOL, "%d"}},\n' % (key, default)
                 for key, default, _, _ in HAPTIC_EVENT_OPTIONS) + (
    '    {"C3xLiteHapticPattern", {PERSISTENT, INT, "0"}},\n')


class HapticNotifications:
  PULSE_FRAMES = 80  # 0.8 s at the existing 100 Hz controller rate
  COOLDOWN_FRAMES = 100
  ROUTINE = frozenset(('engage', 'longEngaged', 'trafficSignGreen', 'trafficSignChanged',
                       'laneChange', 'stopping', 'autoHold', 'engage2', 'audioTurn', 'reverseGear'))
  # All patterns end at 0.8 s. This is pulse timing, NOT motor strength.
  PATTERNS = (((0, 80),), ((0, 30), (50, 80)), ((0, 20), (30, 50), (60, 80)))

  def __init__(self):
    self.previous = 'none'
    self.end_frame = -1
    self.last_frame = -1000
    self.last_update_frame = -1
    self.enabled_events = self.ROUTINE
    self.pattern = 0
    self.active_pattern = self.PATTERNS[0]

  def configure(self, enabled_events=None, pattern=0):
    enabled = self.ROUTINE if enabled_events is None else frozenset(enabled_events) & self.ROUTINE
    # Unknown persisted values use the existing single pulse.
    pattern = pattern if type(pattern) is int and 0 <= pattern < len(self.PATTERNS) else 0
    if enabled != self.enabled_events or pattern != self.pattern:
      self.end_frame = -1  # no continuation/replay after a settings change
    self.enabled_events, self.pattern = enabled, pattern

  def configure_from_params(self, params):
    # Called at the controller's existing low-rate settings refresh only.
    # Original 317-key native modules keep old behavior until the paired native
    # registry is installed. No control permission is granted by these options.
    enabled = set()
    for key, default, names, _ in HAPTIC_EVENT_OPTIONS:
      if (params.get_bool(key) if params.check_key(key) else default):
        enabled.update(names)
    pattern = params.get_int(HAPTIC_PATTERN_KEY) if params.check_key(HAPTIC_PATTERN_KEY) else 0
    self.configure(enabled, pattern)

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
    if (name != self.previous and name in self.enabled_events and frame >= self.end_frame and
        frame - self.last_frame >= self.COOLDOWN_FRAMES):
      self.end_frame = frame + self.PULSE_FRAMES
      self.last_frame = frame
      self.active_pattern = self.PATTERNS[self.pattern]
    elif name != 'none' and name not in self.ROUTINE:
      self.end_frame = -1
    self.previous = name
    return frame < self.end_frame and any(start <= frame-self.last_frame < end
                                         for start, end in self.active_pattern)
