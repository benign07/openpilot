"""Driver intent for LX3. Actuator permission belongs to StateMachine/Panda.

This module must never infer engagement from an icon, stock ACC acknowledgement,
an automatic cruise request, or the previously transmitted actuator command.
"""
from enum import IntEnum


class EngagementMode(IntEnum):
  OFF = 0
  LATERAL = 1
  COMBINED = 2


class Lx3Engagement:
  def __init__(self):
    self.mode = EngagementMode.OFF

  def request(self, buttons):
    """Return a candidate mode and whether there was an explicit request.

    Cancel wins over every enable in a single CAN batch. Main toggles combined
    control; LFA toggles steering assistance (turning it off ends this session).
    A new session requires a new physical release edge after any fault/disable.
    """
    buttons = tuple(buttons)
    released = {str(b.type) for b in buttons if not b.pressed}
    if any(str(b.type) == 'cancel' for b in buttons):
      return EngagementMode.OFF, True
    if 'mainCruise' in released:
      return (EngagementMode.OFF if self.mode == EngagementMode.COMBINED else EngagementMode.COMBINED), True
    if 'lfaButton' in released:
      return (EngagementMode.LATERAL if self.mode == EngagementMode.OFF else EngagementMode.OFF), True
    if released & {'accelCruise', 'decelCruise'}:
      return EngagementMode.COMBINED, True
    return self.mode, False

  def commit(self, candidate, enabled):
    # A denied enable must not become a latched request which restarts later.
    self.mode = candidate if enabled else EngagementMode.OFF


def lx3_control_permissions(mode, enabled, active, inputs_valid, driving_gear, steer_ok):
  """Separate whole-session engagement from longitudinal permission.

  No AlwaysLateral fallback is allowed. Speed/blinker override may suspend
  lateral actuation later without erasing the accepted driver session.
  """
  session = enabled and inputs_valid and driving_gear and steer_ok
  lateral = session and active and mode in (EngagementMode.LATERAL, EngagementMode.COMBINED)
  longitudinal = session and active and mode == EngagementMode.COMBINED
  return lateral, longitudinal


def lx3_pandas_ready(pandas, configs, alternative_experience):
  """No startup grace or extra active Panda for the new engagement path."""
  ignored = {'silent', 'noOutput'}
  if not configs or len(pandas) < len(configs):
    return False
  active_count = 0
  for i, panda in enumerate(pandas):
    if i >= len(configs):
      if str(panda.safetyModel) not in ignored:
        return False
      continue
    cfg = configs[i]
    if (panda.safetyModel != cfg.safetyModel or panda.safetyParam != cfg.safetyParam or
        panda.alternativeExperience != alternative_experience or panda.safetyRxChecksInvalid or panda.faults):
      return False
    if str(panda.safetyModel) not in ignored:
      active_count += 1
      if not panda.controlsAllowed:
        return False
  return active_count > 0
