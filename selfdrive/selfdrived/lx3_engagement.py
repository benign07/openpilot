"""Driver intent for LX3. Actuator permission belongs to StateMachine/Panda.

This module must never infer engagement from an icon, stock ACC acknowledgement,
an automatic cruise request, or the previously transmitted actuator command.
"""
from enum import IntEnum

# Panda safety_declarations.h: ALT_EXP_DISABLE_DISENGAGE_ON_GAS.
LX3_DISABLE_DISENGAGE_ON_GAS = 1


def lx3_alternative_experience(disengage_on_accelerator):
  return 0 if disengage_on_accelerator else LX3_DISABLE_DISENGAGE_ON_GAS


def lx3_disengage_on_gas(alternative_experience):
  # Use the acknowledged startup policy, rather than a live setting that has
  # not been sent to Panda. A change takes effect on the next control startup.
  return not bool(alternative_experience & LX3_DISABLE_DISENGAGE_ON_GAS)


class EngagementMode(IntEnum):
  OFF = 0
  LATERAL = 1
  COMBINED = 2


class Lx3Engagement:
  def __init__(self):
    self.mode = EngagementMode.OFF
    self.pending = None
    self.pending_since = 0.0

  def request(self, buttons, now):
    """Return a candidate mode and whether there was an explicit request.

    Cancel wins over every enable in a single CAN batch. Main toggles combined
    control; LFA toggles steering assistance (turning it off ends this session).
    A new session requires a new physical release edge after any fault/disable.
    """
    buttons = tuple(buttons)
    released = {str(b.type) for b in buttons if not b.pressed}
    current = self.pending if self.pending is not None else self.mode
    candidate = None
    if any(str(b.type) == 'cancel' for b in buttons):
      candidate = EngagementMode.OFF
    elif 'mainCruise' in released:
      candidate = EngagementMode.OFF if current == EngagementMode.COMBINED else EngagementMode.COMBINED
    elif 'lfaButton' in released:
      candidate = EngagementMode.LATERAL if current == EngagementMode.OFF else EngagementMode.OFF
    elif released & {'accelCruise', 'decelCruise'}:
      candidate = EngagementMode.COMBINED
    if candidate is not None:
      self.pending = candidate if candidate != EngagementMode.OFF else None
      self.pending_since = now
      return candidate, True
    if self.pending is not None:
      if 0 <= now - self.pending_since < 0.5:
        return self.pending, True
      self.pending = None
    return self.mode, False

  def commit(self, candidate, enabled):
    # A denied enable must not become a latched request which restarts later.
    self.mode = candidate if enabled else EngagementMode.OFF
    self.pending = None


def lx3_control_permissions(mode, enabled, active, inputs_valid, driving_gear, steer_ok):
  """Separate whole-session engagement from longitudinal permission.

  No AlwaysLateral fallback is allowed. Speed/blinker override may suspend
  lateral actuation later without erasing the accepted driver session.
  """
  session = enabled and inputs_valid and driving_gear and steer_ok
  lateral = session and active and mode in (EngagementMode.LATERAL, EngagementMode.COMBINED)
  longitudinal = session and active and mode == EngagementMode.COMBINED
  return lateral, longitudinal


def lx3_pandas_ready(pandas, configs, alternative_experience, require_controls=True):
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
      if require_controls and not panda.controlsAllowed:
        return False
  return active_count > 0
