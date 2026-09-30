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
    self.pending_counter = 0
    self.pending_generation = 0
    self.accepted_counter = 0
    self.accepted_generation = 0
    self.pending_epoch = 0
    self.accepted_epoch = 0
    self.rejection = None
    self.unbound_rejection = None
    self.clear_ack()

  def clear_ack(self):
    self.ack_mode = EngagementMode.OFF
    self.ack_generation = 0
    self.ack_counter = 0
    self.ack_valid = False
    self.ack_epoch = 0

  def begin_step(self, now):
    self.clear_ack()
    # Keep a rejection long enough for the 10Hz transport to observe it. An old
    # rejection cannot consume a different generation's newly pending request.
    if self.rejection is not None:
      generation, counter, stamp, epoch = self.rejection
      if 0 <= now - stamp < 0.5:
        self.set_ack(EngagementMode.OFF, generation, counter, epoch)
      else:
        self.rejection = None
    if self.unbound_rejection is not None and not 0 <= now - self.unbound_rejection[2] < 0.5:
      self.unbound_rejection = None

  def set_ack(self, mode, generation, counter, epoch=None):
    self.ack_mode, self.ack_generation, self.ack_counter = mode, generation, counter
    self.ack_valid = generation != 0
    self.ack_epoch = self.pending_epoch if epoch is None else epoch

  def reject(self, now):
    if self.pending_generation != 0:
      self.rejection = self.pending_generation, self.pending_counter, now, self.pending_epoch
      self.set_ack(EngagementMode.OFF, self.pending_generation, self.pending_counter)
      self.unbound_rejection = None
    elif self.pending is not None:
      # The physical CAN can precede the companion. Remember refusal identity
      # without latching an enable; a later matching pending is rejected only.
      self.unbound_rejection = self.pending, self.pending_counter, now
    self.mode = EngagementMode.OFF
    self.accepted_generation = 0
    self.accepted_counter = 0
    self.pending = None
    self.pending_generation = 0
    self.pending_epoch = 0
    self.accepted_epoch = 0

  def observe_rejection(self, panda, now):
    if self.unbound_rejection is None or panda is None:
      return False
    mode, counter, stamp = self.unbound_rejection
    if (0 <= now - stamp < 0.5 and panda.lx3PermissionPhase == 1 and panda.lx3RequestAgeMs < 500 and
        panda.lx3RequestedMode == int(mode) and panda.lx3PhysicalCounter == counter):
      self.rejection = panda.lx3RequestGeneration, counter, now, panda.lx3TransportEpoch
      self.set_ack(EngagementMode.OFF, panda.lx3RequestGeneration, counter, panda.lx3TransportEpoch)
      self.unbound_rejection = None
      if self.pending == mode and self.pending_counter == counter:
        # A replayed/default producer must not overwrite a retained refusal of
        # this same physical gesture with an enable ACK in the same frame.
        self.pending_generation = panda.lx3RequestGeneration
        self.pending_epoch = panda.lx3TransportEpoch
        self.reject(now)
        return True
    return False

  def request(self, buttons, now):
    """Return a candidate mode and whether there was an explicit request.

    Cancel wins over every enable in a single CAN batch. Main toggles combined
    control; LFA toggles steering assistance (turning it off ends this session).
    A new session requires a new physical release edge after any fault/disable.
    """
    buttons = tuple(buttons)
    expired = self.pending is not None and not 0 <= now - self.pending_since < 0.5
    if expired:
      self.reject(now)
    if any(str(b.type) == 'cancel' for b in buttons):
      self.reject(now)
      return EngagementMode.OFF, True
    candidate = None
    current = self.mode
    for b in buttons:
      if b.pressed or not getattr(b, 'lx3PhysicalValid', False):
        continue
      name = str(b.type)
      if name == 'mainCruise':
        candidate = EngagementMode.OFF if self.pending is not None or current == EngagementMode.COMBINED else EngagementMode.COMBINED
      elif name == 'lfaButton':
        candidate = EngagementMode.OFF if self.pending is not None or current != EngagementMode.OFF else EngagementMode.LATERAL
      elif name in ('accelCruise', 'decelCruise'):
        if self.pending is not None or current == EngagementMode.COMBINED:
          continue  # Speed adjustment is not another permission transaction.
        candidate = EngagementMode.COMBINED
      else:
        continue
      current = candidate
      if candidate == EngagementMode.OFF:
        self.reject(now)
      else:
        self.pending = candidate
        self.pending_counter = int(b.lx3PhysicalCounter)
        self.pending_generation = 0
        self.pending_epoch = 0
        self.pending_since = now
    if candidate is not None:
      return candidate, True
    if self.pending is not None:
      if 0 <= now - self.pending_since < 0.5:
        return self.pending, True
      self.reject(now)
      return EngagementMode.OFF, True
    return self.mode, expired

  def commit(self, candidate, enabled):
    # A denied enable must not become a latched request which restarts later.
    self.mode = candidate if enabled else EngagementMode.OFF
    self.accepted_generation = self.pending_generation if enabled else 0
    self.accepted_counter = self.pending_counter if enabled else 0
    self.accepted_epoch = self.pending_epoch if enabled else 0
    self.pending = None
    self.pending_generation = 0
    self.pending_epoch = 0
    self.rejection = None
    self.unbound_rejection = None
    self.clear_ack()


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
      if require_controls and not getattr(panda, 'lx3ControlsAllowed', False):
        return False
  return active_count == 1 and lx3_permission_sample(pandas) is not None


def lx3_permission_sample(pandas):
  """Mirror the shared companion decoder's semantic contract, not health bits."""
  active = [p for p in pandas if str(p.safetyModel) not in ('silent', 'noOutput')]
  if len(active) != 1:
    return None
  p = active[0]
  if (getattr(p, 'lx3PermissionVersion', 0) != 2 or not 1 <= p.lx3RequestGeneration <= 65535
      or not 1 <= getattr(p, 'lx3TransportEpoch', 0) <= 2**64 - 1):
    return None
  requested, accepted, phase = p.lx3RequestedMode, p.lx3AcceptedMode, p.lx3PermissionPhase
  if requested not in (0, 1, 2) or accepted not in (0, 1, 2) or phase not in (0, 1, 2):
    return None
  if not 0 <= p.lx3PhysicalCounter <= 255 or not 0 <= p.lx3RequestAgeMs <= 65535:
    return None
  idle = phase == 0 and requested == accepted == 0 and not p.lx3ControlsAllowed
  pending = phase == 1 and requested != 0 and accepted == 0 and not p.lx3ControlsAllowed and p.lx3RequestAgeMs < 500
  enabled = phase == 2 and requested == accepted != 0 and p.lx3ControlsAllowed
  return p if idle or pending or enabled else None


def lx3_permission_matches(panda, mode, generation, counter, accepted=False, epoch=None):
  if panda is None or generation == 0:
    return False
  return ((epoch is None or panda.lx3TransportEpoch == epoch) and
          panda.lx3RequestGeneration == generation and panda.lx3PhysicalCounter == counter and
          panda.lx3RequestedMode == int(mode) and
          (panda.lx3PermissionPhase == 2 and panda.lx3ControlsAllowed and panda.lx3AcceptedMode == int(mode)
           if accepted else panda.lx3PermissionPhase == 1 and not panda.lx3ControlsAllowed))
