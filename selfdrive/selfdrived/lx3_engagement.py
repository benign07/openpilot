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


def lx3_input_ready(cs):
  # Default/missing fields from an old producer must not enable the LX3 path.
  return (str(getattr(cs, 'lx3InputState', 'notApplicable')) == 'ready' and
          bool(getattr(cs, 'lx3PhysicalCounterValid', False)))


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
    self.main_press = None
    self.input_reset_count = None
    self.replay_base = None
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

  def reject(self, now, preserve_replay=False):
    if not preserve_replay:
      self.replay_base = None
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

  def prepare_replay(self, cs, panda, now, healthy, barriers):
    """Discard delayed intent across any input reset, barrier or transport loss."""
    serial = int(getattr(cs, 'lx3InputResetCount', 0))
    qualified = lx3_input_ready(cs) and 0 < serial < 2**32
    reset = self.input_reset_count is not None and serial != self.input_reset_count
    self.input_reset_count = serial
    if not qualified or reset or not healthy or barriers:
      self.main_press = None
      self.replay_base = None
      return
    base = self.replay_base
    if base is not None:
      consumed = (int(cs.lx3PhysicalCounter) - base['counter']) % 256
      if (panda.lx3TransportEpoch != base['epoch'] or panda.lx3PermissionPhase not in (0, 1) or
          not 1 <= panda.lx3RequestGeneration - base['origin_generation'] <= 4 or
          not 0 <= now - base['stamp'] < 0.5 or panda.lx3RequestAgeMs >= 500 or
          serial != base['reset_count'] or consumed > 26 or consumed % 2):
        self.replay_base = None

  def capture_replay(self, cs, panda, now, barriers):
    # Only preserve our own previously accepted intent. This does not preserve
    # authority, renew an old ACK, or adopt a mode reported by Panda.
    serial = int(getattr(cs, 'lx3InputResetCount', 0))
    if (barriers or panda is None or not lx3_input_ready(cs) or not 0 < serial < 2**32 or
        self.pending is not None or self.mode == EngagementMode.OFF or
        self.accepted_generation == 0 or panda.lx3TransportEpoch != self.accepted_epoch or
        not 1 <= panda.lx3RequestGeneration - self.accepted_generation <= 4 or
        panda.lx3PermissionPhase not in (0, 1) or panda.lx3RequestAgeMs >= 500):
      return False
    counter = int(cs.lx3PhysicalCounter)
    delta = (panda.lx3PhysicalCounter - counter) % 256
    if min(delta, 256 - delta) > 26 or delta % 2:
      return False
    self.replay_base = {'mode': self.mode, 'counter': counter,
                        'reset_count': serial, 'stamp': now, 'main_press': self.main_press,
                        'origin_generation': self.accepted_generation,
                        'epoch': self.accepted_epoch}
    return True

  @staticmethod
  def button_candidate(name, current, pending):
    if name == 'mainCruise':
      return EngagementMode.OFF if pending or current == EngagementMode.COMBINED else EngagementMode.COMBINED
    if name == 'lfaButton':
      return EngagementMode.OFF if pending or current != EngagementMode.OFF else EngagementMode.LATERAL
    if name in ('accelCruise', 'decelCruise') and not pending and current != EngagementMode.COMBINED:
      return EngagementMode.COMBINED
    return None

  def replay_buttons(self, buttons, cs, now):
    """Interpret delayed emissions from our own prior mode, without authority.

    MAIN's debounce anchor may precede the consumed counter. A held physical
    press and a bounded neutral anchor are required to interpret that release.
    No event is invented from the companion or a cached mode.
    """
    base = self.replay_base
    if base is None:
      return buttons
    if any(str(b.type) == 'cancel' or not getattr(b, 'lx3PhysicalValid', False) for b in buttons):
      self.replay_base = None
      return buttons
    consumed = (int(cs.lx3PhysicalCounter) - base['counter']) % 256
    for index, b in enumerate(buttons):
      name, counter = str(b.type), int(b.lx3PhysicalCounter)
      forward = (counter - base['counter']) % 256
      if b.pressed:
        if name == 'mainCruise':
          if not 2 <= forward <= consumed or forward % 2:
            self.replay_base = None
            return ()
          base['main_press'] = self.main_press = counter
        continue
      candidate = self.button_candidate(name, base['mode'], False)
      if name not in ('mainCruise', 'lfaButton', 'accelCruise', 'decelCruise'):
        continue
      admitted = 2 <= forward <= consumed and forward % 2 == 0
      if name == 'mainCruise':
        press = base['main_press']
        width = (counter - press) % 256 if press is not None else 0
        behind = (base['counter'] - counter) % 256
        admitted = (press is not None and 2 <= width <= 26 and width % 2 == 0 and
                    (admitted or (behind <= 26 and behind % 2 == 0)))
        base['main_press'] = self.main_press = None
      if not admitted:
        self.replay_base = None
        return ()
      if candidate is None:
        continue
      base['mode'] = candidate
      if candidate != EngagementMode.OFF:
        # Re-enter through the ordinary association, StateMachine PRE_ENABLE,
        # and a new generation-bound ACK. Accepted mode/identity stay cleared.
        # This candidate is derived from CAN. Only the normal matching Panda
        # (mode,counter,generation,epoch) association can issue its fresh ACK.
        self.replay_base = None
        self.pending, self.pending_counter, self.pending_since = candidate, counter, now
        self.pending_generation = self.pending_epoch = 0
        return buttons[index + 1:]
    return ()  # Already consumed these emissions; never interpret them twice.

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
      if not getattr(b, 'lx3PhysicalValid', False):
        continue
      name = str(b.type)
      if name == 'mainCruise':
        self.main_press = int(b.lx3PhysicalCounter) if b.pressed else None
      if b.pressed:
        continue
      next_candidate = self.button_candidate(name, current, self.pending is not None)
      if next_candidate is None:
        continue
      candidate = next_candidate
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
