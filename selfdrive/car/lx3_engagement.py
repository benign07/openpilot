"""LX3 engagement handshake using the normal Events and StateMachine.

A request is not an engagement. Keep CC.enabled false until Panda acknowledges
the longitudinal grant. Genuine disagreement after engagement is still handled
by the existing controlsMismatch detector.
"""
from cereal import log
from openpilot.selfdrive.car.lx3_authority import LONG, DECISION_MAX_NS, STATUS_MAX_NS, verified
from openpilot.selfdrive.selfdrived.events import ET, EVENTS

EventName = log.OnroadEvent.EventName


class Lx3Engagement:
  def __init__(self):
    self.deadline = 0
    self.epoch = 0
    self.revision = 0
    self.automatic = False
    self.request = False
    self.refusing = False
    self.refusal_sequence = 0
    self.key = self.generation = self.start_sequence = self.start_generation = 0

  def update(self, events, CS, enabled, now_ns):
    self.request = False
    a = CS.lx3Authority
    fresh = verified(a) and a.buttonHealthy and 0 <= now_ns - a.statusMonoTime <= STATUS_MAX_NS
    if (fresh and a.sequence > self.refusal_sequence and not ((a.allowed | a.armed) & LONG) and
        not a.longPendingGeneration):
      self.refusing = False
    if enabled:
      self.deadline = 0
      return
    enabling = events.contains(ET.ENABLE)
    blocked = any(events.contains(t) for t in (ET.NO_ENTRY, ET.USER_DISABLE, ET.IMMEDIATE_DISABLE, ET.SOFT_DISABLE))
    if blocked:
      # Preserve stock no-entry alerts and all disabling events.
      if self.deadline and not self.automatic and events.contains(ET.NO_ENTRY) and not enabling:
        events.add(EventName.buttonEnable)
      # Pedals suspend a physically armed session; they do not cancel its
      # resume eligibility. A second brake while awaiting OFF acknowledgement
      # must have the same semantics as the first brake on the MCU.
      internal_pause = CS.activateCruise < 0 and not a.remoteRequest and not any(
        b.physical and str(b.type) == 'cancel' for b in CS.buttonEvents)
      non_pedal_block = any(e != EventName.pedalPressed and not (e == EventName.buttonCancel and internal_pause) and any(
        t in EVENTS.get(e, {}) for t in (ET.NO_ENTRY, ET.USER_DISABLE, ET.IMMEDIATE_DISABLE, ET.SOFT_DISABLE))
        for e in events.events)
      if self.deadline and non_pedal_block:
        self.refusing = True
        self.refusal_sequence = a.sequence
      self.deadline = 0
      return
    if enabling:
      physical = (CS.buttonEnable and a.longitudinalDecisionKey >> 8 in (1, 2, 8) and
                  0 <= now_ns - a.longitudinalDecisionTime < DECISION_MAX_NS and not a.remoteRequest)
      automatic = CS.activateCruise > 0 and a.autoResume and bool(a.armed & LONG) and not a.remoteRequest
      if fresh and not self.refusing and (physical or (automatic and not self.deadline)):
        # Carrot's armed auto request already lives for 100 control ticks. A
        # short brake tap needs an extra OFF-ack round trip at 10 Hz.
        self.deadline = a.longitudinalDecisionTime + DECISION_MAX_NS if physical else now_ns + 1_000_000_000
        self.epoch, self.revision = a.epoch, a.longitudinalRevision
        self.automatic = not physical
        self.key = a.longitudinalDecisionKey if physical else 0
        self.generation = 0
        self.start_sequence, self.start_generation = a.sequence, a.longitudinalGeneration
      elif not self.deadline:
        events.add(EventName.lx3AuthorityDenied)
    # Only defer enable, never remove fault/cancel/mismatch events.
    events.events[:] = [e for e in events.events if ET.ENABLE not in EVENTS.get(e, {})]
    if not self.deadline:
      return
    if (not fresh or now_ns >= self.deadline or a.epoch != self.epoch or
        (self.automatic and (a.longitudinalRevision != self.revision or not a.armed & LONG))):
      self.deadline = 0
      self.refusing = True
      self.refusal_sequence = a.sequence
      events.add(EventName.lx3AuthorityDenied)
      return
    if not self.automatic and a.longPendingGeneration and a.longPendingKey == self.key:
      self.generation = a.longPendingGeneration
    grant = (a.allowed & LONG and a.sequence > self.start_sequence and
             ((self.automatic and a.longitudinalGeneration != self.start_generation) or
              (not self.automatic and self.generation and a.longitudinalGeneration == self.generation)))
    if grant:
      # Normal state transition and enable sound only after a real MCU grant.
      events.add(EventName.buttonEnable)
      self.deadline = 0
      self.request = True  # same atomic selfdriveState also publishes enabled
      return
    # An automatic request waits for the post-brake OFF acknowledgement. A
    # fresh physical citation can supersede that barrier, as on the MCU.
    self.request = not self.automatic or not (a.armed & 4)
