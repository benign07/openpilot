"""Physical LX3 button integrity and driver gestures; no actuator commands.

2026-09-30 vehicle captures: 0x10B, bus 0, 16 bytes, 25 Hz, counter += 2.
Button identities reuse the installed port. OEM validation remains separate.
Never feed a rejected or recovered held gesture to an engagement state machine.
"""

class Lx3ButtonInput:
  TIMEOUT_NS = 200_000_000
  MIN_PERIOD_NS = 10_000_000
  COUNTER_STEP = 2

  def __init__(self):
    self.counter = None
    self.received_ns = 0
    self.last_observed_ns = 0
    self.samples = 0
    self.ready = False
    self.reason = 'missing'
    self.cruise = 0
    self.lfa = False

  def reject(self, reason):
    self.ready = False
    self.samples = 0
    self.reason = reason
    self.cruise = 0
    self.lfa = False
    return False

  def update(self, addr, bus, data, received_ns, now_ns, checksum):
    if addr != 0x10B or bus != 0:
      return False  # Forwarded echoes and other button sources have no authority.
    if len(data) != 16:
      return self.reject('length')
    if received_ns <= 0 or not 0 <= now_ns - received_ns <= self.TIMEOUT_NS:
      return self.reject('stale')
    if received_ns < self.last_observed_ns:
      return self.reject('reordered')
    self.last_observed_ns = received_ns
    if int.from_bytes(data[:2], 'little') != checksum(addr, None, data):
      return self.reject('checksum')
    counter = data[2]
    if self.counter is not None:
      elapsed = received_ns - self.received_ns
      delta = (counter - self.counter) % 256
      if delta == 0:
        return self.reject('duplicate')  # Does not refresh the valid timestamp.
      # pandad timestamps CAN batches, not individual frames. A valid sequence
      # can share one timestamp or be delivered less than 10ms apart. Physical
      # minimum-period enforcement belongs to Panda's actual RX timer.
      if delta != self.COUNTER_STEP or not 0 <= elapsed <= self.TIMEOUT_NS:
        self.counter, self.received_ns = counter, received_ns
        return self.reject('sequence')
    self.counter, self.received_ns = counter, received_ns
    self.samples += 1
    # Requalification requires a neutral baseline, not a cached held press.
    cruise, lfa = data[10] & 15, bool(data[10] & 128)
    if not self.ready:
      if cruise != 0 or lfa:
        return self.reject('neutral_required')
      self.ready = self.samples >= 3
    self.reason = 'valid' if self.ready else 'warming_up'
    self.cruise, self.lfa = (cruise, lfa) if self.ready else (0, False)
    return self.ready

  def fresh(self, now_ns):
    if not self.ready:
      return False  # Preserve neutral warmup progress between host update ticks.
    if not 0 <= now_ns - self.received_ns <= self.TIMEOUT_NS:
      return self.reject('stale')
    return True


class Lx3ButtonIntent:
  # Installed LX3 main-button policy, expressed in CAN time rather than host ticks.
  MAIN_RELEASE_NS = 300_000_000
  NAMES = {1: 'accelCruise', 2: 'decelCruise', 3: 'gapAdjustCruise', 4: 'cancel'}

  def __init__(self):
    self.input = Lx3ButtonInput()
    self.reset_gesture()

  def reset_gesture(self):
    self.main = False
    self.main_last_ns = 0
    self.main_release_counter = 0
    self.main_neutral_seen = False
    self.button = None

  def update(self, addr, bus, data, received_ns, now_ns, checksum):
    if addr != 0x10B or bus != 0:
      return []
    if not self.input.update(addr, bus, data, received_ns, now_ns, checksum):
      self.reset_gesture()
      return []  # Invalid/stale input never fabricates an enable release.
    raw, lfa = self.input.cruise, self.input.lfa
    if raw not in (0, 1, 2, 3, 4, 8) or (lfa and raw not in (0, 4)):
      self.input.reject('ambiguous_button')
      self.reset_gesture()
      return []
    if raw == 4:
      self.reset_gesture()
      self.button = 'cancel'
      return [('cancel', True)]  # Cancel wins over every simultaneous enable.
    events = []
    button = 'lfaButton' if lfa else self.NAMES.get(raw)
    if raw == 8:
      if not self.main:
        events.append(('mainCruise', True))
      self.main, self.main_last_ns = True, received_ns
      self.main_release_counter = (self.input.counter + 2) % 256
      self.main_neutral_seen = False
    elif self.main:
      if raw == 0 and not lfa:
        self.main_neutral_seen = True
      if button is not None and not self.main_neutral_seen:
        self.input.reject('ambiguous_gesture')
        self.reset_gesture()
        return []
      # A different supported button after a real neutral frame confirms the
      # earlier MAIN release. Preserve MAIN's toggle (including explicit OFF),
      # then process the new gesture normally. Plain MAIN/neutral flicker keeps
      # the existing 300ms debounce. No icon/host mode determines this evidence.
      if button is not None or received_ns - self.main_last_ns >= self.MAIN_RELEASE_NS:
        self.main = False
        self.main_neutral_seen = False
        events.append(('mainCruise', False))
    if button != self.button:
      # Native releases RES/SET/LFA only on fully neutral physical input.
      # Direct B->C (or B->MAIN) discards B without manufacturing its release.
      if self.button is not None and raw == 0 and not lfa:
        events.append((self.button, False))
      if button is not None:
        events.append((button, True))
      self.button = button
    return events

  def fresh(self, now_ns):
    ready = self.input.fresh(now_ns)
    if not ready:
      self.reset_gesture()
    return ready

  def from_parser(self, parser, checksum, with_counter=False):
    if parser.raw_overflow:
      self.input.reject('capture_overflow')
      self.reset_gesture()
      parser.raw_frames.clear()
      parser.raw_overflow = False
    events = []
    while parser.raw_frames:
      address, bus, data, received_ns = parser.raw_frames.popleft()
      frame_events = self.update(address, bus, data, received_ns, parser._last_update_nanos, checksum)
      if with_counter:
        events.extend((name, pressed, self.main_release_counter if name == 'mainCruise' and not pressed
                       else self.input.counter) for name, pressed in frame_events)
      else:
        events.extend(frame_events)
      if not self.input.ready:
        # Discard every potential enable after a later invalid frame, but keep
        # an earlier CRC-valid explicit cancel. Not-ready also faults the host.
        events[:] = [event for event in events if event[0] == 'cancel']
    return events, self.fresh(parser._last_update_nanos)
