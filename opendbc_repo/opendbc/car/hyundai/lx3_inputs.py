"""Physical LX3 button stream qualification, without engagement permission.

2026-09-30 vehicle captures: 0x10B, bus 0, 16 bytes, 25 Hz, counter += 2.
Button identity and release debounce require operator-labelled captures.
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
    if received_ns <= self.last_observed_ns:
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
      if delta != self.COUNTER_STEP or not self.MIN_PERIOD_NS <= elapsed <= self.TIMEOUT_NS:
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
    if not self.ready or not 0 <= now_ns - self.received_ns <= self.TIMEOUT_NS:
      return self.reject('stale')
    return True
