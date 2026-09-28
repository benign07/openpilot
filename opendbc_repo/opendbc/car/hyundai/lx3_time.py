"""Validated Korean LX3 HEV calendar, receive-only; see docs/LX3_CAN_TIME.md."""
from datetime import datetime, timedelta, timezone

MESSAGE = 'LX3_LOCAL_TIME'
UTC_MESSAGE = 'LX3_UTC_SNAPSHOT'
MAX_AGE_NS = 2_500_000_000
KST = timezone(timedelta(hours=9))


def calendar_millis(signals):
  try:
    year = int(signals['YEAR'])
    if not 0 <= year <= 99:
      return 0
    stamp = datetime(2000 + year, int(signals['MONTH']), int(signals['DATE']),
                     int(signals['HOURS']), int(signals['MINUTES']), int(signals['SECONDS']), tzinfo=KST)
    return int(stamp.timestamp() * 1000)
  except (KeyError, TypeError, ValueError, OverflowError):
    return 0


def utc_snapshot_millis(signals):
  try:
    stamp = datetime(int(signals['YEAR']), int(signals['MONTH']), int(signals['DATE']),
                     int(signals['HOURS']), int(signals['MINUTES']), int(signals['SECONDS']), tzinfo=timezone.utc)
    return int(stamp.timestamp() * 1000)
  except (KeyError, TypeError, ValueError, OverflowError):
    return 0


class Lx3Clock:
  """Require progressing frames; cached, frozen or discontinuous calendars expire."""
  def __init__(self):
    self.reset()

  def reset(self):
    self.received_ns = 0
    self.changed_ns = 0
    self.millis = 0
    self.advances = 0
    self.zone_verified = False

  def update(self, signals, received_ns, now_ns, utc_millis=0, utc_received_ns=0):
    if received_ns <= 0 or not 0 <= now_ns - received_ns <= MAX_AGE_NS:
      self.reset()
      return 0
    if received_ns != self.received_ns:
      millis = calendar_millis(signals)
      if not millis:
        self.reset()
        return 0
      if self.received_ns:
        elapsed = (received_ns - self.received_ns) / 1e9
        advance = (millis - self.millis) / 1000
        if elapsed <= 0 or elapsed > 2.5 or advance < 0 or abs(advance - elapsed) > 1.5:
          self.reset()
      if not self.received_ns or millis != self.millis:
        if self.received_ns:
          self.advances = min(2, self.advances + 1)
        self.changed_ns = received_ns
      self.received_ns, self.millis = received_ns, millis
    if now_ns - self.changed_ns > MAX_AGE_NS:
      self.reset()
      return 0
    # This car does not broadcast the old HDA country-code frame. Confirm KST
    # using a matching UTC snapshot. The snapshot may later freeze for minutes;
    # it is never used as the running clock. Discontinuities revoke confirmation.
    if (not self.zone_verified and utc_received_ns > 0 and 0 <= now_ns - utc_received_ns <= MAX_AGE_NS and
        utc_millis > 0 and abs(self.millis - utc_millis) <= 5000):
      self.zone_verified = True
    return self.millis if self.advances >= 2 and self.zone_verified else 0
