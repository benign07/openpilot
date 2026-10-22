"""Display-only preference, separate from driving preferences and native Params."""
import json
import math
import os
from pathlib import Path
import threading
import uuid

KEY = 'NotificationBorderPulse'
_LOCK = threading.RLock()


class NotificationPreferences:
  def __init__(self, path='/data/community/notification_preferences.json'):
    self.path = Path(path)

  def read(self):
    try:
      if self.path.is_symlink() or not 0 < self.path.stat().st_size <= 1024:
        raise ValueError('invalid notification preferences')
      row = json.loads(self.path.read_text(encoding='utf8'))
      if (not isinstance(row, dict) or set(row) != {'schema', 'border'} or
          type(row['schema']) is not int or row['schema'] != 1 or
          type(row['border']) is not int or row['border'] not in (0, 1)):
        raise ValueError('invalid notification preferences')
      return row['border']
    except (OSError, ValueError, TypeError):
      return 1

  def set(self, value):
    if type(value) is not int or value not in (0, 1):
      raise ValueError('border must be 0 or 1')
    with _LOCK:
      if self.path.is_symlink():
        raise ValueError('notification preferences must not be a symlink')
      self.path.parent.mkdir(parents=True, exist_ok=True)
      temporary = self.path.with_name(self.path.name+'.'+uuid.uuid4().hex+'.tmp')
      try:
        with temporary.open('x', encoding='utf8') as stream:
          json.dump({'schema': 1, 'border': value}, stream)
          stream.flush()
          os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        if os.name == 'posix':
          fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
          try:
            os.fsync(fd)
          finally:
            os.close(fd)
      finally:
        temporary.unlink(missing_ok=True)
    return value


def border_pulse(critical, notification, enabled, now):
  """Critical visual emphasis is unconditional; optional alerts obey preference.

  One flash per second. No alert text, alert state, audio or CAN modification.
  """
  if not (critical or (notification and enabled)) or not math.isfinite(now) or now < 0:
    return None
  if now % 1.0 >= 0.5:
    return None
  return (201, 34, 49, 255) if critical else (218, 111, 37, 255)


class BorderPulse:
  """Anchor pulses to onset; short optional alerts get one half-second flash."""
  def __init__(self):
    self.token = None
    self.started = None
    self.until = 0.0
    self.critical_started = None

  def update(self, critical, token, enabled, now):
    if not math.isfinite(now) or now < 0:
      self.__init__()
      return None
    if critical:
      self.token, self.started, self.until = None, None, 0.0
      if self.critical_started is None:
        self.critical_started = now
      return border_pulse(True, False, True, now-self.critical_started)
    self.critical_started = None
    if not enabled:
      self.token, self.started, self.until = None, None, 0.0
      return None
    if token is not None and token != self.token:
      self.started, self.until = now, now+0.5
    self.token = token
    if self.started is None or (token is None and now >= self.until):
      self.started = None
      return None
    return border_pulse(False, True, True, now-self.started)
