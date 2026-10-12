#!/usr/bin/env python3
"""C3xLite binary GPIO buzzer. Routine mute preserves critical alerts."""
import os
import logging
import subprocess
import time
from cereal import car, messaging
from openpilot.common.params import Params
from openpilot.common.realtime import Ratekeeper

AudibleAlert = car.CarControl.HUDControl.AudibleAlert
# Unclassified alerts (including custom audio1..10) remain protected.
OPTIONAL_ALERTS = frozenset(getattr(AudibleAlert, name) for name in (
  'engage', 'longEngaged', 'trafficSignGreen', 'trafficSignChanged',
  'laneChange', 'stopping', 'autoHold', 'engage2', 'audioTurn', 'reverseGear',
))
REPEAT_ALERTS = frozenset(getattr(AudibleAlert, name) for name in (
  'promptRepeat', 'promptDistracted', 'warningSoft', 'warningImmediate',
))


class Beepd:
  def __init__(self):
    self.params = Params()
    self.current_alert = AudibleAlert.none
    self.carrot_count_down = 0
    self.selfdrive_timeout_alert = False
    self.pattern = ()
    self.pattern_start = 0.0
    self.pattern_index = 0
    self.deadline = None
    self.protected = False
    self.last_repeat = -1.0
    self.output = False
    self.gpio = None
    self.gpio_fallback_logged = False
    self.mute_checked = float('-inf')
    self.mute_value = False
    self.enable_gpio()
    self._write_gpio(False)

  def enable_gpio(self):
    for path, value in (('/sys/class/gpio/export', '42'), ('/sys/class/gpio/gpio42/direction', 'out')):
      subprocess.run(f'echo "{value}" | sudo tee {path}', shell=True,
                     stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL, encoding='utf8', check=False)
    # Elevation is restricted to initialization, not every 10 ms pulse edge.
    path = '/sys/class/gpio/gpio42/value'
    try:
      subprocess.run(['sudo', 'chown', f'{os.getuid()}:{os.getgid()}', path], check=True,
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
      self.gpio = open(path, 'w', encoding='ascii')
    except (OSError, subprocess.SubprocessError):
      self.gpio_fallback()

  def gpio_fallback(self):
    if self.gpio is not None:
      try:
        self.gpio.close()
      except OSError:
        pass
    self.gpio = None
    if not self.gpio_fallback_logged:
      logging.getLogger(__name__).warning('Buzzer persistent GPIO unavailable; using legacy writer, pulse timing unverified')
      self.gpio_fallback_logged = True

  def _write_gpio(self, on):
    if self.gpio is not None:
      try:
        self.gpio.seek(0)
        self.gpio.write(str(int(on)))
        self.gpio.flush()
        return
      except OSError:
        self.gpio_fallback()
    # Compatibility fallback: retain an audible attempt instead of killing
    # soundd on ownership/open failure. Its exact pulse timing is not promised.
    subprocess.run(['sudo', 'tee', '/sys/class/gpio/gpio42/value'], input=str(int(on)),
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, text=True, check=False)

  def muted(self, now):
    if now - self.mute_checked >= 0.5:
      self.mute_checked = now
      self.mute_value = self.params.get_bool('C3xLiteBuzzerMute') or self.params.get_int('SoundVolumeAdjust') <= 5
    return self.mute_value

  def update_alert(self, new_alert, now=None):
    now = time.monotonic() if now is None else now
    changed = new_alert != self.current_alert
    self.current_alert = new_alert
    if new_alert == AudibleAlert.none:
      return
    protected = new_alert not in OPTIONAL_ALERTS
    if not protected and (self.muted(now) or (self.pattern and self.protected)):
      return
    if not changed and not (new_alert in REPEAT_ALERTS and not self.pattern and now - self.last_repeat >= 1.0):
      return
    # One scheduler owns GPIO; an old routine pattern cannot stop a warning.
    # Preserve the original short pulses at a 100 Hz output loop rate.
    if new_alert in (AudibleAlert.disengage, AudibleAlert.longDisengaged, AudibleAlert.disengage2):
      self.pattern = ((True, 0.01), (False, 0.01), (True, 0.01))
    elif protected:
      self.pattern = ((True, 0.01), (False, 0.01)) * 3
    else:
      self.pattern = ((True, 0.05),)
    self.protected = protected
    self.pattern_start = self.last_repeat = now
    self.pattern_index = 0
    self.deadline = None

  def tick(self, now=None):
    now = time.monotonic() if now is None else now
    if self.pattern and not self.protected and self.muted(now):
      self.pattern = ()
    output = False
    if self.pattern:
      if self.deadline is None:
        self.deadline = now + self.pattern[0][1]
      elif now + 1e-9 >= self.deadline:
        self.pattern_index += 1
        if self.pattern_index >= len(self.pattern):
          self.pattern = ()
        else:
          self.deadline = now + self.pattern[self.pattern_index][1]
      if self.pattern:
        output = self.pattern[self.pattern_index][0]
    if output != self.output:
      self._write_gpio(output)
      self.output = output

  def get_audible_alert(self, sm, timeout_check):
    if sm.updated['selfdriveState']:
      new_alert = sm['selfdriveState'].alertSound.raw
      if new_alert == AudibleAlert.none:
        count_down = sm['carrotMan'].leftSec
        if self.carrot_count_down != count_down:
          self.carrot_count_down = count_down
          if count_down == 0:
            new_alert = AudibleAlert.longDisengaged
          elif 0 < count_down <= 10:
            new_alert = getattr(AudibleAlert, f'audio{count_down}')
          elif count_down == 11:
            new_alert = AudibleAlert.promptDistracted
      self.update_alert(new_alert)
      self.selfdrive_timeout_alert = False
    elif timeout_check(sm):
      self.update_alert(AudibleAlert.warningImmediate)
      self.selfdrive_timeout_alert = True
    elif self.selfdrive_timeout_alert:
      self.update_alert(AudibleAlert.none)
      self.selfdrive_timeout_alert = False

  def beepd_thread(self, timeout_check):
    sm = messaging.SubMaster(['selfdriveState', 'carrotMan'])
    rk = Ratekeeper(100)
    try:
      while True:
        sm.update(0)
        self.get_audible_alert(sm, timeout_check)
        self.tick()
        rk.keep_time()
    finally:
      try:
        self._write_gpio(False)
      finally:
        if self.gpio is not None:
          self.gpio.close()


def main():
  from openpilot.selfdrive.ui.soundd import check_selfdrive_timeout_alert
  Beepd().beepd_thread(check_selfdrive_timeout_alert)


if __name__ == '__main__':
  main()
