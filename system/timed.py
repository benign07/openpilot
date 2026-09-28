#!/usr/bin/env python3
import datetime
import subprocess
import time
from pathlib import Path
from typing import NoReturn

import cereal.messaging as messaging
from cereal import car
from openpilot.common.time_helpers import min_date, MAX_DATE, system_time_valid
from openpilot.common.swaglog import cloudlog
from openpilot.common.params import Params
from openpilot.common.gps import get_gps_location_service


def set_time(new_time):
  diff = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - new_time
  if abs(diff) < datetime.timedelta(seconds=10):
    cloudlog.debug(f"Time diff too small: {diff}")
    return False

  cloudlog.debug(f"Setting time to {new_time}")
  try:
    subprocess.run(f"TZ=UTC date -s '{new_time}'", shell=True, check=True, timeout=5)
    return True
  except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
    cloudlog.exception("timed.failed_setting_time")
    return False


def has_lx3_calendar(params):
  raw = params.get('CarParams')
  if raw is None:
    return False
  try:
    with car.CarParams.from_bytes(raw) as cp:
      return cp.carFingerprint == 'HYUNDAI_PALISADE_LX3_HEV'
  except Exception:
    return False


def parked_can_time(sm, now_nanos):
  """Only accept the validated car calendar with fresh, parked, inactive controls."""
  services = ('carState', 'carControl', 'selfdriveState')
  if not all(sm.valid[s] and 0 <= now_nanos - sm.logMonoTime[s] < 500_000_000 for s in services):
    return None
  cs, cc, ss = (sm[s] for s in services)
  if (not cs.canValid or cs.canTimeout or str(cs.gearShifter) != 'park' or not cs.standstill or
      not abs(cs.vEgo) < .03 or cc.enabled or cc.latActive or cc.longActive or ss.enabled or ss.active):
    return None
  try:
    candidate = datetime.datetime.fromtimestamp(cs.datetime / 1000., datetime.timezone.utc).replace(tzinfo=None)
    return candidate if min_date() < candidate < MAX_DATE else None
  except (ValueError, OverflowError, OSError):
    return None


def main() -> NoReturn:
  """
    timed has two responsibilities:
    - getting the current time from GPS, or a validated parked-car calendar at boot
    - publishing the time in the logs

    AGNOS will also use NTP to update the time.
  """

  params = Params()
  gps_location_service = get_gps_location_service(params)

  pm = messaging.PubMaster(['clocks'])
  sm = messaging.SubMaster([gps_location_service, 'carState', 'carControl', 'selfdriveState'])
  can_recovered = False
  while True:
    sm.update(1000)

    msg = messaging.new_message('clocks')
    msg.valid = system_time_valid()
    msg.clocks.wallTimeNanos = time.time_ns()
    pm.send('clocks', msg)

    gps = sm[gps_location_service]
    gps_fresh = (sm.updated[gps_location_service] and sm.valid[gps_location_service] and
                 0 <= time.monotonic() - sm.logMonoTime[gps_location_service] / 1e9 <= 2.0)
    try:
      gps_time = datetime.datetime.fromtimestamp(gps.unixTimestampMillis / 1000., datetime.timezone.utc).replace(tzinfo=None)
      gps_usable = gps_fresh and gps.hasFix and min_date() < gps_time < MAX_DATE
    except (ValueError, OverflowError, OSError):
      gps_usable = False
    # Preserve GPS/NTP priority. This only recovers an invalid boot clock, once,
    # while parked; a valid clock is never disciplined by user-adjustable car time.
    if not can_recovered and not system_time_valid() and not Path('/run/systemd/timesync/synchronized').exists():
      if not gps_usable and has_lx3_calendar(params):
        candidate = parked_can_time(sm, time.monotonic_ns())
        if candidate is not None:
          can_recovered = bool(set_time(candidate))
          if can_recovered:
            cloudlog.info('timed recovered invalid boot clock from parked car calendar')
    if not gps_usable:
      # Car telemetry wakes SubMaster at 100 Hz. Keep this background clock
      # service bounded even while GPS is missing or has no usable fix.
      time.sleep(1)
      continue

    set_time(gps_time)
    time.sleep(10)

if __name__ == "__main__":
  main()
