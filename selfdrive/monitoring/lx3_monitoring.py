"""Explicit camera-less monitoring for the LX3 authority profile only.

DisableDM=1 selects the stock wheel-interaction policy on this profile. It
does not report a healthy camera or relax native actuator permission checks.
"""
from opendbc.car.hyundai.lx3_buttons import uses_lx3_authority


def uses_wheel_monitoring(CP, disable_dm):
  return disable_dm == 1 and uses_lx3_authority(CP)


def monitoring_enabled(CP, disable_dm):
  return disable_dm == 0 or uses_wheel_monitoring(CP, disable_dm)


def monitoring_state_ready(CP, disable_dm, sm):
  return (sm.alive['driverMonitoringState'] and sm.valid['driverMonitoringState'] and
          (uses_wheel_monitoring(CP, disable_dm) or sm.freq_ok['driverMonitoringState']))
