"""Bridge Carrot decisions to observed LX3 Panda authority, without granting it.

Button intent, native permission and actuator activity are separate states.
The MCU remains the only source of permission/generation; neither a HUD command
nor an OEM cruise-state packet supplies a physical event citation.
"""
LAT, LONG = 1, 2
VERSION = 3
STATUS_MAX_NS = 250_000_000
DECISION_MAX_NS = 600_000_000


def verified(a):
  return (a.version == VERSION and a.profile == 1 and a.epoch != 0 and a.inputReady and
          a.heartbeatAgeMs <= 250 and a.allowed in (0, 1, 2, 3) and
          bool(a.allowed & LAT) == bool(a.lateralGeneration) and
          bool(a.allowed & LONG) == bool(a.longitudinalGeneration))


def monitor_lateral(a):
  return verified(a) and bool(a.allowed & LAT)


def populate_car_state(CS, sm, cruise, params, now_ns):
  healthy = CS.lx3Authority.buttonHealthy
  states = sm['pandaStates']
  if (len(states) == 1 and sm.valid['pandaStates'] and sm.alive['pandaStates'] and
      0 <= now_ns - sm.logMonoTime['pandaStates'] <= STATUS_MAX_NS):
    CS.lx3Authority = states[0].lx3Authority
  a = CS.lx3Authority
  a.buttonHealthy = healthy
  a.config = (int(params.get_bool('AlwaysLateral')) | (int(cruise.disengage_on_accelerator) << 1) |
              (int(cruise.autoCruiseControl > 0) << 2) | (min(max(cruise._lfa_button_mode, 0), 2) << 3) |
              (int(cruise._cancel_button_mode == 1) << 5))
  a.longPressMs = min(max((cruise._cruise_button_long_delay + 30) * 10, 100), 10000)
  a.lateralDecisionKey, a.lateralDecisionTime = cruise.lx3_lat_key, cruise.lx3_lat_time
  a.longitudinalDecisionKey, a.longitudinalDecisionTime = cruise.lx3_long_key, cruise.lx3_long_time
  a.autoResume = cruise.lx3_auto_until > cruise.frame and not cruise._cruise_cancel_state


def cited_decision(a, intent, now_ns):
  """Match each newly requested axis to one still-pending *physical* gesture."""
  additions = intent & ~a.allowed
  if not additions or not a.pendingGeneration or a.pendingAgeMs >= 600:
    return 0, 0, intent
  matching = 0
  for axis, key, stamp in ((LAT, a.lateralDecisionKey, a.lateralDecisionTime),
                           (LONG, a.longitudinalDecisionKey, a.longitudinalDecisionTime)):
    if additions & axis and key == a.pendingKey and 0 <= now_ns - stamp < DECISION_MAX_NS:
      matching |= axis
  if not matching:
    return 0, 0, intent
  # Independent LFA and RES releases can be pending simultaneously. Cite one
  # physical event per STATE; the next status advertises the other pending axis.
  return a.pendingKey, a.pendingGeneration, (intent & a.allowed) | matching


def lateral_events_clear(events, always_lateral):
  # These events govern longitudinal availability. Other real faults, including
  # driver-monitoring disable events, still remove lateral intent.
  long_only = {'wrongCarMode', 'pcmDisable', 'buttonCancel', 'cruiseDisabled', 'resumeBlocked',
               'belowEngageSpeed', 'preEnableStandstill', 'wrongCruiseMode'}
  if always_lateral:
    long_only.add('pedalPressed')
  return not any(str(e.name) not in long_only and
                 (e.noEntry or e.immediateDisable or e.softDisable or e.userDisable) for e in events)


def configure_control(CC, CS, events, enabled, always_lateral, driving, fresh, now_ns):
  CC.lx3Authority = CS.lx3Authority
  a = CC.lx3Authority
  ready = fresh and a.buttonHealthy and verified(a)
  lat = ready and driving and CS.latEnabled and lateral_events_clear(events, always_lateral)
  lat = lat and not CS.steerFaultTemporary and not CS.steerFaultPermanent
  if not always_lateral and (CS.brakePressed or (CS.gasPressed and a.config & 2)):
    lat = False
  long = ready and enabled and not CS.brakePressed and not (CS.gasPressed and a.config & 2)
  a.intent = (LAT if lat else 0) | (LONG if long else 0)
  a.decisionKey, a.pendingGeneration, a.intent = cited_decision(a, a.intent, now_ns)
  a.observedLongRevision = a.longitudinalRevision
  a.autoResume = a.autoResume and long
  return bool(lat and a.allowed & LAT and not a.oemEmergency), bool(long and a.allowed & LONG)
