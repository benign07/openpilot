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


def copy_status(CS, sm, now_ns):
  healthy = CS.lx3Authority.buttonHealthy
  states = sm['pandaStates']
  if (len(states) == 1 and sm.valid['pandaStates'] and sm.alive['pandaStates'] and
      0 <= now_ns - sm.logMonoTime['pandaStates'] <= STATUS_MAX_NS):
    CS.lx3Authority = states[0].lx3Authority
    CS.lx3Authority.statusMonoTime = sm.logMonoTime['pandaStates']
  else:
    CS.lx3Authority = {}
  CS.lx3Authority.buttonHealthy = healthy


def reconcile_lateral(cruise, CS, always_lateral, now_ns):
  a = CS.lx3Authority
  valid_status = a.version == VERSION and a.profile == 1 and a.epoch != 0
  new_request = any(b.physical and not b.pressed and b.physicalKey == cruise.lx3_lat_key for b in CS.buttonEvents)
  pedal_block = not always_lateral and (CS.brakePressed or (CS.gasPressed and a.config & 2))
  blocked = (str(CS.gearShifter) not in ('drive', 'sport', 'manumatic', 'eco') or
             CS.steerFaultTemporary or CS.steerFaultPermanent or
             (pedal_block and (not a.armed & LONG or new_request)))
  live = cruise.lx3_lat_key and (0 <= now_ns - cruise.lx3_lat_time < DECISION_MAX_NS or
                                 a.statusMonoTime <= cruise.lx3_lat_time + DECISION_MAX_NS)
  revoked = valid_status and not (a.allowed & LAT or a.armed & LAT) and (
    cruise.lx3_lat_was_allowed or not live)
  if cruise._lat_enabled and (blocked or revoked):
    cruise._lat_enabled = False
    cruise.lx3_lat_key = cruise.lx3_lat_time = 0
    cruise.lx3_lateral_refused = True
    cruise._add_log('LFA request refused or permission revoked; press LFA after the condition clears')
  if valid_status:
    cruise.lx3_lat_was_allowed = bool(a.allowed & LAT)


def populate_car_state(CS, sm, cruise, params, now_ns):
  copy_status(CS, sm, now_ns)
  a = CS.lx3Authority
  a.config = (int(params.get_bool('AlwaysLateral')) | (int(cruise.disengage_on_accelerator) << 1) |
              (int(cruise.autoCruiseControl > 0) << 2) | (min(max(cruise._lfa_button_mode, 0), 2) << 3) |
              (int(cruise._cancel_button_mode == 1) << 5))
  a.longPressMs = min(max((cruise._cruise_button_long_delay + 30) * 10, 100), 10000)
  a.lateralDecisionKey, a.lateralDecisionTime = cruise.lx3_lat_key, cruise.lx3_lat_time
  a.longitudinalDecisionKey, a.longitudinalDecisionTime = cruise.lx3_long_key, cruise.lx3_long_time
  a.autoResume = cruise.lx3_auto_until > cruise.frame and not cruise._cruise_cancel_state
  a.remoteRequest = cruise.lx3_remote_cycle
  a.lateralRefused = cruise.lx3_lateral_refused


def cited_decision(a, intent, now_ns):
  """Match each newly requested axis to one still-pending *physical* gesture."""
  additions = intent & ~a.allowed
  if not additions:
    return 0, 0, intent
  for pending_key, generation, age, axes in ((a.pendingKey, a.pendingGeneration, a.pendingAgeMs, a.pendingAxes),
                                            (a.longPendingKey, a.longPendingGeneration, a.longPendingAgeMs, a.longPendingAxes)):
    if not generation or age >= 600:
      continue
    matching = 0
    for axis, key, stamp in ((LAT, a.lateralDecisionKey, a.lateralDecisionTime),
                             (LONG, a.longitudinalDecisionKey, a.longitudinalDecisionTime)):
      if additions & axes & axis and key == pending_key and 0 <= now_ns - stamp < DECISION_MAX_NS:
        matching |= axis
    if matching:
      return pending_key, generation, (intent & a.allowed) | matching
  return 0, 0, intent


def lateral_events_clear(events, always_lateral):
  # These events govern longitudinal availability. Other real faults, including
  # driver-monitoring disable events, still remove lateral intent.
  long_only = {'wrongCarMode', 'pcmDisable', 'buttonCancel', 'cruiseDisabled', 'resumeBlocked',
               'belowEngageSpeed', 'preEnableStandstill', 'wrongCruiseMode', 'accFaulted', 'radarFault', 'radarTempUnavailable'}
  if always_lateral:
    long_only.add('pedalPressed')
  return not any(str(e.name) in ('driverDistracted3', 'driverUnresponsive3', 'tooDistracted') or
                 (str(e.name) not in long_only and
                  (e.noEntry or e.immediateDisable or e.softDisable or e.userDisable)) for e in events)


def configure_control(CC, CS, events, enabled, always_lateral, driving, fresh, now_ns,
                      long_request=False, refuse_long=False, refuse_after=0):
  CC.lx3Authority = CS.lx3Authority
  a = CC.lx3Authority
  ready = fresh and a.buttonHealthy and verified(a)
  lat = ready and driving and CS.latEnabled and lateral_events_clear(events, always_lateral)
  lat = lat and not CS.steerFaultTemporary and not CS.steerFaultPermanent
  if not always_lateral and (CS.brakePressed or (CS.gasPressed and a.config & 2)):
    lat = False
  request_clear = not any(e.noEntry or e.immediateDisable or e.softDisable or e.userDisable for e in events)
  long = ready and (enabled or (long_request and request_clear)) and not CS.brakePressed and not (CS.gasPressed and a.config & 2)
  long = long and not refuse_long
  a.refuseLong = refuse_long
  a.refuseAfterSequence = refuse_after if refuse_long else 0
  a.intent = (LAT if lat else 0) | (LONG if long else 0)
  a.decisionKey, a.pendingGeneration, a.intent = cited_decision(a, a.intent, now_ns)
  a.observedLongRevision = a.longitudinalRevision
  a.autoResume = a.autoResume and long
  pedal_suspend = (not always_lateral and (CS.brakePressed or (CS.gasPressed and a.config & 2)) and
                   bool(a.armed & LONG) and lateral_events_clear(events, True))
  # Do not resurrect a rejected pending LFA request when a short fault clears
  # before its citation expires. Keep full-session pedal resume as a suspend.
  # A short freshness gap immediately makes output/intent inactive, but only
  # an actual evaluated refusal latches the driver's request off. A sustained
  # gap still revokes through the normal native heartbeat/host-off path.
  a.lateralRefused = bool(CS.latEnabled and ready and not lat and not pedal_suspend)
  return bool(lat and a.allowed & LAT and not a.oemEmergency), bool(long and a.allowed & LONG)
