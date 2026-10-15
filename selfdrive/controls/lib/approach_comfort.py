"""Optional LX3 lead-approach preferences; never an actuator/authority path.

The planner applies an upper acceleration ceiling before MPC. Extra distance
is a comfort reference above the existing legacy Carrot following target.
Legacy dynamic/deceleration TF adjustments remain in that base target.
Physical obstacles, braking bounds, FCW and standstill hold are unchanged.
"""

from dataclasses import dataclass
import json
import math
import time
import uuid
import os
from pathlib import Path
import threading


# These are versioned application preferences, not native Params keys. A source
# OTA must never bypass Params.check_key or require a new native Params binary.
DRIVING_PREFERENCES = {
  'LaunchFollowStrength': (0, 20, 0),
  'YellowLineBiasCm': (0, 10, 0),
  'NearbyVehicleBiasCm': (0, 10, 0),
  'LateralMarginShadow': (0, 1, 1),
}
_PREFERENCE_LOCK = threading.RLock()


class DrivingPreferences:
  def __init__(self, path='/data/community/driving_preferences.json'):
    self.path = Path(path)
    self.signature = None
    self.values = {k: v[2] for k, v in DRIVING_PREFERENCES.items()}
    self.error = None

  def read(self):
    defaults = {k: v[2] for k, v in DRIVING_PREFERENCES.items()}
    try:
      stat = self.path.stat()
      signature = (stat.st_mtime_ns, stat.st_size)
      if signature == self.signature:
        return dict(self.values)
      if self.path.is_symlink() or not 0 < stat.st_size <= 8192:
        raise ValueError('invalid driving preference file')
      row = json.loads(self.path.read_text(encoding='utf-8'))
      if not isinstance(row, dict) or row.get('schema') != 1 or not isinstance(row.get('values'), dict):
        raise ValueError('unsupported driving preference schema')
      values = row['values']
      if set(values) - set(defaults):
        raise ValueError('unknown driving preference')
      for name, value in values.items():
        lo, hi, _ = DRIVING_PREFERENCES[name]
        if type(value) is not int or not lo <= value <= hi:
          raise ValueError('invalid driving preference value')
      self.values = {**defaults, **values}
      self.error = None
      self.signature = signature
    except FileNotFoundError:
      self.values, self.signature, self.error = defaults, None, None
    except (OSError, ValueError, TypeError):
      self.values, self.signature, self.error = defaults, None, 'invalid_preferences_features_off'
    return dict(self.values)

  def set(self, name, value):
    if name not in DRIVING_PREFERENCES:
      raise ValueError('unknown driving preference')
    lo, hi, _ = DRIVING_PREFERENCES[name]
    if type(value) is not int or not lo <= value <= hi:
      raise ValueError('invalid driving preference value')
    with _PREFERENCE_LOCK:
      self.signature = None
      values = self.read()
      if self.error:
        raise ValueError('driving preference file needs repair; preserved original')
      values[name] = value
      self.path.parent.mkdir(parents=True, exist_ok=True)
      temporary = self.path.with_name(self.path.name + '.' + uuid.uuid4().hex + '.tmp')
      try:
        with temporary.open('x', encoding='utf-8') as stream:
          json.dump({'schema': 1, 'values': values}, stream, allow_nan=False)
          stream.flush()
          os.fsync(stream.fileno())
        os.replace(temporary, self.path)
        if os.name == 'posix':
          fd = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
          try: os.fsync(fd)
          finally: os.close(fd)
      finally:
        temporary.unlink(missing_ok=True)
      self.signature = None
      return self.read()[name]


class LaunchFollow:
  """Optional positive ceiling allowance behind a confirmed opening lead.

  It never changes starting permission, hold, physical obstacles or braking.
  An increased capability is not a guaranteed command or reduced road gap.
  """
  def __init__(self, dt=0.05):
    self.dt = dt
    self.track = None
    self.since_ns = self.last_ns = 0
    self.stopped_ns = 0
    self.extra = 0.0
    self.reason = 'off'

  def update(self, strength, radar, *, stamp_ns, eligible, should_stop, v_ego,
             desired_distance, table_max, approach_active):
    self.reason = 'off'
    previous_extra, self.extra = self.extra, 0.0
    lead = radar.leadOne
    stamp_ok = stamp_ns > 0 and stamp_ns >= self.last_ns
    lead_ok = (lead.status and lead.radar and lead.radarTrackId >= 0 and
               all(math.isfinite(x) for x in (lead.dRel, lead.vRel, lead.vLead, lead.aLeadK)))
    if not stamp_ok or not lead_ok or lead.radarTrackId != self.track or stamp_ns-self.last_ns > 200_000_000:
      self.track = lead.radarTrackId if lead_ok and stamp_ok else None
      self.since_ns = stamp_ns
    self.last_ns = stamp_ns
    if not strength or not eligible:
      self.track = None
      self.stopped_ns = 0
      return table_max
    if v_ego <= 0.15:
      self.stopped_ns = stamp_ns
    if not lead_ok or not stamp_ok or stamp_ns-self.since_ns < 1_000_000_000:
      self.reason = 'track_unqualified'
      return table_max
    self.reason = 'hold_or_approach'
    if should_stop or v_ego <= 0.5 or approach_active:
      return table_max
    if not self.stopped_ns or not 0 <= stamp_ns-self.stopped_ns < 25_000_000_000:
      self.reason = 'outside_launch_window'
      return table_max
    self.reason = 'lead_not_opening'
    if lead.vRel <= 0.5 or lead.vLead <= 1.0 or lead.aLeadK < 0.0 or lead.dRel <= desired_distance+2.0:
      return table_max
    other = radar.leadTwo
    self.reason = 'second_lead_or_cutin'
    if other.status and (not all(math.isfinite(x) for x in (other.dRel, other.vRel, other.aLeadK)) or
                         other.dRel < lead.dRel or other.vRel < 0.0 or other.aLeadK < 0.0):
      return table_max
    if any(t.status and (not math.isfinite(t.dRel) or 0 < t.dRel < 50) for t in radar.leadsCutIn):
      return table_max
    if not all(math.isfinite(x) for x in (v_ego, desired_distance, table_max)) or table_max <= 0:
      self.reason = 'invalid_limits'
      return table_max
    amount = clamp(strength, 0, 20) * 0.01 * clamp((lead.vRel-0.5)/2.5, 0., 1.)
    amount *= clamp((60/3.6-v_ego)/(20/3.6), 0., 1.)
    allowed = min(1.6, table_max*(1.+amount))
    self.reason = 'opening_allowance' if allowed > table_max else 'speed_or_stock_limit'
    # A higher stock table must never be lowered by this optional allowance.
    self.extra = min(max(0., allowed-table_max), previous_extra+0.15*self.dt)
    return table_max+self.extra if self.extra else table_max


def clamp(value, low, high):
  return min(high, max(low, value))


@dataclass(frozen=True)
class ApproachSettings:
  enabled: bool = False
  coast: int = 40
  margin: int = 40
  smooth: int = 40
  traffic: int = 40

  @classmethod
  def read(cls, params):
    return cls(params.get_bool("ApproachComfortEnabled"), *(
      int(clamp(params.get_int(name), 0, 100)) for name in (
        "ApproachCoastStrength", "ApproachBrakeMargin", "ApproachStopSmooth", "ApproachTrafficCalm",
      )
    ))


@dataclass(frozen=True)
class ApproachRequest:
  accel_ceiling: float | None = None
  extra_tf: float = 0.0
  jerk_factor: float = 1.0


class ApproachComfort:
  def __init__(self, dt=0.05):
    self.dt = dt
    self.reset()

  def reset(self):
    self.tracks = {}
    self.extra_tf = 0.0
    self.coast_weight = 0.0
    self.traffic_weight = 0.0
    self.jerk_factor = 1.0
    self.smooth_ready_time = 0.0
    self.smooth_engaged = False

  def update(self, settings, leads, *, eligible, v_ego, a_ego, base_tf, stop_distance, maximum_accel, comfort_brake=2.5, cruise_speed=None, planned_accel=None):
    values = (v_ego, a_ego, base_tf, stop_distance, maximum_accel, comfort_brake)
    if not settings.enabled or not eligible or not all(math.isfinite(x) for x in values):
      self.reset()
      return ApproachRequest()
    if v_ego < 0.0 or base_tf <= 0.0 or stop_distance < 0.0 or comfort_brake <= 0.0:
      self.reset()
      return ApproachRequest()

    valid = []
    next_tracks = {}
    for lead in leads:
      if not lead.status or not lead.radar or lead.radarTrackId < 0:
        continue
      d, vl, vr, al = lead.dRel, lead.vLead, lead.vRel, lead.aLeadK
      if not all(math.isfinite(x) for x in (d, vl, vr, al)) or d <= 0.0 or vl < 0.0 or abs(vr) > 80.0:
        continue
      # Count a track only once even if both selected slots contain it.
      key = int(lead.radarTrackId)
      next_tracks[key] = self.tracks.get(key, 0) + 1
      if next_tracks[key] >= 3 and key not in {item[0] for item in valid}:
        valid.append((key, d, vl, vr, al))
    self.tracks = next_tracks
    if not valid:
      self.extra_tf = self.coast_weight = self.traffic_weight = 0.0
      self.jerk_factor = 1.0
      self.smooth_ready_time = 0.0
      self.smooth_engaged = False
      return ApproachRequest()

    risk = traffic = smooth = 0.0
    for _, d, vl, vr, al in valid:
      closing = max(0.0, -vr)
      # Only slow/braking leads which we will approach within a bounded horizon.
      # Far braking traffic does not suppress acceleration indefinitely.
      # Early braking must cover ordinary mid-speed approaches too. This is
      # a bounded cue window, not a guarantee of a particular braking point.
      # Extending the preview at walking speed suppressed a launch behind an
      # opening lead, then left a later stop to catch up. Keep the base window
      # in stop-go; the extra early-braking window grows with road speed.
      horizon = 4.0 + 3.0 * clamp(settings.margin, 0, 100) / 100.0 * clamp((v_ego - 5.0) / 9.0, 0.0, 1.0)
      lead_decel = clamp(-al, 0.0, 3.0)
      projected_closing = closing + min(vl, lead_decel * horizon) * 0.5
      # Preview actual positive planning intent, never the global capability.
      # Equal-speed cruising at its target and a launch from rest must not
      # acquire an extra coasting constraint just because acceleration exists.
      preview_accel = 0.0
      if cruise_speed is not None and planned_accel is not None and math.isfinite(cruise_speed) and math.isfinite(planned_accel):
        slow_to_target = clamp((cruise_speed - vl - 0.5) / 1.0, 0.0, 1.0)
        opening_fade = clamp((1.0 - vr) / 0.5, 0.0, 1.0)
        moving_fade = clamp((v_ego - 0.5) / 1.0, 0.0, 1.0)
        preview_accel = clamp(planned_accel, 0.0, max(0.0, maximum_accel)) * slow_to_target * opening_fade * moving_fade
      preview_closing = preview_accel * horizon
      reference = max(1.0, stop_distance + v_ego * base_tf +
                      max(0.0, (v_ego * v_ego - vl * vl) / (2.0 * comfort_brake)))
      window = (reference + closing * horizon + 0.5 * preview_accel * horizon * horizon +
                min(vl * horizon, lead_decel * horizon * horizon * 0.5))
      cue = max(clamp((closing - 0.3) / 2.0, 0.0, 1.0), clamp((lead_decel - 0.2) / 0.8, 0.0, 1.0),
                clamp((closing + preview_closing - 0.3) / 2.0, 0.0, 1.0) if preview_accel > 0.0 else 0.0)
      # Do not respond to a much faster lead merely because it is decelerating.
      cue *= clamp((projected_closing + preview_closing - max(0.0, vr)) / 1.0, 0.0, 1.0)
      proximity = clamp((window - d) / max(10.0, window - reference), 0.0, 1.0)
      risk = max(risk, cue * proximity)
      if v_ego < 6.0 and vl < 6.0 and vr <= 0.5 and d < reference + 10.0:
        traffic = max(traffic, clamp((6.0 - v_ego) / 4.0, 0.0, 1.0))
      # More jerk cost only in a mild, low-speed, well-spaced final approach.
      # Any close/rapidly closing selected lead disables this comfort modifier.
      clearance = 2.0 if self.smooth_engaged else 2.5
      if v_ego < 5.0 and vl < 2.0 and -0.8 < a_ego < 0.1 and closing < 1.5 and d > stop_distance + clearance + closing * 1.5:
        smooth = max(smooth, clamp((5.0 - v_ego) / 3.0, 0.0, 1.0))
    unsafe_to_smooth = any(
      d <= stop_distance + 2.0 + max(0.0, -vr) * 1.5 or vr < -1.5
      for _, d, _, vr, _ in valid
    )
    if unsafe_to_smooth or a_ego <= -0.8:
      smooth = 0.0
    self.smooth_ready_time = self.smooth_ready_time + self.dt if smooth > 0.0 else 0.0
    if smooth <= 0.0:
      self.smooth_engaged = False
    elif self.smooth_ready_time >= 0.5 - 1e-9:
      self.smooth_engaged = True

    target_tf = clamp(settings.margin, 0, 100) * 0.005 * clamp((v_ego - 2.0) / 12.0, 0.0, 1.0) * risk
    # Add headroom early; release slowly through deceleration, never below base.
    # Match legacy apply_t_follow's rise rate; soft headroom must not bypass
    # the existing preference for slowly increasing the following target.
    self.extra_tf += clamp(target_tf - self.extra_tf, -0.08 * self.dt, 0.10 * self.dt)
    for name, target in (("coast_weight", risk), ("traffic_weight", traffic)):
      old = getattr(self, name)
      release_rate = 0.25 + 0.75 * min(clamp((vr - 0.5) / 0.5, 0.0, 1.0) for _, _, _, vr, _ in valid)
      setattr(self, name, old + clamp(target - old, -release_rate * self.dt, 1.0 * self.dt))

    # The strength adjusts how early a confirmed approach can reach a zero
    # positive ceiling. At 40, a strong cue (at least 5/6) reaches zero;
    # it never becomes a negative brake command. MPC entry feasibility and
    # all existing negative braking bounds remain the planner's responsibility.
    reduction = max(clamp(self.coast_weight * clamp(settings.coast, 0, 100) * 0.03, 0.0, 1.0),
                    self.traffic_weight * clamp(settings.traffic, 0, 100) * 0.005)
    ceiling = None if reduction <= 0.0 else max(0.0, maximum_accel) * (1.0 - reduction)
    # Zero upper acceleration means no acceleration request, not guaranteed
    # physical coasting on every grade/powertrain. Negative MPC braking remains.
    if smooth > 0.0 and self.smooth_engaged:
      target_jerk = 1.0 + smooth * clamp(settings.smooth, 0, 100) * 0.005
      self.jerk_factor = min(target_jerk, self.jerk_factor + 0.5 * self.dt)
    elif unsafe_to_smooth or a_ego <= -0.8:
      self.jerk_factor = 1.0
    else:
      self.jerk_factor = max(1.0, self.jerk_factor - 0.5 * self.dt)
    return ApproachRequest(ceiling, self.extra_tf, self.jerk_factor)


def apply_approach_ceiling(maximum_accel, a_desired, request, dt=0.05):
  """Slew only the extra positive ceiling; negative braking stays available."""
  if request.accel_ceiling is None:
    return maximum_accel
  # A rapid downward positive bound forced a sharper acceleration transition
  # in linked native comparisons. Allow 0.4 m/s^3 of ceiling reduction. This
  # is not a hard jerk limit or a lower bound on the requested acceleration:
  # MPC can still brake harder for a lead, and stricter stock ceilings win.
  return min(maximum_accel, max(request.accel_ceiling, a_desired - 0.4 * dt))


def additional_approach_tf(applied_tf, nominal_tf, requested_extra):
  """Credit existing stock headroom; never subtract stock TF or update its state.

  Mode, speed scaling, deceleration hold/boost and dynamic TF run first. The
  early preference asks for headroom above the nominal (unboosted) request,
  rather than adding the same allowance again above an already boosted TF.
  """
  if not math.isfinite(requested_extra):
    return 0.0
  existing = max(0.0, applied_tf - nominal_tf) if math.isfinite(nominal_tf) and nominal_tf > 0.0 else 0.0
  return min(max(0.0, clamp(requested_extra, 0.0, 0.5) - existing), max(0.0, 2.5 - applied_tf))


def traffic_ceiling_comparison(approach, settings, maximum_accel, a_desired, dt=0.05):
  """Read the CURRENT filter state; never advance a second control calculation.

  This compares only a positive ceiling. It is neither a solved alternative
  command nor a vehicle timing/comfort counterfactual. Stock bounds still win.
  """
  result = {}
  for traffic in (30, 40):
    coast = clamp(approach.coast_weight * clamp(settings.coast, 0, 100) * .03, 0., 1.)
    congestion = approach.traffic_weight * traffic * .005
    reduction = max(coast, congestion) if settings.enabled else 0.
    raw = None if reduction <= 0. else max(0., maximum_accel) * (1. - reduction)
    applied = apply_approach_ceiling(maximum_accel, a_desired, ApproachRequest(raw), dt)
    result[str(traffic)] = {'raw_ceiling': raw, 'applied_ceiling': float(applied),
                            'winning_term': 'disabled' if not settings.enabled else (
                              'coast' if coast > congestion else 'traffic' if congestion > coast else 'tie')}
  return result


class LongitudinalEvidence:
  """Bounded producer on existing nonblocking swaglog IPC, with no file/CAN I/O.

  Every LX3 low-speed/approach cycle is recorded, ordinary cruising one in four.
  Logging failures affect only evidence. Missing sequences are not zero effect.
  """
  KEY = 'lx3_longitudinal_evidence'
  MAX_BYTES = 32768

  def __init__(self, enabled=False):
    self.enabled = enabled
    self.producer = uuid.uuid4().hex
    self.cycle = self.sequence = self.errors = 0
    self.max_emit_us = 0.
    self.settings_generation = 0
    self.previous_settings = None
    self.cooldown = 0
    self.last_error = None

  def note_error(self, exc):
    self.errors += 1
    self.last_error = f'{type(exc).__name__}: {str(exc)[:140]}'

  def due(self, high_rate):
    self.cycle += 1
    if self.cooldown:
      self.cooldown -= 1
      return False
    return self.enabled and (high_rate or self.cycle % 4 == 0)

  def emit(self, build, sink):
    if not self.enabled:
      return
    self.sequence += 1
    started = time.perf_counter_ns()
    try:
      row = {'schema': 1, 'producer': self.producer, 'sequence': self.sequence,
             'cycle': self.cycle, 'mono_ns': time.monotonic_ns(),
             'producer_errors': self.errors, 'previous_max_emit_us': self.max_emit_us,
             'last_error': self.last_error, 'record_type': 'frame',
             **build()}
      boot_clock = getattr(time, 'CLOCK_BOOTTIME', None)
      before = time.monotonic_ns()
      native = time.clock_gettime_ns(boot_clock) if boot_clock is not None else None
      after = time.monotonic_ns()
      row['native_clock_offset_ns'] = (native - (before + after)//2) if native is not None else None
      if row.get('settings') != self.previous_settings:
        self.settings_generation += 1
        self.previous_settings = row.get('settings')
      row['settings_generation'] = self.settings_generation
      # Reject nonfinite/oversized evidence, never change a control value.
      size = len(json.dumps(row, separators=(',', ':'), allow_nan=False).encode())
      if size > self.MAX_BYTES:
        raise ValueError('longitudinal evidence exceeds bounded frame size')
      if (time.perf_counter_ns() - started) / 1000. + row.get('snapshot_us', 0.) + (row.get('problem') or {}).get('copy_us', 0.) > 5000.:
        self.cooldown = 20
        raise ValueError('diagnostic build exceeded 5ms; skip one second')
      sink({self.KEY: row})
    except Exception as exc:
      self.note_error(exc)
    finally:
      self.max_emit_us = max(self.max_emit_us, (time.perf_counter_ns() - started) / 1000.)
