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


def speed_gap_reduction(v_ego, nominal_distance, stop_distance):
  """LX3 comfort target: 100% at rest, 90% at/above 100 km/h.

  The caller retains physical obstacles, braking constraints and any extra
  deceleration headroom. This is a reduction in metres, never a TF multiplier.
  Existing flexible-gap reduction is combined by max(), rather than added.
  """
  if (not all(math.isfinite(x) for x in (v_ego, nominal_distance, stop_distance))
      or v_ego <= 0.0 or stop_distance < 0.0 or nominal_distance <= stop_distance):
    return 0.0
  fraction = 0.10 * min(1.0, v_ego * 3.6 / 100.0)
  return min(nominal_distance - stop_distance, nominal_distance * fraction)


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
  DEFINITIONS = DRIVING_PREFERENCES

  def __init__(self, path='/data/community/driving_preferences.json'):
    self.path = Path(path)
    self.signature = None
    self.values = {k: v[2] for k, v in self.DEFINITIONS.items()}
    self.error = None

  def read(self):
    defaults = {k: v[2] for k, v in self.DEFINITIONS.items()}
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
        lo, hi, _ = self.DEFINITIONS[name]
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
    if name not in self.DEFINITIONS:
      raise ValueError('unknown driving preference')
    lo, hi, _ = self.DEFINITIONS[name]
    if type(value) is not int or not lo <= value <= hi:
      raise ValueError('invalid driving preference value')
    with _PREFERENCE_LOCK:
      self.signature = None
      values = self.read()
      if self.error:
        raise ValueError('driving preference file needs repair; preserved original')
      values[name] = value
      return self._write(values)[name]

  def reset_defaults(self):
    with _PREFERENCE_LOCK:
      self.signature = None
      self.read()
      if self.error and (self.path.exists() or self.path.is_symlink()):
        os.replace(self.path, self.path.with_name(self.path.name+'.invalid.'+uuid.uuid4().hex))
      return self._write({k:v[2] for k,v in self.DEFINITIONS.items()})

  def _write(self, values):
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
    return self.read()


CRUISE_BUTTON_PREFERENCES = {'CruiseOffKeepLateral': (0, 1, 0)}


class CruiseButtonPreferences(DrivingPreferences):
  """Separate file: rolling back cannot invalidate existing driving preferences."""
  DEFINITIONS = CRUISE_BUTTON_PREFERENCES

  def __init__(self, path='/data/community/cruise_button_preferences.json'):
    super().__init__(path)


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
    self.previous_lead = None
    self.relative_samples = []
    self.relative_speed_growing = False

  def observe_opening(self, stamp_ns, relative_speed, reset):
    if reset:
      self.relative_samples = []
    if stamp_ns > self.last_ns:
      self.relative_samples.append((stamp_ns, relative_speed))
    self.relative_samples = [(stamp, value) for stamp, value in self.relative_samples
                             if 0 <= stamp_ns-stamp <= 650_000_000][-32:]
    self.relative_speed_growing = False
    if not self.relative_samples or stamp_ns-self.relative_samples[0][0] < 550_000_000:
      return
    # Three means over about 0.6s reject a single spike/threshold chatter.
    # Only distinct radar timestamps advance the trend; no wall-time timer.
    bins = [[], [], []]
    for stamp, value in self.relative_samples:
      age = stamp_ns-stamp
      bins[0 if age >= 400_000_000 else 1 if age >= 200_000_000 else 2].append(value)
    if all(bins):
      old, middle, recent = [sum(values)/len(values) for values in bins]
      self.relative_speed_growing = (recent-old >= .2 and middle-old >= .05 and
                                     recent-middle >= .05 and relative_speed >= old+.2)

  def withdraw(self, table_max, reason, *, immediate=False):
    self.reason = reason
    self.extra = 0.0 if immediate else max(0.0, self.extra-0.3*self.dt)
    if immediate:
      self.relative_samples = []
      self.relative_speed_growing = False
    return table_max+self.extra if self.extra else table_max

  def update(self, strength, radar, *, stamp_ns, eligible, should_stop, v_ego,
             desired_distance, table_max, approach_active):
    self.reason = 'off'
    lead = radar.leadOne
    stamp_ok = stamp_ns > 0 and stamp_ns >= self.last_ns
    lead_ok = (lead.status and lead.radar and lead.radarTrackId >= 0 and
               all(math.isfinite(x) for x in (lead.dRel, lead.vRel, lead.vLead, lead.aLeadK)))
    continuous = False
    if lead_ok and self.previous_lead is not None:
      d, velocity = self.previous_lead
      delta = (stamp_ns-self.last_ns)/1e9
      continuous = (0 <= delta <= .2 and abs(lead.dRel-d-lead.vRel*delta)<1.5 and
                    abs(lead.vLead-velocity)<5.)
    reset_track = not stamp_ok or not lead_ok or lead.radarTrackId != self.track or not continuous
    if reset_track:
      self.track = lead.radarTrackId if lead_ok and stamp_ok else None
      self.since_ns = stamp_ns
      self.extra = 0.0
    if lead_ok and stamp_ok:
      self.observe_opening(stamp_ns, lead.vRel, reset_track)
    else:
      self.relative_samples = []
      self.relative_speed_growing = False
    self.previous_lead = (lead.dRel, lead.vLead) if lead_ok else None
    self.last_ns = stamp_ns
    if not strength or not eligible:
      self.track = None
      self.stopped_ns = 0
      return self.withdraw(table_max, 'off_or_ineligible', immediate=True)
    if v_ego <= 0.15:
      self.stopped_ns = stamp_ns
    if not lead_ok or not stamp_ok or stamp_ns-self.since_ns < 1_000_000_000:
      return self.withdraw(table_max, 'track_unqualified', immediate=True)
    if should_stop or v_ego <= 0.5 or approach_active:
      return self.withdraw(table_max, 'hold_or_approach', immediate=True)
    if not self.stopped_ns or not 0 <= stamp_ns-self.stopped_ns < 25_000_000_000:
      return self.withdraw(table_max, 'outside_launch_window')
    if not all(math.isfinite(x) for x in (v_ego, desired_distance, table_max)) or table_max <= 0:
      return self.withdraw(table_max, 'invalid_limits', immediate=True)
    other = radar.leadTwo
    # Selected slots may both name the first lead. Also inspect the exported
    # center tracks: a stopped queue beyond it can be absent from leadTwo.
    duplicate = other.status and getattr(other, 'radarTrackId', None) == lead.radarTrackId
    if other.status and not duplicate and (
        not all(math.isfinite(x) for x in (other.dRel, other.vRel, other.aLeadK)) or
        other.dRel < lead.dRel or other.vRel < 0.0 or other.aLeadK < 0.0):
      return self.withdraw(table_max, 'second_lead_or_cutin', immediate=True)
    for center in getattr(radar, 'leadsCenter', ()):
      if not center.status or getattr(center, 'radarTrackId', None) == lead.radarTrackId:
        continue
      if (not all(math.isfinite(x) for x in (center.dRel, center.vLead, center.aLeadK)) or
          (1.0 < center.dRel-lead.dRel < 60.0 and
           (center.vLead < 3.0 or center.vLead < lead.vLead-1.0 or center.aLeadK < -.15))):
        return self.withdraw(table_max, 'center_queue', immediate=True)
    if any(t.status and (not math.isfinite(t.dRel) or 0 < t.dRel < 50) for t in radar.leadsCutIn):
      return self.withdraw(table_max, 'second_lead_or_cutin', immediate=True)
    # Material closing/deceleration is a hard exit; mild measurement jitter
    # uses hysteresis and withdraws the optional capability gradually.
    if lead.vRel < 0.0 or lead.aLeadK < -.15 or lead.dRel <= desired_distance:
      return self.withdraw(table_max, 'closing_lead', immediate=True)
    active = self.extra > 0.0
    if (lead.vRel <= (.2 if active else .5) or lead.vLead <= 1.0 or
        lead.aLeadK < (-.05 if active else 0.0) or
        lead.dRel <= desired_distance+1.0):
      return self.withdraw(table_max, 'lead_not_opening')
    # Sustained growing separation can use the full configured allowance;
    # otherwise keep the existing relative-speed scaling. This raises a
    # capability only: MPC obstacles/jerk and stock turn/approach limits win.
    opening_factor = 1.0 if self.relative_speed_growing else clamp((lead.vRel-0.5)/2.5, 0., 1.)
    amount = clamp(strength, 0, 20) * 0.01 * opening_factor
    amount *= clamp((60/3.6-v_ego)/(20/3.6), 0., 1.)
    allowed = min(1.6, table_max*(1.+amount))
    self.reason = (('opening_growth_allowance' if self.relative_speed_growing else 'opening_allowance')
                   if allowed > table_max else 'speed_or_stock_limit')
    # A higher stock table must never be lowered by this optional allowance.
    target = max(0., allowed-table_max)
    self.extra += clamp(target-self.extra, -.3*self.dt, .15*self.dt)
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
