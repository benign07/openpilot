"""Optional LX3 lead-approach preferences; never an actuator/authority path.

The planner applies an upper acceleration ceiling before MPC. Extra distance
is a comfort reference only, combined by max with Carrot's existing headroom.
Physical obstacles, braking bounds, FCW and standstill hold are unchanged.
"""

from dataclasses import dataclass
import math


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

  def update(self, settings, leads, *, eligible, v_ego, a_ego, base_tf, stop_distance, maximum_accel, comfort_brake=2.5):
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
      horizon = 4.0
      lead_decel = clamp(-al, 0.0, 3.0)
      projected_closing = closing + min(vl, lead_decel * horizon) * 0.5
      reference = max(1.0, stop_distance + v_ego * base_tf +
                      max(0.0, (v_ego * v_ego - vl * vl) / (2.0 * comfort_brake)))
      window = reference + closing * horizon + min(vl * horizon, lead_decel * horizon * horizon * 0.5)
      cue = max(clamp((closing - 0.3) / 2.0, 0.0, 1.0), clamp((lead_decel - 0.2) / 0.8, 0.0, 1.0))
      # Do not respond to a much faster lead merely because it is decelerating.
      cue *= clamp((projected_closing - max(0.0, vr)) / 1.0, 0.0, 1.0)
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

    target_tf = clamp(settings.margin, 0, 100) * 0.005 * clamp((v_ego - 3.0) / 22.0, 0.0, 1.0) * risk
    # Add headroom early; release slowly through deceleration, never below base.
    self.extra_tf += clamp(target_tf - self.extra_tf, -0.08 * self.dt, 0.30 * self.dt)
    for name, target in (("coast_weight", risk), ("traffic_weight", traffic)):
      old = getattr(self, name)
      release_rate = 1.0 if name == 'traffic_weight' and all(vr > 0.5 for _, _, _, vr, _ in valid) else 0.25
      setattr(self, name, old + clamp(target - old, -release_rate * self.dt, 1.0 * self.dt))

    reduction = max(self.coast_weight * clamp(settings.coast, 0, 100) / 100.0,
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


def apply_approach_ceiling(maximum_accel, a_desired, request):
  """Keep existing negative ceilings and the planner's feasible entry bound."""
  if request.accel_ceiling is None:
    return maximum_accel
  return min(maximum_accel, max(request.accel_ceiling, a_desired - 0.05))
