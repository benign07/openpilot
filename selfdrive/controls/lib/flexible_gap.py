"""Opt-in comfort reference, never a replacement for the MPC danger constraint.

70% refers to the moving time-gap component, not braking/standstill distance.
The cruising branch retains its 1.25 s floor. The opt-in mild-stop branch
uses only a fraction of the owner's nominal moving gap. Neither branch
reduces the absolute standstill/braking terms or certifies road safety.
"""
from dataclasses import dataclass
import math

MIN_FLEX_TF = 1.25


@dataclass(frozen=True)
class FlexibleGapSettings:
  enabled: bool = False
  minimum_ratio: int = 70

  @classmethod
  def read(cls, params):
    return cls(params.get_bool('ApproachFlexibleGap'), max(70, min(100, params.get_int('ApproachFlexibleGapMin'))))


class FlexibleGap:
  def __init__(self, dt=0.05):
    self.dt = dt
    self.reset()

  def reset(self):
    self.tracks = None
    self.previous = {}
    self.stable_time = 0.0
    self.reduction = 0.0
    self.gentle_stop = False

  def update(self, settings, leads, *, eligible, v_ego, a_ego, base_tf, stop_distance, comfort_brake, gentle_stop=False):
    leads = tuple(leads)
    if bool(gentle_stop) != self.gentle_stop:
      self.reset()
      self.gentle_stop = bool(gentle_stop)
    ratio = max(70, min(100, settings.minimum_ratio)) / 100.0
    floor_tf = base_tf * ratio if gentle_stop else MIN_FLEX_TF
    speed_ok = 1.0 <= v_ego < 6.0 if gentle_stop else v_ego >= 5.0
    accel_ok = -0.8 < a_ego < -0.1 if gentle_stop else -0.2 <= a_ego <= 0.1
    values = (v_ego, a_ego, base_tf, stop_distance, comfort_brake)
    if (not settings.enabled or settings.minimum_ratio >= 100 or not eligible
        or not all(math.isfinite(x) for x in values)
        or not speed_ok or not accel_ok
        or base_tf <= max(0.0, floor_tf) or stop_distance < 0.0 or comfort_brake <= 0.0
        or not leads or not leads[0].status):
      self.reset()
      return 0.0

    observed = {}
    for lead in leads:
      if not lead.status:
        continue
      d, vl, vr, al, jerk, prob = lead.dRel, lead.vLead, lead.vRel, lead.aLeadK, lead.jLead, lead.modelProb
      if (not lead.radar or lead.radarTrackId < 0 or not all(math.isfinite(x) for x in (d, vl, vr, al, jerk, prob))
          or prob < 0.9 or vl < 0.0 or not (-0.5 <= vr <= 0.05 if gentle_stop else -0.6 <= vr <= 0.15)
          or not (-0.8 < al <= 0.0 if gentle_stop else al >= -0.2)
          or abs(al - a_ego) > (0.3 if gentle_stop else 0.2) or abs(jerk) > (0.3 if gentle_stop else 0.2)
          or abs(vl - v_ego - vr) > 0.5
          or d <= stop_distance + v_ego * floor_tf + max(0.0, -vr)**2 / (2.0 * comfort_brake) + (0.5 if gentle_stop else 0.0)
          or d / max(0.01, -vr) < (20.0 if gentle_stop else 12.0)):
        self.reset()
        return 0.0
      key = int(lead.radarTrackId)
      if key in observed and abs(d - observed[key][0]) > 0.5:
        self.reset()
        return 0.0
      observed[key] = (d, vr)

    primary = leads[0]
    nominal_range = (stop_distance + v_ego * base_tf
                     + max(0.0, v_ego**2 - primary.vLead**2) / (2.0 * comfort_brake))
    if primary.dRel > nominal_range + max(2.0, 0.1 * nominal_range):
      # A distant mildly slower lead must not suppress ordinary acceleration.
      self.reset()
      return 0.0

    # Primary/secondary swaps and cut-ins must reconfirm; an uncertain second
    # selected lead cannot be ignored just because the primary is stable.
    identity = tuple(int(lead.radarTrackId) if lead.status else None for lead in leads)
    if not observed or identity != self.tracks:
      self.reset()
      self.gentle_stop = bool(gentle_stop)
      self.tracks = identity if observed else None
      self.previous = observed
      return 0.0

    for key, (d, vr) in observed.items():
      old_d, old_vr = self.previous[key]
      # Discontinuous range/relative velocity cancels even with an unchanged ID.
      if abs(d - (old_d + old_vr * self.dt)) > 0.5 or abs(vr - old_vr) > 0.1:
        self.reset()
        return 0.0
    self.previous = observed
    closing = any(vr <= -0.1 for _, vr in observed.values())
    if not closing and self.reduction == 0.0:
      self.stable_time = 0.0
      return 0.0
    self.stable_time += self.dt
    if self.stable_time + 1e-9 < 3.0:
      return 0.0

    permitted = min(1.0 - ratio, max(0.0, 1.0 - floor_tf / base_tf))
    # Slow acquisition only. All risk/ineligibility exits above restore the
    # original planning reference immediately, not the physical range.
    self.reduction = min(permitted, self.reduction + 0.03 * self.dt)
    return base_tf * self.reduction
