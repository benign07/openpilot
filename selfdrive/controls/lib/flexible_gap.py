"""Opt-in comfort reference, never a replacement for the MPC danger constraint.

70% refers to the moving time-gap component, not braking/standstill distance.
The 1.25 s floor is the stock aggressive time gap, not a safety certification.
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

  def update(self, settings, leads, *, eligible, v_ego, a_ego, base_tf, stop_distance, comfort_brake):
    leads = tuple(leads)
    values = (v_ego, a_ego, base_tf, stop_distance, comfort_brake)
    if (not settings.enabled or settings.minimum_ratio >= 100 or not eligible
        or not all(math.isfinite(x) for x in values)
        or v_ego < 5.0 or not -0.2 <= a_ego <= 0.1
        or base_tf <= MIN_FLEX_TF or stop_distance < 0.0 or comfort_brake <= 0.0
        or not leads or not leads[0].status):
      self.reset()
      return 0.0

    observed = {}
    for lead in leads:
      if not lead.status:
        continue
      d, vl, vr, al, jerk, prob = lead.dRel, lead.vLead, lead.vRel, lead.aLeadK, lead.jLead, lead.modelProb
      if (not lead.radar or lead.radarTrackId < 0 or not all(math.isfinite(x) for x in (d, vl, vr, al, jerk, prob))
          or prob < 0.9 or vl < 0.0 or not -0.6 <= vr <= 0.15
          or al < -0.2 or abs(al - a_ego) > 0.2 or abs(jerk) > 0.2
          or abs(vl - v_ego - vr) > 0.5
          or d <= stop_distance + v_ego * MIN_FLEX_TF + max(0.0, -vr)**2 / (2.0 * comfort_brake)
          or d / max(0.01, -vr) < 12.0):
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

    permitted = min(1.0 - max(70, min(100, settings.minimum_ratio)) / 100.0,
                    max(0.0, 1.0 - MIN_FLEX_TF / base_tf))
    # Slow acquisition only. All risk/ineligibility exits above restore the
    # original planning reference immediately, not the physical range.
    self.reduction = min(permitted, self.reduction + 0.03 * self.dt)
    return base_tf * self.reduction
