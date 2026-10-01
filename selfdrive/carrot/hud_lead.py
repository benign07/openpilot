"""Display-only lead selection; radar and control roles remain unchanged."""
import math


def select_display_lead(radar_state):
  # Adapted from carrot-wip abe1a232, hyundaicanfd._display_lead.
  # Equal distances retain leadOne. Neither control lead is modified.
  leads = (getattr(radar_state, name, None) for name in ('leadOne', 'leadTwo'))
  return min((lead for lead in leads
              if lead is not None and lead.status and lead.dRel > 0
              and all(math.isfinite(v) for v in (lead.dRel, lead.yRel, lead.vRel))),
             key=lambda lead: lead.dRel, default=None)
