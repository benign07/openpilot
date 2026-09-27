"""Fail closed for control settings and maintenance while a vehicle can move."""
from __future__ import annotations

import asyncio
import json
import math
import time

from aiohttp import web


DISPLAY_SETTINGS = frozenset({
  "IsMetric", "LanguageSetting", "ShowDateTime", "ShowDebugUI", "ShowDeviceState",
  "ShowLaneInfo", "ShowRadarInfo", "ShowPathMode", "ShowPathColor", "ShowPathColorLane",
  "ShowPathModeLane", "ShowPathColorCruiseOff", "ShowPlotMode", "ShowCustomBrightness",
  "ShowModelView", "SoundVolumeAdjust", "SoundVolumeAdjustEngage",
})


def parked_state_error(sm, now: float | None = None) -> str | None:
  """Require fresh valid CAN state plus inactive actual actuator requests."""
  now = time.monotonic() if now is None else now
  try:
    for name in ("carState", "carControl", "selfdriveState"):
      age = now - sm.logMonoTime[name] / 1e9
      if not sm.alive[name] or not sm.valid[name] or not 0 <= age < 0.5:
        return "차량 상태를 확인할 수 없습니다. 기기 연결 후 다시 시도하세요."
    cs, cc, sd = (sm[name] for name in ("carState", "carControl", "selfdriveState"))
    if not cs.canValid or not math.isfinite(cs.vEgo) or abs(cs.vEgo) >= 0.03 or str(cs.gearShifter) != "park":
      return "정차 후 P단에서 변경할 수 있습니다."
    if any((cc.enabled, cc.latActive, cc.longActive, sd.enabled, sd.active)):
      return "정차 후 조향·가감속 제어를 모두 해제하세요."
  except (AttributeError, KeyError, TypeError, ValueError):
    return "차량 상태를 확인할 수 없습니다. 기기 연결 후 다시 시도하세요."
  return None


async def require_parked(request) -> None:
  broker = request.app.get("realtime_broker")
  lock = request.app.get("realtime_broker_poll_lock")
  reason = "차량 상태를 확인할 수 없습니다. 기기 연결 후 다시 시도하세요."
  if broker is not None and lock is not None:
    try:
      async with lock:
        await asyncio.to_thread(broker.poll, 0)
        reason = parked_state_error(broker.sm)
    except Exception:
      pass
  if reason is not None:
    raise web.HTTPConflict(text=json.dumps({"ok": False, "error": reason, "error_code": "PARKED_REQUIRED"}, ensure_ascii=False), content_type="application/json")
