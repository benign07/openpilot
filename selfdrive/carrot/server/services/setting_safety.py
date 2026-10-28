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


def settings_write_lock(request):
  """Serialize HUD profile/individual writes within this server process."""
  if 'settings_write_lock' not in request.app:
    request.app['settings_write_lock'] = asyncio.Lock()
  return request.app['settings_write_lock']


async def settled_setting_write(function, *args, terminal=False):
  """Keep the write lock until a thread settles even if its HTTP task cancels."""
  from concurrent.futures import ThreadPoolExecutor
  global _settings_executor
  try: executor = _settings_executor
  except NameError:
    executor = _settings_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='carrot-settings')
  task = asyncio.get_running_loop().run_in_executor(executor, function, *args)
  cancelled = False
  while not task.done():
    try:
      await asyncio.shield(task)
    except asyncio.CancelledError:
      cancelled = True
  if cancelled and not terminal:
    # The durable profile intent remains pending; never claim completion.
    try: task.result()
    except Exception: pass
    raise asyncio.CancelledError()
  return task.result()


async def parked_write_proof(request):
  await require_parked(request)
  return {'sm': request.app['realtime_broker'].sm, 'observed': time.monotonic()}


def check_parked_write_proof(proof):
  now = time.monotonic()
  if not 0 <= now - proof['observed'] < .5:
    raise ValueError('주차 확인 이후 쓰기 대기가 길어졌습니다. 다시 확인하세요.')
  error = parked_state_error(proof['sm'], now)
  if error:
    raise ValueError(error)


def serialize_settings_mutation(handler):
  """Legacy bulk writes share the same process lock; external writers do not."""
  from functools import wraps
  @wraps(handler)
  async def locked(request):
    async with settings_write_lock(request):
      await legacy_settings_maintenance_safe(request)
      return await handler(request)
  return locked


async def legacy_settings_maintenance_safe(request):
  # Preserve the existing verified-offroad maintenance path for legacy backups.
  from ..features.system import _require_maintenance_safe
  await _require_maintenance_safe(request)
