import os

from aiohttp import web

from ..config import UNIT_CYCLE
from ..services.params import HAS_PARAMS, ParamKeyType, Params, get_param_values
from ..services.settings import get_settings_cached, settings_cache


async def api_settings(request: web.Request) -> web.Response:
  path = settings_cache["path"]
  if not os.path.exists(path):
    return web.json_response({"ok": False, "error": f"settings file not found: {path}"}, status=404)

  try:
    data, groups, by_name, groups_list = get_settings_cached()
    steering_type = None
    if HAS_PARAMS:
      try:
        from cereal import car
        with car.CarParams.from_bytes(Params().get("CarParams")) as cp:
          steering_type = str(cp.steerControlType)
      except Exception:
        pass
    current_values = get_param_values(list(by_name), {n: p.get("default", 0) for n, p in by_name.items()})
    # keep insertion order of groups
    items_by_group = {g: items for g, items in groups.items()}
    return web.json_response({
      "ok": True,
      "path": path,
      "apilot": data.get("apilot"),
      "groups": groups_list,
      "items_by_group": items_by_group,
      "categories": settings_cache.get("categories"),  # 대>중>소 트리 (없으면 None → 프런트 폴백)
      "unit_cycle": UNIT_CYCLE,
      "has_params": HAS_PARAMS,
      "current_values": current_values,
      "steering_type": steering_type,
      "has_param_type": bool(ParamKeyType is not None and hasattr(Params(), "get_type")) if HAS_PARAMS else False,
    })
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


def register(app: web.Application) -> None:
  app.router.add_get("/api/settings", api_settings)
