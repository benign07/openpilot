import asyncio
import os
import subprocess
import threading
import time
from typing import Any

from aiohttp import web

from ..live_runtime.broker import RealtimeBroker
from ..live_runtime.normalize import to_transport_safe
from ..config import OFFROAD_ASSETS_DIR
from ..services.device_info import get_calibration_status, get_device_network
from ..services.params import HAS_PARAMS, Params, restore_param_values_validated, driving_preferences, notification_preferences, cruise_button_preferences
from openpilot.selfdrive.controls.lib.notification_preferences import KEY as BORDER_KEY
from ..services.setting_safety import require_parked
from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES, CRUISE_BUTTON_PREFERENCES
from ..services.settings import get_settings_cached
from ..services.time_sync import TIME_SYNC_DEBUG_DEFAULT, sync_system_time_from_browser


_LIVE_RUNTIME_SERVICE_NAMES = (
  "selfdriveState",
  "carState",
  "controlsState",
  "deviceState",
  "peripheralState",
  "longitudinalPlan",
  "lateralPlan",
  "radarState",
  "carrotMan",
)

_CARROT_DEFAULT_RESET_EXCLUDED_NAMES = {
  # Vehicle identity / selection / fingerprinting.
  "CarName",
  "CarParams",
  "CarParamsCache",
  "CarParamsPersistent",
  "CarParamsPrevRoute",
  "CarModel",
  "CarFingerprint",
  "SupportedCars",
  # Training, terms, calibration, and learned live parameters.
  "CompletedTrainingVersion",
  "TrainingVersion",
  "TermsVersion",
  "HasAcceptedTerms",
  "CalibrationParams",
  "LiveParameters",
  "LiveTorqueParameters",
  # Device/user/system identity and platform settings.
  "DongleId",
  "HardwareSerial",
  "DeviceSerial",
  "DeviceType",
  "LanguageSetting",
  "IsMetric",
  "OpenpilotEnabledToggle",
  "ExperimentalMode",
  "ExperimentalModeConfirmed",
  "SshEnabled",
  "AdbEnabled",
  "GithubUsername",
  "GithubSshKeys",
  # Recording/upload/update/git state should not be reset by Carrot tuning reset.
  "RecordFront",
  "RecordAudio",
  "GitBranch",
  "GitCommit",
  "GitCommitDate",
  "UpdaterState",
  "UpdaterTargetBranch",
  "UpdaterCurrentDescription",
}

_CARROT_DEFAULT_RESET_EXCLUDED_PREFIXES = (
  "CarParams",
  "Calibration",
  "CompletedTraining",
  "Git",
  "Github",
  "Updater",
  "Dongle",
  "Hardware",
  "DeviceSerial",
)


def _is_carrot_default_reset_param(name: str, meta: Any) -> bool:
  if not name or not isinstance(meta, dict) or "default" not in meta:
    return False
  if name in _CARROT_DEFAULT_RESET_EXCLUDED_NAMES:
    return False
  return not any(name.startswith(prefix) for prefix in _CARROT_DEFAULT_RESET_EXCLUDED_PREFIXES)


def _select_live_runtime_services(snapshot: dict[str, Any]) -> dict[str, Any]:
  services = snapshot.get("services")
  if not isinstance(services, dict):
    return {}
  out: dict[str, Any] = {}
  for name in _LIVE_RUNTIME_SERVICE_NAMES:
    value = services.get(name)
    if isinstance(value, dict):
      out[name] = value
  return out


def _is_drive_engaged(request: web.Request) -> bool:
  broker: RealtimeBroker | None = request.app.get("realtime_broker")
  snapshot = broker.last_snapshot if broker is not None and isinstance(broker.last_snapshot, dict) else {}
  services = snapshot.get("services") if isinstance(snapshot, dict) else {}
  if not isinstance(services, dict):
    return False

  for service_name in ("selfdriveState", "controlsState"):
    service = services.get(service_name)
    if isinstance(service, dict) and bool(service.get("enabled")):
      return True
  return False


def _reject_if_engaged(request: web.Request) -> web.Response | None:
  if not _is_drive_engaged(request):
    return None
  return web.json_response({"ok": False, "error": "Disengage first"}, status=409)


def _maintenance_offroad_precondition(sm, params, now: float | None = None) -> bool:
  """Reject onroad/unknown requests before touching another IPC subscription."""
  now = time.monotonic() if now is None else now
  try:
    age = now - sm.logMonoTime['deviceState'] / 1e9
    if (not sm.alive['deviceState'] or not sm.valid['deviceState'] or not 0 <= age < 1.5 or
        sm['deviceState'].started or not params.get_bool('IsOffroad') or params.get_bool('IsOnroad')):
      return False
    for name in ('carState', 'carControl', 'selfdriveState', 'controlsState'):
      stamp = sm.logMonoTime.get(name, 0)
      if stamp != 0 and not now - stamp / 1e9 >= 1.:
        return False
    return True
  except (AttributeError, KeyError, TypeError, ValueError):
    return False


def _maintenance_offroad_safe(sm, params, panda=None, now: float | None = None,
                              boot_now: float | None = None) -> bool:
  """Offroad maintenance only; never a setting/installation permission."""
  if not _maintenance_offroad_precondition(sm, params, now):
    return False
  try:
    # pandad's C++ Event timestamps use CLOCK_BOOTTIME, unlike hardwared's
    # Python deviceState timestamps. Never compare the two clock domains.
    boot_now = time.clock_gettime(time.CLOCK_BOOTTIME) if boot_now is None else boot_now
    if (panda is None or not panda['valid'] or not panda['alive'] or
        not 0 <= boot_now - panda['stamp_ns'] / 1e9 < .5 or not panda['states']):
      return False
    for state in panda['states']:
      if (state['panda_type'] == 'unknown' or state['ignition_line'] or
          state['ignition_can'] or state['controls_allowed']):
        return False
    return True
  except (AttributeError, KeyError, TypeError, ValueError):
    return False


_maintenance_panda_reader = None
_maintenance_panda_lock = threading.Lock()


def _read_maintenance_panda():
  """One lazy, locked reader, polled only on offroad maintenance requests.

  msgq does not reclaim a subscriber slot on close, so do not open a new
  subscriber for each request. No polling thread or HUD contract is added.
  """
  global _maintenance_panda_reader
  with _maintenance_panda_lock:
    if _maintenance_panda_reader is None:
      from cereal import messaging
      _maintenance_panda_reader = messaging.SubMaster(['pandaStates'])
    sm = _maintenance_panda_reader
    deadline = time.monotonic() + 1.5
    while time.monotonic() < deadline:
      sm.update(100)
      if sm.updated['pandaStates']:
        return {'valid': bool(sm.valid['pandaStates']), 'alive': bool(sm.alive['pandaStates']),
                'stamp_ns': int(sm.logMonoTime['pandaStates']),
                'states': [{'panda_type': str(p.pandaType), 'ignition_line': bool(p.ignitionLine),
                            'ignition_can': bool(p.ignitionCan), 'controls_allowed': bool(p.controlsAllowed)}
                           for p in sm['pandaStates']]}
    return None


async def _require_maintenance_safe(request: web.Request) -> None:
  try:
    await require_parked(request)
  except web.HTTPConflict:
    broker = request.app.get('realtime_broker')
    lock = request.app.get('realtime_broker_poll_lock')
    if broker is not None and lock is not None:
      try:
        async with lock:
          await asyncio.to_thread(broker.poll, 0)
          offroad = _maintenance_offroad_precondition(broker.sm, broker.params)
        if offroad:
          panda = await asyncio.to_thread(_read_maintenance_panda)
          async with lock:
            await asyncio.to_thread(broker.poll, 0)
            if _maintenance_offroad_safe(broker.sm, broker.params, panda):
              return
      except Exception:
        pass
    raise


async def api_heartbeat_status(request: web.Request) -> web.Response:
  return web.json_response({"ok": True, "hb": request.app.get("hb_last")})


async def api_live_runtime(request: web.Request) -> web.Response:
  broker: RealtimeBroker | None = request.app.get("realtime_broker")
  broker_error = request.app.get("realtime_broker_error")
  if broker is None:
    return web.json_response({"ok": False, "error": broker_error or "realtime broker unavailable"}, status=503)

  force = request.query.get("force") == "1"
  runtime = broker.last_snapshot.get("runtime") if isinstance(broker.last_snapshot, dict) else None
  if not isinstance(runtime, dict):
    runtime = {}

  age_ms = broker.snapshot_age_ms()
  if force or age_ms is None or age_ms > 250 or not runtime.get("params"):
    # Serialize poll across concurrent requests: broker.poll() runs
    # SubMaster.update(), and msgq's SubMaster is NOT thread-safe — parallel
    # polls (multiple tabs/devices hitting /api/live_runtime) can corrupt or
    # crash it and take the whole server down. Re-check freshness inside the
    # lock so a request that waited skips a now-redundant poll.
    lock = request.app.get("realtime_broker_poll_lock")
    try:
      if lock is not None:
        async with lock:
          fresh_age = broker.snapshot_age_ms()
          fresh_runtime = broker.last_snapshot.get("runtime") if isinstance(broker.last_snapshot, dict) else {}
          if force or fresh_age is None or fresh_age > 250 or not (fresh_runtime or {}).get("params"):
            await asyncio.to_thread(broker.poll, 0)
      else:
        await asyncio.to_thread(broker.poll, 0)
    except Exception as exc:
      return web.json_response({"ok": False, "error": str(exc)}, status=500)
    runtime = broker.last_snapshot.get("runtime") if isinstance(broker.last_snapshot, dict) else {}

  meta = broker.last_snapshot.get("meta") if isinstance(broker.last_snapshot, dict) else {}
  services = _select_live_runtime_services(broker.last_snapshot if isinstance(broker.last_snapshot, dict) else {})
  return web.json_response(to_transport_safe({
    "ok": True,
    "meta": meta if isinstance(meta, dict) else {},
    "runtime": runtime if isinstance(runtime, dict) else {},
    "services": services,
    "snapshotAgeMs": broker.snapshot_age_ms(),
  }))


async def api_reboot(request: web.Request) -> web.Response:
  await _require_maintenance_safe(request)
  try:
    if HAS_PARAMS:
      Params().put_bool("DoReboot", True)
    else:
      # Params-less development fallback only.
      subprocess.Popen(["sudo", "reboot"])
    return web.json_response({"ok": True})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_time_sync(request: web.Request) -> web.Response:
  try:
    body = await request.json()
  except Exception as e:
    return web.json_response({"ok": False, "error": f"bad json: {e}"}, status=400)

  epoch_ms = body.get("epoch_ms")
  timezone_name = (body.get("timezone") or "").strip()
  debug = bool(body.get("debug", False))
  client_iso = body.get("client_iso")

  if not isinstance(epoch_ms, (int, float)):
    return web.json_response({"ok": False, "error": "epoch_ms required"}, status=400)

  if not timezone_name:
    timezone_name = "UTC"

  effective_debug = debug or TIME_SYNC_DEBUG_DEFAULT
  if effective_debug:
    print(f"[time_sync] client={request.remote} timezone={timezone_name} client_iso={client_iso} debug={debug}")

  result = await asyncio.to_thread(
    sync_system_time_from_browser,
    int(epoch_ms),
    timezone_name,
    effective_debug,
  )

  if effective_debug:
    print(
      f"[time_sync] result ok={result.get('ok')} "
      f"applied={result.get('applied')} "
      f"diff_sec={result.get('diff_sec')} "
      f"message={result.get('message')}"
    )

  status = 200 if result.get("ok") else 500
  return web.json_response(result, status=status)


async def api_device_network(request: web.Request) -> web.Response:
  try:
    force = request.query.get("force") == "1"
    network = await asyncio.to_thread(get_device_network, force)
    return web.json_response({"ok": True, "network": network})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_calibration_status(request: web.Request) -> web.Response:
  try:
    calibration = await asyncio.to_thread(get_calibration_status)
    return web.json_response({"ok": True, "calibration": calibration})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_regulatory(request: web.Request) -> web.Response:
  try:
    path = os.path.join(OFFROAD_ASSETS_DIR, "fcc.html")
    if not os.path.isfile(path):
      return web.json_response({"ok": False, "error": "regulatory info unavailable"}, status=404)

    def read_file() -> str:
      with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()

    html = await asyncio.to_thread(read_file)
    return web.json_response({"ok": True, "html": html})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_poweroff(request: web.Request) -> web.Response:
  await _require_maintenance_safe(request)
  if not HAS_PARAMS:
    return web.json_response({"ok": False, "error": "params unavailable"}, status=500)
  try:
    Params().put_bool("DoShutdown", True)
    return web.json_response({"ok": True})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_recalibrate(request: web.Request) -> web.Response:
  await _require_maintenance_safe(request)
  if not HAS_PARAMS:
    return web.json_response({"ok": False, "error": "params unavailable"}, status=500)
  try:
    params = Params()
    for key in ("CalibrationParams", "LiveTorqueParameters",
                "LiveParameters", "LiveParametersV2", "LiveDelay"):
      try:
        params.remove(key)
      except Exception:
        pass
    # Request onroad cycle restart if available
    try:
      params.put_bool("OnroadCycleRequested", True)
    except Exception:
      pass
    try:
      params.put_bool("DoReboot", True)
    except Exception:
      pass
    return web.json_response({"ok": True})
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


async def api_set_default(request: web.Request) -> web.Response:
  if not HAS_PARAMS:
    return web.json_response({"ok": False, "error": "params unavailable"}, status=500)
  try:
    _, _, by_name, _ = get_settings_cached()
    values = {
      name: meta.get("default", 0)
      for name, meta in by_name.items()
      if _is_carrot_default_reset_param(name, meta)
    }
    virtual = {name:values.pop(name) for name in list(values) if name in DRIVING_PREFERENCES}
    button_defaults = {name:values.pop(name) for name in list(values) if name in CRUISE_BUTTON_PREFERENCES}
    border_default = values.pop(BORDER_KEY, None)
    # These optional control preferences have the same parked gate on reset
    # as on individual changes. Reject before ANY native settings are reset.
    if virtual or button_defaults:
      await require_parked(request)
    if virtual:
      virtual = driving_preferences.reset_defaults()
    if button_defaults:
      virtual.update(cruise_button_preferences.reset_defaults())
    if border_default is not None:
      notification_preferences.set(border_default)
      virtual[BORDER_KEY] = border_default
    restored = restore_param_values_validated(values)
    result = restored.get("result") or {}
    ok = int(result.get("fail_cnt") or 0) == 0
    applied_values = {
      entry["key"]: entry["value"]
      for entry in restored.get("preview", {}).get("entries", [])
      if entry.get("apply")
    }
    applied_values.update(virtual)
    status = 200 if ok else 500
    return web.json_response({
      "ok": ok,
      "message": "설정 초기화 성공" if ok else "설정 초기화 실패",
      "error": None if ok else "설정 초기화 실패",
      "values": applied_values,
      **restored,
    }, status=status)
  except web.HTTPException:
    raise
  except Exception as e:
    return web.json_response({"ok": False, "error": str(e)}, status=500)


def register(app: web.Application) -> None:
  app.router.add_get("/api/heartbeat_status", api_heartbeat_status)
  app.router.add_get("/api/live_runtime", api_live_runtime)
  app.router.add_get("/api/device_network", api_device_network)
  app.router.add_get("/api/calibration_status", api_calibration_status)
  app.router.add_get("/api/regulatory", api_regulatory)
  app.router.add_post("/api/reboot", api_reboot)
  app.router.add_post("/api/poweroff", api_poweroff)
  app.router.add_post("/api/recalibrate", api_recalibrate)
  app.router.add_post("/api/set_default", api_set_default)
  app.router.add_post("/api/time_sync", api_time_sync)
