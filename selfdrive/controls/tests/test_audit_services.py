"""Regression checks for maintenance gates, audio boundaries and diagnostic settings.

Runs with Python 3.12 and numpy, without native Params, msgq, audio or vehicle I/O.
Only production AST functions are executed; preference writes use temporary files.
"""
import ast
import asyncio
import __future__
from enum import IntEnum
import importlib
import json
import math
import os
from pathlib import Path
import tempfile
import threading
import unittest
from types import SimpleNamespace as NS
import uuid

import numpy as np

ROOT = Path(__file__).resolve().parents[3]


def load_nodes(relative, names, scope=None):
  namespace = dict(scope or {})
  tree = ast.parse((ROOT / relative).read_text(encoding="utf-8"))
  nodes = []
  for node in tree.body:
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
      selected = node.name in names
    elif isinstance(node, ast.Assign):
      selected = any(isinstance(t, ast.Name) and t.id in names for t in node.targets)
    elif isinstance(node, (ast.AugAssign, ast.AnnAssign)):
      selected = isinstance(node.target, ast.Name) and node.target.id in names
    else:
      selected = False
    if selected:
      nodes.append(node)
  exec(compile(ast.Module(body=nodes, type_ignores=[]), str(ROOT / relative), "exec",
               flags=__future__.annotations.compiler_flag, dont_inherit=True), namespace)
  return namespace


class Conflict(Exception):
  def __init__(self, *, text, content_type):
    super().__init__(text)
    self.text, self.content_type, self.status = text, content_type, 409


class ServiceState(dict):
  def __init__(self):
    super().__init__(
      carState=NS(canValid=True, vEgo=0., gearShifter="park"),
      carControl=NS(enabled=False, latActive=False, longActive=False),
      selfdriveState=NS(enabled=False, active=False))
    self.alive = {name: True for name in self}
    self.valid = self.alive.copy()
    self.logMonoTime = {name: 100_000_000_000 for name in self}


class TestAuditServicesMaintenance(unittest.TestCase):
  def setUp(self):
    self.effects = []
    web = NS(HTTPConflict=Conflict, json_response=lambda body, status=200: NS(status=status, body=body))
    safety = load_nodes("selfdrive/carrot/server/services/setting_safety.py",
                        {"parked_state_error", "require_parked"},
                        {"asyncio": asyncio, "json": json, "math": math,
                         "time": NS(monotonic=lambda: 100.), "web": web})
    effects = self.effects

    class ParamsSpy:
      def __init__(self):
        effects.append(("Params",))

      def put_bool(self, key, value):
        effects.append(("put_bool", key, value))

      def remove(self, key):
        effects.append(("remove", key))

    self.scope = load_nodes("selfdrive/carrot/server/features/system.py",
                           {"api_reboot", "api_poweroff", "api_recalibrate"},
                           {"web": web, "require_parked": safety["require_parked"],
                            "HAS_PARAMS": True, "Params": ParamsSpy,
                            "subprocess": NS(Popen=lambda args: effects.append(("Popen", args)))})

  async def invoke(self, name, condition="parked"):
    sm = ServiceState()
    if condition == "lat_only":
      sm["carControl"].latActive = True
    elif condition == "long_only":
      sm["carControl"].longActive = True
    elif condition == "drive_gear":
      sm["carState"].gearShifter = "drive"
    elif condition == "moving":
      sm["carState"].vEgo = .03
    elif condition == "nan_speed":
      sm["carState"].vEgo = float("nan")
    elif condition == "can_invalid":
      sm["carState"].canValid = False
    elif condition == "stale":
      sm.logMonoTime["carControl"] = 99_400_000_000
    elif condition == "future":
      sm.logMonoTime["carState"] = 100_100_000_000
    elif condition == "invalid":
      sm.valid["selfdriveState"] = False
    elif condition == "missing_service":
      del sm["carControl"]

    polls = []

    def poll(timeout):
      polls.append(timeout)
      if condition == "poll_error":
        raise RuntimeError("simulated unavailable broker")
      if condition == "fresh_poll_lat_only":
        sm["carControl"].latActive = True

    broker = NS(sm=sm, poll=poll, last_snapshot={"services": {"selfdriveState": {"enabled": False}}})
    app = {"realtime_broker": broker, "realtime_broker_poll_lock": asyncio.Lock()}
    if condition == "no_broker":
      app.pop("realtime_broker")
    elif condition == "no_lock":
      app.pop("realtime_broker_poll_lock")
    response = await self.scope[name](NS(app=app))
    return response, polls

  def test_all_maintenance_rejects_unsafe_or_unknown_without_side_effects(self):
    conditions = ("lat_only", "long_only", "drive_gear", "moving", "nan_speed", "can_invalid",
                  "stale", "future", "invalid", "missing_service", "no_broker", "no_lock",
                  "poll_error", "fresh_poll_lat_only")
    for name in ("api_reboot", "api_poweroff", "api_recalibrate"):
      for has_params in (True, False):
        self.scope["HAS_PARAMS"] = has_params
        for condition in conditions:
          with self.subTest(handler=name, has_params=has_params, condition=condition):
            self.effects.clear()
            with self.assertRaises(Conflict) as raised:
              asyncio.run(self.invoke(name, condition))
            self.assertEqual(raised.exception.status, 409)
            self.assertEqual(json.loads(raised.exception.text)["error_code"], "PARKED_REQUIRED")
            self.assertEqual(self.effects, [])

  def test_fresh_parked_handlers_keep_existing_requested_operations(self):
    expected = {
      "api_reboot": [("Params",), ("put_bool", "DoReboot", True)],
      "api_poweroff": [("Params",), ("put_bool", "DoShutdown", True)],
      "api_recalibrate": [("Params",)] + [
        ("remove", key) for key in ("CalibrationParams", "LiveTorqueParameters", "LiveParameters",
                                   "LiveParametersV2", "LiveDelay")
      ] + [("put_bool", "OnroadCycleRequested", True), ("put_bool", "DoReboot", True)],
    }
    for name, operations in expected.items():
      with self.subTest(handler=name):
        self.effects.clear()
        response, polls = asyncio.run(self.invoke(name))
        self.assertEqual(response.status, 200)
        self.assertEqual(polls, [0])
        self.assertEqual(self.effects, operations)


class Alert(IntEnum):
  none = 0
  engage = 1
  warningImmediate = 2


def sound_instance(loops=1, start=0, volume=1.):
  tree = ast.parse((ROOT / "selfdrive/ui/soundd.py").read_text(encoding="utf-8"))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Soundd")
  nodes = [n for n in cls.body if isinstance(n, ast.FunctionDef) and
           n.name in ("get_sound_data", "update_alert")]
  scope = {"np": np, "AudibleAlert": Alert, "OPTIONAL_ALERTS": {Alert.engage},
           "sound_list": {Alert.engage: ("unused", loops, 1.), Alert.warningImmediate: ("unused", loops, 1.)}}
  exec(compile(ast.Module(body=nodes, type_ignores=[]), "<production Soundd boundaries>", "exec"), scope)
  actual = type("ActualSoundMethods", (), {name: scope[name] for name in ("get_sound_data", "update_alert")})()
  actual.current_alert, actual.current_sound_frame = Alert.engage, start
  actual.current_volume, actual.routine_mute = volume, False
  actual.loaded_sounds = {alert: np.array([10., 20., 30., 40.], dtype=np.float32)
                         for alert in (Alert.engage, Alert.warningImmediate)}
  return actual


class TestAuditServicesSound(unittest.TestCase):
  def test_exact_samples_at_partial_finite_and_infinite_loop_boundaries(self):
    cases = (
      (1, 0, 6, [10, 20, 30, 40, 0, 0], 4),
      (1, 3, 4, [40, 0, 0, 0], 4),
      (1, 4, 3, [0, 0, 0], 4),
      (2, 3, 7, [40, 10, 20, 30, 40, 0, 0], 8),
      (None, 3, 7, [40, 10, 20, 30, 40, 10, 20], 10),
      (1, 0, 0, [], 0),
    )
    for loops, start, count, expected, final_frame in cases:
      with self.subTest(loops=loops, start=start, frames=count):
        sound = sound_instance(loops, start)
        np.testing.assert_array_equal(sound.get_sound_data(count), expected)
        self.assertEqual(sound.current_sound_frame, final_frame)

  def test_callback_partition_does_not_change_the_waveform(self):
    for loops in (1, 2, None):
      with self.subTest(loops=loops):
        whole, split = sound_instance(loops), sound_instance(loops)
        expected = whole.get_sound_data(11)
        actual = np.concatenate([split.get_sound_data(n) for n in (1, 3, 2, 5)])
        np.testing.assert_array_equal(actual, expected)
        self.assertEqual(split.current_sound_frame, whole.current_sound_frame)

  def test_exact_first_completion_clears_and_same_alert_can_replay(self):
    sound = sound_instance()
    sound.get_sound_data(3)
    sound.update_alert(Alert.none)
    self.assertEqual(sound.current_alert, Alert.engage)
    np.testing.assert_array_equal(sound.get_sound_data(1), [40])
    sound.update_alert(Alert.none)
    self.assertEqual(sound.current_alert, Alert.none)
    sound.update_alert(Alert.engage)
    self.assertEqual(sound.current_sound_frame, 0)
    np.testing.assert_array_equal(sound.get_sound_data(4), [10, 20, 30, 40])

  def test_volume_and_protected_first_play_are_preserved(self):
    sound = sound_instance(volume=.25)
    sound.current_alert, sound.routine_mute = Alert.warningImmediate, True
    sound.get_sound_data(2)
    sound.update_alert(Alert.engage)
    self.assertEqual(sound.current_alert, Alert.warningImmediate)
    np.testing.assert_array_equal(sound.get_sound_data(4), [7.5, 10, 0, 0])
    sound.update_alert(Alert.engage)
    self.assertEqual(sound.current_alert, Alert.none)


def diagnostic_scope():
  return load_nodes("selfdrive/carrot/can_diagnostics/automatic_runtime.py",
                    {"PARAMS", "settings_snapshot", "read_param", "cruise_button_snapshot",
                     "optional_cruise_button_preferences"},
                    {"importlib": importlib})


def preference_class():
  namespace = load_nodes("selfdrive/controls/lib/approach_comfort.py",
                         {"DRIVING_PREFERENCES", "_PREFERENCE_LOCK", "DrivingPreferences",
                          "CRUISE_BUTTON_PREFERENCES", "CruiseButtonPreferences"},
                         {"Path": Path, "threading": threading, "json": json, "uuid": uuid, "os": os})
  return namespace["CruiseButtonPreferences"]


class ReadOnlyParams:
  def all_keys(self):
    return [b"MyDrivingMode", b"HardwareC3xLite"]

  def get(self, name):
    return {"MyDrivingMode": b"3", "HardwareC3xLite": b"0"}[name]


class TestAuditServicesDiagnostics(unittest.TestCase):
  def setUp(self):
    self.scope = diagnostic_scope()
    self.temporary = tempfile.TemporaryDirectory(prefix="audit-services-")
    self.addCleanup(self.temporary.cleanup)
    self.path = Path(self.temporary.name) / "cruise_button_preferences.json"
    self.preferences = preference_class()(self.path)

  def snapshot(self, preferences):
    return self.scope["settings_snapshot"](
      ReadOnlyParams(), NS(read=lambda: {"LaunchFollowStrength": 10}), preferences)

  def test_missing_file_is_effective_default_without_creating_a_file(self):
    row = self.snapshot(self.preferences)
    self.assertEqual(row["cruise_button_preferences"],
                     {"status": "missing_file_defaulted", "values": {"CruiseOffKeepLateral": 0}, "error": None})
    self.assertFalse(self.path.exists())
    self.assertEqual(row["driving_preferences"], {"LaunchFollowStrength": 10})
    self.assertEqual(row["MyDrivingMode"], "3")
    self.assertNotIn("CruiseOffKeepLateral", self.scope["PARAMS"])

  def test_on_and_off_are_observed_without_rewriting_user_file(self):
    for value in (1, 0):
      with self.subTest(value=value):
        self.preferences.set("CruiseOffKeepLateral", value)
        before = self.path.read_bytes(), self.path.stat().st_mtime_ns
        row = self.snapshot(self.preferences)["cruise_button_preferences"]
        self.assertEqual(row, {"status": "observed", "values": {"CruiseOffKeepLateral": value}, "error": None})
        self.assertEqual((self.path.read_bytes(), self.path.stat().st_mtime_ns), before)

  def test_invalid_file_reports_fallback_and_preserves_original(self):
    for original in ('{broken', '{"schema": 1, "values": {"CruiseOffKeepLateral": 2}}'):
      with self.subTest(original=original):
        self.path.write_text(original, encoding="utf-8")
        row = self.snapshot(self.preferences)["cruise_button_preferences"]
        self.assertEqual(row["status"], "invalid_preferences_defaulted")
        self.assertEqual(row["values"], {"CruiseOffKeepLateral": 0})
        self.assertEqual(row["error"], "invalid_preferences_features_off")
        self.assertEqual(self.path.read_text(encoding="utf-8"), original)

  def test_unavailable_and_reader_failure_are_not_reported_as_observed_off(self):
    missing = self.snapshot(None)["cruise_button_preferences"]
    self.assertEqual(missing, {"status": "not_available_on_this_build", "values": None})

    def fail():
      raise OSError("simulated disk error")

    failure = self.snapshot(NS(read=fail))["cruise_button_preferences"]
    self.assertEqual(failure, {"status": "read_failed", "values": None, "error": "OSError"})

  def test_old_build_optional_class_and_unrelated_import_failure(self):
    module_name = "openpilot.selfdrive.controls.lib.approach_comfort"
    factory = self.scope["optional_cruise_button_preferences"]
    self.scope["importlib"] = NS(import_module=lambda name: NS())
    self.assertIsNone(factory())
    self.scope["importlib"] = NS(import_module=lambda name: NS(CruiseButtonPreferences=lambda: self.preferences))
    self.assertIs(factory(), self.preferences)

    def missing(name):
      raise ModuleNotFoundError("optional module missing", name=module_name)

    self.scope["importlib"] = NS(import_module=missing)
    self.assertIsNone(factory())

    def dependency_error(name):
      raise ModuleNotFoundError("unexpected dependency failure", name="unexpected_dependency")

    self.scope["importlib"] = NS(import_module=dependency_error)
    with self.assertRaises(ModuleNotFoundError):
      factory()

  def test_source_hash_inventory_includes_both_button_authority_sources(self):
    tree = ast.parse((ROOT / "selfdrive/carrot/can_diagnostics/automatic_runtime.py").read_text(encoding="utf-8"))
    assignment = next(n for n in ast.walk(tree) if isinstance(n, ast.Assign) and
                      any(isinstance(t, ast.Name) and t.id == "sources" for t in n.targets))
    sources = set(ast.literal_eval(assignment.value))
    self.assertTrue({"selfdrive/car/cruise.py", "selfdrive/car/lx3_authority.py"} <= sources)


if __name__ == "__main__":
  unittest.main(verbosity=2)
