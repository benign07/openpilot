"""Carrot HUD's freshness and mode fields must remain available in Carrot Web."""

import ast
import asyncio
from pathlib import Path
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from . import broker
from .snapshot import build_live_payload


def run_composition_startup():
  # Execute the actual startup function, replacing only unrelated network,
  # camera and background jobs. The real broker still selects subscriptions.
  source = Path(__file__).resolve().parents[1] / 'app.py'
  startup = next(node for node in ast.parse(source.read_text(encoding='utf8')).body
                 if isinstance(node, ast.AsyncFunctionDef) and node.name == 'on_startup')
  namespace = {
    'web': SimpleNamespace(Application=dict), 'ClientSession': object,
    'HAS_PARAMS': False, 'Params': None, 'messaging': broker.messaging,
    'RealtimeBroker': broker.RealtimeBroker, 'create_carrot_hud_broker': broker.create_carrot_hud_broker,
    'CameraWsHub': lambda messaging: None, 'RawWsHub': lambda messaging: None,
    'asyncio': SimpleNamespace(Lock=object, create_task=lambda task: None), 'WEB_DIR': '',
    'start_popular_value_upload': lambda app: None, 'start_precompress': lambda path: None,
  }
  namespace.update({name: lambda *args: None for name in ('heartbeat_loop', 'git_status_loop', 'auto_update_loop',
                                                    '_malloc_trim_loop', '_warm_settings_cache')})
  exec(compile(ast.Module(body=[startup], type_ignores=[]), str(source), 'exec'), namespace)
  app = {}
  asyncio.run(namespace['on_startup'](app))
  if app.get('realtime_broker_error'):
    raise AssertionError(app['realtime_broker_error'])
  return app['realtime_broker']


class HudContractTest(unittest.TestCase):
  def test_startup_broker_subscribes_and_polls_android_hud_state(self):
    now_ns = time.monotonic_ns()

    class SubscriptionSM:
      def __init__(self, names):
        self.names = names
        self.data = dict.fromkeys(names, object())
        self.alive = dict.fromkeys(names, True)
        self.valid = dict.fromkeys(names, True)
        self.logMonoTime = dict.fromkeys(names, now_ns)
        self.values = {
          'carState': SimpleNamespace(vEgo=12.0, canValid=True, canTimeout=False),
          'longitudinalPlan': SimpleNamespace(myDrivingMode=3),
          'carrotMan': SimpleNamespace(xSpdLimit=60),
        }

      def __getitem__(self, name):
        if name not in self.data:
          raise KeyError(name)
        return self.values.get(name, SimpleNamespace())

      def update(self, timeout):
        pass

    with patch.object(broker, 'messaging', SimpleNamespace(SubMaster=SubscriptionSM)), patch.object(broker, 'Params', None):
      runtime = run_composition_startup()
      self.assertTrue({'selfdriveState', 'carState', 'longitudinalPlan', 'carrotMan'}.issubset(runtime.service_names))
      self.assertTrue({'modelV2', 'liveCalibration', 'roadCameraState', 'deviceState'}.isdisjoint(runtime.service_names))
      payload = runtime.poll()
    self.assertTrue(payload['runtime']['serviceValid']['carState'])
    self.assertTrue(payload['services']['carState']['canValid'])
    self.assertEqual(payload['services']['longitudinalPlan']['myDrivingMode'], 3)
    self.assertEqual(payload['services']['carrotMan']['xSpdLimit'], 60)

  def test_fresh_vehicle_and_mode_are_separate_from_stale_vehicle(self):
    now_ns = time.monotonic_ns()
    class FakeSM:
      data = {'carState': object(), 'longitudinalPlan': object()}
      alive = {'carState': True, 'longitudinalPlan': True}
      valid = {'carState': True, 'longitudinalPlan': True}
      logMonoTime = {'carState': now_ns, 'longitudinalPlan': now_ns}
      values = {
        'carState': SimpleNamespace(vEgo=12.0, canValid=True, canTimeout=False,
                                    cruiseState=SimpleNamespace(enabled=True, speed=15.0)),
        'longitudinalPlan': SimpleNamespace(myDrivingMode=3),
      }

      def __getitem__(self, name):
        return self.values[name]

    sm = FakeSM()
    payload = build_live_payload(sm=sm, repo_flavor='carrot')
    self.assertTrue(payload['runtime']['serviceValid']['carState'])
    self.assertLess(payload['runtime']['serviceAgeMs']['carState'], 1000)
    self.assertTrue(payload['services']['carState']['canValid'])
    self.assertFalse(payload['services']['carState']['canTimeout'])
    self.assertEqual(payload['services']['longitudinalPlan']['myDrivingMode'], 3)

    sm.valid['carState'] = False
    sm.logMonoTime['carState'] = now_ns - 3_000_000_000
    payload = build_live_payload(sm=sm, repo_flavor='carrot', previous_payload=payload)
    self.assertFalse(payload['runtime']['serviceValid']['carState'])
    self.assertGreater(payload['runtime']['serviceAgeMs']['carState'], 1000)


if __name__ == '__main__':
  unittest.main()
