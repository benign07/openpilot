"""Exercise the actual HTTP handler with fake Params; never send vehicle commands."""
import ast
import json
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

ROOT = Path(__file__).resolve().parents[3]


class TestLiveDrivingMode(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    source = ast.parse((ROOT / 'selfdrive/carrot/server/features/params.py').read_text(encoding='utf-8'))
    handler = next(node for node in source.body if isinstance(node, ast.AsyncFunctionDef) and node.name == 'api_param_set')
    self.parked_calls = 0
    self.writes = []
    self.values = {'MyDrivingMode': 3}
    async def parked(request):
      self.parked_calls += 1
      raise web.HTTPConflict(text='moving or active')
    def write(name, value, definition):
      self.writes.append((name, value))
      self.values[name] = value
    env = {'web': web, 'DISPLAY_SETTINGS': {'ShowLaneInfo'}, 'require_parked': parked, 'HAS_PARAMS': True,
           'get_settings_cached': lambda: ({}, {}, {}, []), 'set_param_value': write,
           'get_param_values': lambda names, defaults: {name: self.values.get(name, defaults[name]) for name in names}}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[handler], type_ignores=[])), 'production-handler', 'exec'), env)
    app = web.Application()
    app.router.add_post('/api/param_set', env['api_param_set'])
    self.client = TestClient(TestServer(app))
    await self.client.start_server()

  async def asyncTearDown(self):
    await self.client.close()

  async def test_each_existing_mode_can_change_while_controls_are_active(self):
    for value in (1, 2, 3, 4, '1', '4'):
      response = await self.client.post('/api/param_set', json={'name': 'MyDrivingMode', 'value': value})
      self.assertEqual(response.status, 200)
      self.assertEqual((await response.json())['value'], int(value))
    self.assertEqual(self.parked_calls, 0)

  async def test_invalid_modes_never_write(self):
    for value in (0, 5, -1, True, False, 2.5, 2.0, None, {}, ' 2', '2.0', 'mode'):
      response = await self.client.post('/api/param_set', json={'name': 'MyDrivingMode', 'value': value})
      self.assertEqual(response.status, 400, value)
    self.assertEqual(self.writes, [])

  async def test_calibration_auto_mode_and_similar_names_still_require_parked(self):
    for name in ('SteerRatioRate', 'TFollowGap1', 'MyDrivingModeAuto', 'MyDrivingMode ', 'DisableDM'):
      response = await self.client.post('/api/param_set', json={'name': name, 'value': 1})
      self.assertEqual(response.status, 409)
    self.assertEqual(self.writes, [])
    self.assertEqual(self.parked_calls, 5)

  async def test_display_settings_remain_available(self):
    response = await self.client.post('/api/param_set', json={'name': 'ShowLaneInfo', 'value': 1})
    self.assertEqual(response.status, 200)
    self.assertEqual(self.parked_calls, 0)


if __name__ == '__main__': unittest.main()
