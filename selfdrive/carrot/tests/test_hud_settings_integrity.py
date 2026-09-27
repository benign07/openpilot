"""Offline regressions: fake Params and telemetry, never write vehicle settings."""
import asyncio
import enum
import json
import math
import unittest
from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import patch

from aiohttp import web
from selfdrive.carrot.server.services import params as service, settings, setting_profiles
from selfdrive.carrot.server.services.setting_safety import parked_state_error, require_parked

ROOT = Path(__file__).resolve().parents[3]
DEFS = {p['name']: p for p in json.loads((ROOT / 'selfdrive/carrot_settings.json').read_text(encoding='utf-8'))['params']}

class Kind(enum.Enum):
  BOOL=1; INT=2; FLOAT=3; STRING=4; TIME=5; JSON=6; BYTES=7

class FakeParams:
  def __init__(self):
    self.values = {'SteerRatioRate': b'100', 'DynamicTFollowLC': b'100'}
    self.writes = []
    self.fail_key = None
  def get_type(self, key):
    if key not in DEFS: raise KeyError(key)
    return Kind.INT
  def get(self, key, **kwargs): return self.values.get(key)
  def put(self, key, value): self.values[key] = value
  def remove(self, key): self.values.pop(key, None)
  def put_int(self, key, value):
    self.writes.append(key)
    self.values[key] = str(value).encode()
    if key == self.fail_key: raise OSError('simulated disk error after write')

class TestSettingsIntegrity(unittest.TestCase):
  def setUp(self):
    self.fake = FakeParams()
    self.patches = [patch.object(service, 'HAS_PARAMS', True), patch.object(service, 'Params', return_value=self.fake),
      patch.object(service, 'ParamKeyType', Kind), patch.object(settings, 'get_settings_cached', return_value=({}, {}, DEFS, [])),
      patch.object(service, 'get_param_values', side_effect=lambda keys, defaults: {k:int(self.fake.values.get(k,b'0')) for k in keys})]
    for p in self.patches: p.start(); self.addCleanup(p.stop)

  def test_every_supported_default_is_accepted(self):
    for name, meta in DEFS.items():
      with self.subTest(name=name):
        self.assertLessEqual(meta['min'], meta['default']); self.assertLessEqual(meta['default'], meta['max'])
        if meta.get('supported') is not False: service.validate_setting_value(name,meta['default'])

  def test_dangerous_zero_and_nonfinite_values_rejected(self):
    for value in (0,-1,math.nan,math.inf,'nope',99.5):
      with self.subTest(value=value), self.assertRaises(ValueError):
        service.set_param_value('SteerRatioRate', value)
    self.assertFalse(self.fake.writes)

  def test_unsupported_setting_cannot_report_success(self):
    for name in ('ShowTpms','MapboxStyle','HotspotOnBoot'):
      with self.assertRaises(ValueError): service.set_param_value(name,0)

  def test_bulk_validates_every_value_before_first_write(self):
    with self.assertRaises(ValueError):
      service.restore_param_values_from_backup({'SteerRatioRate':110,'DynamicTFollowLC':0})
    self.assertFalse(self.fake.writes)

  def test_bulk_disk_failure_rolls_back_even_partially_written_key(self):
    before = self.fake.values.copy(); self.fake.fail_key = 'DynamicTFollowLC'
    with self.assertRaises(RuntimeError):
      service.restore_param_values_from_backup({'SteerRatioRate':110,'DynamicTFollowLC':80})
    self.assertEqual(self.fake.values, before)

  def test_bulk_rollback_removes_originally_absent_value(self):
    self.fake.values.pop('DynamicTFollowLC'); before=self.fake.values.copy(); self.fake.fail_key='DynamicTFollowLC'
    with self.assertRaises(RuntimeError):
      service.restore_param_values_from_backup({'SteerRatioRate':110,'DynamicTFollowLC':80})
    self.assertEqual(self.fake.values,before)

  def test_empty_selection_never_restores_all(self):
    out=service.restore_param_values_validated({'SteerRatioRate':110},[])
    self.assertEqual(out['result']['ok_cnt'],0); self.assertFalse(self.fake.writes)

  def test_invalid_selected_key_does_not_partially_apply(self):
    with self.assertRaises(ValueError): service.restore_param_values_validated({'SteerRatioRate':110,'DynamicTFollowLC':0})
    self.assertFalse(self.fake.writes)

  def test_unselected_invalid_key_does_not_block_selected_valid_key(self):
    out=service.restore_param_values_validated({'SteerRatioRate':110,'DynamicTFollowLC':0},['SteerRatioRate'])
    self.assertEqual(out['result']['ok_cnt'],1); self.assertEqual(self.fake.values['SteerRatioRate'],b'110')

  def test_profile_excludes_vehicle_identity_and_unimplemented_features(self):
    with patch.object(setting_profiles,'get_settings_cached',return_value=({}, {}, DEFS, [])):
      out=setting_profiles._clean_values({'CanfdHDA2':0,'DisableDM':0,'ShowTpms':1,'SteerRatioRate':100,'TFollowGap1':100})
      self.assertEqual(out,{'TFollowGap1':100})

class Telemetry(dict): pass
def safe_telemetry():
  sm=Telemetry(carState=NS(vEgo=0.,gearShifter='park',canValid=True),carControl=NS(enabled=False,latActive=False,longActive=False),selfdriveState=NS(enabled=False,active=False))
  sm.alive={n:True for n in sm}; sm.valid=sm.alive.copy(); sm.logMonoTime={n:99.9e9 for n in sm}
  return sm

class TestParkedGate(unittest.TestCase):
  def test_fresh_parked_passes(self): self.assertIsNone(parked_state_error(safe_telemetry(),100))
  def test_manual_motion_and_neutral_rejected(self):
    for gear,speed in [('drive',20),('neutral',0),('park',.03),('park',math.nan)]:
      sm=safe_telemetry(); sm['carState'].gearShifter=gear; sm['carState'].vEgo=speed
      self.assertIsNotNone(parked_state_error(sm,100))
  def test_lateral_only_and_longitudinal_control_rejected(self):
    for key in ('enabled','latActive','longActive'):
      sm=safe_telemetry(); setattr(sm['carControl'],key,True)
      self.assertIsNotNone(parked_state_error(sm,100))
  def test_stale_invalid_future_or_absent_telemetry_rejected(self):
    for service_name in safe_telemetry():
      for field,value in [('alive',False),('valid',False),('logMonoTime',99e9),('logMonoTime',101e9)]:
        sm=safe_telemetry(); getattr(sm,field)[service_name]=value
        self.assertIsNotNone(parked_state_error(sm,100))
    self.assertIsNotNone(parked_state_error({},100))
  def test_missing_broker_conflict(self):
    with self.assertRaises(web.HTTPConflict): asyncio.run(require_parked(NS(app={})))

if __name__=='__main__': unittest.main()
