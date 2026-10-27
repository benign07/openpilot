"""Real HUD read/write routing and parked gates for the new separate store."""
import asyncio
import ast
import __future__
import json
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
from openpilot.selfdrive.controls.lib.approach_comfort import ResponsePreferences,DRIVING_PREFERENCES,CRUISE_BUTTON_PREFERENCES,RESPONSE_PREFERENCES
from openpilot.selfdrive.carrot.server.services import params,settings


@pytest.mark.parametrize('native_available',[False,True])
def test_json_option_never_uses_or_adds_a_native_params_key(tmp_path,monkeypatch,native_available):
  store=ResponsePreferences(tmp_path/'response.json')
  monkeypatch.setattr(params,'response_preferences',store)
  monkeypatch.setattr(params,'HAS_PARAMS',native_available)
  def forbidden():raise AssertionError('Virtual option reached native Params')
  monkeypatch.setattr(params,'Params',forbidden,raising=False)
  params.set_param_value('DriverPositionHold',1)
  assert params.get_param_value('DriverPositionHold',0)==1
  assert params.get_param_values(['DriverPositionHold'])=={'DriverPositionHold':1}
  data=settings.inject_driving_preferences({'params':[],'menu':[]})
  option=next(p for p in data['params'] if p['name']=='DriverPositionHold')
  assert option['default']==0 and option['virtual'] and option['group']=='개발 중 기능'
  assert '3초' in option['descr'] and '5초' in option['descr']
  assert 'EarlyLeadStart' not in {p['name'] for p in data['params']}
  assert json.dumps(data['menu']).count('DriverPositionHold')==1


def test_api_rejects_before_any_setting_write_when_not_parked(tmp_path,monkeypatch):
  store=ResponsePreferences(tmp_path/'response.json');effects=[]
  async def body():return {'name':'DriverPositionHold','value':1}
  async def reject(request):effects.append('parked_gate');raise RuntimeError('PARKED_REQUIRED')
  # Execute the actual route without importing every unrelated dashcam,
  # upload and hardware feature through features/__init__.py.
  path=Path(__file__).resolve().parents[3]/'selfdrive/carrot/server/features/params.py'
  node=next(n for n in ast.parse(path.read_text(encoding='utf8')).body if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_param_set')
  scope=dict(require_parked=reject,set_param_value=lambda *a:effects.append('write'),
    DRIVING_PREFERENCES=DRIVING_PREFERENCES,CRUISE_BUTTON_PREFERENCES=CRUISE_BUTTON_PREFERENCES,RESPONSE_PREFERENCES=RESPONSE_PREFERENCES)
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec',flags=__future__.annotations.compiler_flag),scope)
  with pytest.raises(RuntimeError,match='PARKED_REQUIRED'):
    asyncio.run(scope['api_param_set'](NS(json=body)))
  assert effects==['parked_gate'] and not store.path.exists()
