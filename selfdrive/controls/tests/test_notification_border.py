"""Display emphasis and preference routing; no display/CAN injection required."""
import ast
import asyncio
import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace as NS

import pytest

ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('notification_preferences_under_test', ROOT/'selfdrive/controls/lib/notification_preferences.py')
pref = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pref)


@pytest.mark.parametrize('enabled', [False, True])
@pytest.mark.parametrize('critical,notification', [(False,False),(False,True),(True,False),(True,True)])
@pytest.mark.parametrize('now,on', [(0.,True),(.499,True),(.5,False),(.999,False),(1.,True)])
def test_border_channel_and_one_hz_pulse(enabled, critical, notification, now, on):
  result = pref.border_pulse(critical, notification, enabled, now)
  expected = (201,34,49,255) if critical else (218,111,37,255)
  assert result == (expected if on and (critical or notification and enabled) else None)


def test_separate_store_does_not_invalidate_driving_preferences(tmp_path):
  driving = tmp_path/'driving_preferences.json'
  original = b'{"schema":1,"values":{"LaunchFollowStrength":10}}'
  driving.write_bytes(original)
  store = pref.NotificationPreferences(tmp_path/'notification_preferences.json')
  assert store.read() == 1
  assert store.set(0) == store.read() == 0
  assert store.set(1) == store.read() == 1
  assert driving.read_bytes() == original
  assert not list(tmp_path.glob('*.tmp'))


@pytest.mark.parametrize('raw', ['null','[]','{"schema":1,"border":true}',
                               '{"schema":2,"border":0}','{"schema":1,"border":0,"extra":1}','{','x'*1025])
def test_invalid_store_preserves_visual_emphasis(tmp_path, raw):
  path=tmp_path/'notifications.json';path.write_text(raw)
  store=pref.NotificationPreferences(path)
  assert store.read() == 1 and path.read_text() == raw


@pytest.mark.parametrize('value', [True,False,-1,2,'0',0.5,None])
def test_invalid_writes_do_not_replace_previous_preference(tmp_path, value):
  store=pref.NotificationPreferences(tmp_path/'notification.json');store.set(0)
  before=store.path.read_bytes()
  with pytest.raises(ValueError):store.set(value)
  assert store.path.read_bytes()==before


def test_hud_routes_single_bulk_and_write_without_native_params(tmp_path, monkeypatch):
  from openpilot.selfdrive.carrot.server.services import params
  store=pref.NotificationPreferences(tmp_path/'notification.json')
  monkeypatch.setattr(params,'notification_preferences',store)
  class NoNative:
    def __init__(self):raise AssertionError('virtual border must never use native Params')
  monkeypatch.setattr(params,'Params',NoNative)
  monkeypatch.setattr(params,'HAS_PARAMS',True)
  params.set_param_value(pref.KEY,0)
  assert params.get_param_value(pref.KEY,1)==0
  assert params.get_param_values([pref.KEY],{pref.KEY:1})=={pref.KEY:0}
  params.set_param_value(pref.KEY,1)
  assert params.get_param_values([pref.KEY])=={pref.KEY:1}


def border_renderer():
  clock=NS(value=0.);draws=[]
  state=NS(alertSound=NS(raw=0),alertType='engage')
  sm=type('SM',(dict,),{})({'selfdriveState':state})
  sm.alive={'selfdriveState':True};sm.valid={'selfdriveState':True}
  sm.recv_frame={'selfdriveState':11};sm.recv_time={'selfdriveState':0.}
  ui=NS(sm=sm,started_frame=10)
  env={'time':NS(monotonic=lambda:clock.value),'ui_state':ui,'AlertStatus':NS(critical=2),
       'rl':NS(Rectangle=object,Color=lambda *args:args,
                                            draw_rectangle_lines_ex=lambda *args:draws.append(args))}
  tree=ast.parse((ROOT/'selfdrive/ui/onroad/alert_renderer.py').read_text(encoding='utf8'))
  node=next(n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name=='render_notification_border')
  exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual border render method>','exec'),env)
  store=NS(value=0)
  renderer=NS(_notification_preferences=NS(read=lambda:store.value),_notification_border_enabled=False,
              _notification_border_checked=float('-inf'),_notification_border_pulse=pref.BorderPulse(),alert=None)
  renderer.get_alert=lambda unused:renderer.alert
  return env['render_notification_border'],renderer,clock,draws,sm,store


def test_critical_border_survives_optional_off_and_car_state_loss():
  render,r,clock,draws,sm,store=border_renderer();r.alert=NS(status=2)
  sm.alive['selfdriveState']=False
  render(r,'screen');assert draws[-1]==('screen',10,(201,34,49,255))
  clock.value=.5;draws.clear();render(r,'screen');assert not draws
  clock.value=1.;render(r,'screen');assert draws[-1][2]==(201,34,49,255)


def test_routine_border_requires_fresh_state_and_tracks_live_preference():
  render,r,clock,draws,sm,store=border_renderer();sm['selfdriveState'].alertSound.raw=1
  render(r,'screen');assert not draws
  store.value=1;clock.value=1.;sm.recv_time['selfdriveState']=1.
  render(r,'screen');assert draws[-1][2]==(218,111,37,255)
  draws.clear();clock.value=2.;render(r,'screen');assert not draws
  sm.recv_time['selfdriveState']=2.;sm.valid['selfdriveState']=False
  render(r,'screen');assert not draws


def test_border_is_drawn_after_original_border_and_alerts_not_replaced():
  source=(ROOT/'selfdrive/ui/onroad/augmented_road_view.py').read_text(encoding='utf8')
  assert source.index('self.alert_renderer.render(self._content_rect)') < source.index('self._draw_border_carrot(rect)')
  assert source.index('self._draw_border_carrot(rect)') < source.index('self.alert_renderer.render_notification_border(rect)')


def test_reset_routes_display_default_without_unknown_native_key():
  tree=ast.parse((ROOT/'selfdrive/carrot/server/features/system.py').read_text(encoding='utf8'))
  node=next(n for n in ast.walk(tree) if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_set_default')
  writes=[];native=[]
  env={'HAS_PARAMS':True,'BORDER_KEY':pref.KEY,'DRIVING_PREFERENCES':{},
       'get_settings_cached':lambda:(None,None,{pref.KEY:{'default':1},'Existing':{'default':5}},None),
       '_is_carrot_default_reset_param':lambda *args:True,
       'notification_preferences':NS(set=lambda v:writes.append(v)),
       'restore_param_values_validated':lambda values:(native.append(values) or {'result':{'fail_cnt':0}}),
       'web':NS(Request=object,Response=object,HTTPException=RuntimeError,json_response=lambda body,status:body)}
  exec(compile(ast.Module(body=[node],type_ignores=[]),'<actual reset API>','exec'),env)
  result=asyncio.run(env['api_set_default'](None))
  assert writes==[1] and native==[{'Existing':5}]
  assert result['values'][pref.KEY]==1


@pytest.mark.parametrize('onset', [i/100 for i in range(100)])
def test_short_notification_never_misses_initial_flash(onset):
  pulse=pref.BorderPulse()
  for i in range(50):
    now=onset+i/100
    token=('engage',1) if i < 20 else None
    assert pulse.update(False,token,True,now)==(218,111,37,255)
  assert pulse.update(False,None,True,onset+.51) is None


def test_latched_flash_clears_on_disable_and_critical_preemption():
  pulse=pref.BorderPulse()
  assert pulse.update(False,'engage',True,.9) is not None
  assert pulse.update(False,None,False,1.) is None
  assert pulse.update(True,None,False,1.1)==(201,34,49,255)
  assert pulse.update(False,None,True,1.2) is None
