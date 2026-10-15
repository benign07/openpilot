import json
from types import SimpleNamespace as NS

import pytest

from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences, LaunchFollow


def scene():
  lead = NS(status=True, radar=True, radarTrackId=1, dRel=20., vRel=3., vLead=5., aLeadK=.2)
  return NS(leadOne=lead, leadTwo=NS(status=False), leadsCutIn=[])


def run(helper, radar, stamp, **changes):
  args = dict(strength=10, stamp_ns=stamp, eligible=True, should_stop=False,
              v_ego=2., desired_distance=8., table_max=1.3, approach_active=False)
  args.update(changes)
  return helper.update(radar=radar, **args)


def stopped(helper, radar):
  for i in range(30):
    assert run(helper, radar, 1_000_000_000+i*50_000_000, v_ego=0., should_stop=True) == 1.3


def test_opening_launch_allowance_is_bounded_and_slewed():
  h=LaunchFollow();r=scene();stopped(h,r)
  values=[run(h,r,2_500_000_000+i*50_000_000) for i in range(30)]
  assert 1.3 < values[0] < values[-1] <= 1.43+1e-12
  assert max(b-a for a,b in zip([1.3]+values,values)) <= .15*.05+1e-12
  assert h.reason == 'opening_allowance'


@pytest.mark.parametrize('change', [
  {'strength':0}, {'eligible':False}, {'should_stop':True}, {'v_ego':.5},
  {'approach_active':True}, {'desired_distance':30.}, {'v_ego':60/3.6},
])
def test_hold_approach_pedals_and_existing_limits_win(change):
  h=LaunchFollow();r=scene();stopped(h,r)
  assert run(h,r,2_500_000_000,**change) == 1.3


@pytest.mark.parametrize('changes', [
  {'radar':False}, {'status':False}, {'radarTrackId':2}, {'vRel':0.},
  {'vLead':.5}, {'aLeadK':-.01}, {'dRel':5.}, {'aLeadK':float('nan')},
])
def test_new_creeping_closing_or_unconfirmed_lead_never_gets_allowance(changes):
  h=LaunchFollow();r=scene();stopped(h,r)
  for key,value in changes.items():setattr(r.leadOne,key,value)
  assert run(h,r,2_500_000_000) == 1.3


def test_second_lead_and_cut_in_remove_the_allowance():
  h=LaunchFollow();r=scene();stopped(h,r)
  r.leadTwo=NS(status=True,dRel=18.,vRel=-.5,aLeadK=0.)
  assert run(h,r,2_500_000_000) == 1.3
  r.leadTwo.status=False;r.leadsCutIn=[NS(status=True,dRel=40.)]
  assert run(h,r,2_550_000_000) == 1.3


def test_not_a_general_cruising_acceleration_boost_or_repeated_frame_timer():
  h=LaunchFollow();r=scene()
  for i in range(100):assert run(h,r,1_000_000_000) == 1.3
  for i in range(100):assert run(h,r,2_000_000_000+i*50_000_000) == 1.3


def test_preference_store_invalid_or_missing_is_all_off(tmp_path):
  f=tmp_path/'prefs.json';p=DrivingPreferences(f)
  assert p.read()['LaunchFollowStrength'] == 0 and p.read()['LateralMarginShadow'] == 1
  for row in ({'schema':2,'values':{}}, {'schema':1,'values':{'LaunchFollowStrength':float('nan')}},
              {'schema':1,'values':{'LaunchFollowStrength':True}}, {'schema':1,'values':{'unknown':1}}):
    f.write_text(json.dumps(row),encoding='utf-8')
    assert p.read()['LaunchFollowStrength'] == 0 and p.error
    with pytest.raises(ValueError):p.set('LaunchFollowStrength',10)
    assert json.loads(f.read_text(encoding='utf-8'))['schema'] == row['schema']


def test_versioned_preferences_preserve_other_keys_and_never_need_native_params(tmp_path):
  f=tmp_path/'prefs.json';p=DrivingPreferences(f)
  p.set('LaunchFollowStrength',10);p.set('YellowLineBiasCm',5)
  q=DrivingPreferences(f)
  assert q.read()['LaunchFollowStrength'] == 10 and q.read()['YellowLineBiasCm'] == 5
  p.set('LaunchFollowStrength',0)
  assert q.read()['LaunchFollowStrength'] == 0
  assert not list(tmp_path.glob('*.tmp'))
  for value in (-1,21,10.1,True,'10'):
    with pytest.raises(ValueError):p.set('LaunchFollowStrength',value)


def test_server_virtual_keys_never_reach_params_or_profiles(tmp_path,monkeypatch):
  from openpilot.selfdrive.carrot.server.services import params,setting_profiles
  p=DrivingPreferences(tmp_path/'prefs.json')
  monkeypatch.setattr(params,'driving_preferences',p)
  monkeypatch.setattr(params,'HAS_PARAMS',True)
  monkeypatch.setattr(params,'Params',lambda: (_ for _ in ()).throw(AssertionError('native Params called')))
  params.set_param_value('LaunchFollowStrength',10)
  assert params.get_param_value('LaunchFollowStrength',0)==10
  assert params.get_param_values(['LaunchFollowStrength'])['LaunchFollowStrength']==10
  monkeypatch.setattr(setting_profiles,'get_settings_cached',lambda: ({},{},{
    'LaunchFollowStrength':{'virtual':True,'default':0},'stock':{'default':1}},[]))
  assert setting_profiles._clean_values({'LaunchFollowStrength':20,'stock':2})=={'stock':2}
  assert setting_profiles._setting_defaults()=={'stock':1}


def test_new_virtual_setting_api_requires_parked_before_writing(monkeypatch):
  import asyncio,ast
  from pathlib import Path
  from aiohttp import web
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES
  # Execute the actual endpoint without importing the package initializer,
  # which eagerly imports Linux hardware services unrelated to this route.
  path=Path(__file__).resolve().parents[3]/'selfdrive/carrot/server/features/params.py'
  node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body
            if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_param_set')
  writes=[]
  scope={'web':web,'DRIVING_PREFERENCES':DRIVING_PREFERENCES,'HAS_PARAMS':False,
         'get_settings_cached':lambda: ({},{},{},[]),'set_param_value':lambda *args:writes.append(args)}
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
  async def body():return {'name':'LaunchFollowStrength','value':10}
  request=NS(json=body)
  async def blocked(_):raise web.HTTPConflict(text='PARKED_REQUIRED')
  scope['require_parked']=blocked
  with pytest.raises(web.HTTPConflict):asyncio.run(scope['api_param_set'](request))
  assert not writes
  async def parked(_):pass
  scope['require_parked']=parked
  assert asyncio.run(scope['api_param_set'](request)).status==200 and writes
