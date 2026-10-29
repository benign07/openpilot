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


def test_center_queue_beyond_first_lead_wins_without_selected_second_lead():
  h=LaunchFollow();r=scene();stopped(h,r)
  r.leadsCenter=[NS(status=True,radarTrackId=7,dRel=35.,vLead=0.,aLeadK=0.)]
  assert run(h,r,2_500_000_000)==1.3 and h.reason=='center_queue'


def test_reused_slot_range_jump_must_qualify_again():
  h=LaunchFollow();r=scene();stopped(h,r)
  assert run(h,r,2_500_000_000)>1.3
  r.leadOne.dRel=35.
  assert run(h,r,2_550_000_000)==1.3 and h.reason=='track_unqualified'


def test_mild_measurement_jitter_and_comfort_exit_do_not_snap():
  h=LaunchFollow();r=scene();stopped(h,r)
  for i in range(30):run(h,r,2_500_000_000+i*50_000_000)
  for i in range(8):
    r.leadOne.aLeadK=(-.01 if i%2 else .01)
    assert run(h,r,4_000_000_000+i*50_000_000)>1.3
  before=h.extra;r.leadOne.vRel=.15
  assert run(h,r,4_400_000_000)==pytest.approx(1.3+before-.015)
  r.leadsCutIn=[NS(status=True,dRel=30.)]
  assert run(h,r,4_450_000_000)==1.3


@pytest.mark.parametrize('margin,allowed',[(.99,False),(1.,False),(1.01,True),(1.5,True)])
def test_launch_activates_above_one_metre_of_target_gap(margin,allowed):
  h=LaunchFollow();r=scene();r.leadOne.dRel=8.+margin;stopped(h,r)
  assert (run(h,r,2_500_000_000)>1.3) is allowed


def growing_launch(helper,radar,count=45,*,strength=10,table_max=1.3):
  values=[]
  for i in range(count):
    relative=.6+.6*i*.05
    radar.leadOne.vRel=relative
    radar.leadOne.vLead=2.+relative
    radar.leadOne.dRel+=relative*.05
    values.append(run(helper,radar,2_500_000_000+i*50_000_000,
                      strength=strength,table_max=table_max))
  return values


def test_sustained_growing_opening_uses_the_configured_allowance_ceiling():
  h=LaunchFollow();r=scene();stopped(h,r)
  values=growing_launch(h,r)
  assert values[-1]==pytest.approx(1.43)
  assert h.relative_speed_growing and h.reason=='opening_growth_allowance'
  assert max(b-a for a,b in zip([1.3]+values,values))<=.15*.05+1e-12


@pytest.mark.parametrize('strength,table_max,expected',[(10,1.3,1.43),(20,1.3,1.56),(20,1.5,1.6),(20,2.,2.)])
def test_growth_respects_existing_strength_cap_and_never_lowers_stock_limit(strength,table_max,expected):
  h=LaunchFollow();r=scene();stopped(h,r)
  values=growing_launch(h,r,80,strength=strength,table_max=table_max)
  assert values[-1]==pytest.approx(expected)
  assert max(values)<=max(table_max,expected)+1e-12


@pytest.mark.parametrize('kind',['steady','jitter','single_spike','falling'])
def test_growth_requires_a_sustained_trend_not_a_sample_or_large_gap(kind):
  h=LaunchFollow();r=scene();stopped(h,r)
  for i in range(40):
    value={'steady':1.,'jitter':1.+(.03 if i%2 else -.03),
           'single_spike':1.6 if i==39 else 1.,'falling':2.-i*.02}[kind]
    r.leadOne.vRel=value;r.leadOne.vLead=2.+value;r.leadOne.dRel+=value*.05
    run(h,r,2_500_000_000+i*50_000_000)
  assert not h.relative_speed_growing
  assert h.extra<.13


def test_repeated_radar_stamp_cannot_age_the_opening_trend():
  h=LaunchFollow();r=scene();stopped(h,r)
  for i in range(20):
    r.leadOne.vRel=.6+i*.05;r.leadOne.vLead=2.+r.leadOne.vRel
    run(h,r,2_500_000_000)
  assert not h.relative_speed_growing
  assert len(h.relative_samples)==1


@pytest.mark.parametrize('kind',['new_id','range_jump','old_clock','long_gap','missing','nan'])
def test_growth_history_cannot_survive_unqualified_tracks(kind):
  h=LaunchFollow();r=scene();stopped(h,r);growing_launch(h,r)
  assert h.relative_speed_growing
  stamp=4_750_000_000
  if kind=='new_id':r.leadOne.radarTrackId+=1
  elif kind=='range_jump':r.leadOne.dRel+=10.
  elif kind=='old_clock':stamp=4_690_000_000
  elif kind=='long_gap':stamp=5_000_000_000
  elif kind=='missing':r.leadOne.status=False
  else:r.leadOne.vRel=float('nan')
  assert run(h,r,stamp)==1.3
  assert not h.relative_speed_growing and not h.relative_samples


@pytest.mark.parametrize('kind',['stop','approach','inactive','zero_strength','closing','braking','queue','cutin'])
def test_existing_safety_and_approach_exits_win_over_the_full_growth_ceiling(kind):
  h=LaunchFollow();r=scene();stopped(h,r);growing_launch(h,r)
  changes={}
  if kind=='stop':changes['should_stop']=True
  elif kind=='approach':changes['approach_active']=True
  elif kind=='inactive':changes['eligible']=False
  elif kind=='zero_strength':changes['strength']=0
  elif kind=='closing':r.leadOne.vRel=-.1
  elif kind=='braking':r.leadOne.aLeadK=-.2
  elif kind=='queue':r.leadsCenter=[NS(status=True,radarTrackId=8,dRel=r.leadOne.dRel+10.,vLead=0.,aLeadK=0.)]
  else:r.leadsCutIn=[NS(status=True,dRel=20.)]
  assert run(h,r,4_750_000_000,**changes)==1.3
  assert not h.relative_speed_growing and h.extra==0.


def full_launch():
  h=LaunchFollow();r=scene();stopped(h,r)
  for i in range(80):
    run(h,r,2_500_000_000+i*50_000_000,strength=20)
  assert h.extra==pytest.approx(.26)
  return h,r,6_500_000_000


@pytest.mark.parametrize('speed,factor',[(20.,1.),(30.,.5),(39.,.05),(40.,0.),(50.,0.),(60.,0.)])
def test_launch_speed_envelope_returns_to_the_existing_curve_by_40(speed,factor):
  h,r,stamp=full_launch()
  # Small speed increments preserve the same qualified radar track.
  for kph in range(8,int(speed)+1):
    r.leadOne.vLead=kph/3.6+r.leadOne.vRel
    value=run(h,r,stamp,v_ego=kph/3.6,strength=20)
    stamp+=50_000_000
  assert value==pytest.approx(1.3+.26*factor)
  if speed>=40.:
    assert h.extra==0. and h.stopped_ns==0


def test_mild_withdrawal_cannot_leak_extra_past_the_speed_envelope():
  h,r,stamp=full_launch()
  for kph in range(8,39):
    r.leadOne.vLead=kph/3.6+3.
    run(h,r,stamp,v_ego=kph/3.6,strength=20);stamp+=50_000_000
  r.leadOne.vRel=.15;r.leadOne.vLead=39/3.6+.15
  value=run(h,r,stamp,v_ego=39/3.6,strength=20)
  assert 1.3<=value<=1.3+.26*.05
  assert run(h,r,stamp+50_000_000,v_ego=40/3.6,strength=20)==1.3
  assert h.extra==0.


@pytest.mark.parametrize('approach_active',[False,True])
def test_reaching_40_retires_this_departure_until_a_fresh_standstill(approach_active):
  h,r,stamp=full_launch()
  for kph in range(8,41):
    r.leadOne.vLead=kph/3.6+3.
    run(h,r,stamp,v_ego=kph/3.6,strength=20,approach_active=approach_active)
    stamp+=50_000_000
  for kph in range(39,6,-1):
    r.leadOne.vLead=kph/3.6+3.
    assert run(h,r,stamp,v_ego=kph/3.6,strength=20)==1.3
    stamp+=50_000_000
  for i in range(30):
    run(h,r,stamp,v_ego=0.,should_stop=True,strength=20);stamp+=50_000_000
  r.leadOne.vLead=5.
  assert run(h,r,stamp,v_ego=2.,strength=20)>1.3


@pytest.mark.parametrize('bad', [{'desired_distance':float('nan')},{'table_max':float('nan')},{'v_ego':float('nan')}])
def test_invalid_limits_retire_the_departure_even_if_the_speed_boundary_is_unobservable(bad):
  h,r,stamp=full_launch()
  changes={'v_ego':40/3.6,'strength':20,**bad}
  run(h,r,stamp,**changes)
  assert h.stopped_ns==0 and h.extra==0.
  assert run(h,r,stamp+50_000_000,strength=20)==1.3


def test_boundary_chatter_cannot_reenable_a_completed_departure():
  h,r,stamp=full_launch()
  r.leadOne.vLead=40/3.6+3.
  for kph in (40.,39.99,40.01,39.99):
    assert run(h,r,stamp,v_ego=kph/3.6,strength=20)==1.3
    stamp+=50_000_000
  assert h.stopped_ns==0


@pytest.mark.parametrize('table',[1.3,1.4])
def test_realistic_acceleration_tapers_the_cap_without_a_boundary_step(table):
  h,r,stamp=full_launch();values=[];v=2.
  for i in range(145):
    r.leadOne.vLead=v+3.
    cap=run(h,r,stamp,v_ego=v,strength=20,table_max=table)
    assert table<=cap<=max(table,1.6)
    if v>=40/3.6:assert cap==table
    values.append(cap);v+=1.5*.05;stamp+=50_000_000
  assert all(a-b<=.3*.05+1e-12 for a,b in zip(values,values[1:]))


@pytest.mark.parametrize('curvature,previous_accel',[(0.,.5),(.015,.5),(0.,1.5)])
def test_production_planner_launch_block_sends_identical_mpc_limits_at_mid_speed(curvature,previous_accel):
  import ast,math,struct
  from pathlib import Path
  from openpilot.selfdrive.controls.lib.approach_comfort import ApproachRequest,apply_approach_ceiling
  path=Path(__file__).resolve().parents[3]/'selfdrive/controls/lib/longitudinal_planner.py'
  tree=ast.parse(path.read_text(encoding='utf-8'))
  update=next(n for n in next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='LongitudinalPlanner').body
              if isinstance(n,ast.FunctionDef) and n.name=='update')
  first=next(i for i,n in enumerate(update.body) if isinstance(n,ast.Assign) and
             isinstance(n.targets[0],ast.Name) and n.targets[0].id=='launch_table_maximum')
  block=update.body[first:first+3]
  assert 'apply_approach_ceiling' in ast.unparse(block[-1])
  setter=next(n for n in update.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and
              isinstance(n.value.func,ast.Attribute) and n.value.func.attr=='set_accel_limits')
  turn=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='limit_accel_in_turns')
  code=compile(ast.Module(body=block+[setter],type_ignores=[]),str(path),'exec')
  outputs=[]
  for strength in (0,20):
    helper,radar,stamp=full_launch();sent=[];rows=[]
    self=NS(launch_follow=helper,drive_settings={'LaunchFollowStrength':strength},
            output_should_stop=False,approach=NS(coast_weight=0.,traffic_weight=0.),
            a_desired=previous_accel,dt=.05,mpc=NS(set_accel_limits=lambda *x:sent.append(struct.pack('!dd',*x))))
    sm=type('SM',(dict,),{})({'radarState':radar});sm.logMonoTime={}
    scope={'math':math,'self':self,'sm':sm,'carrot':NS(stop_distance=5.5,t_follow_last=1.),
           'launch_eligible':True,'curvature_future':curvature,'a_lat_max':3.,
           'approach_request':ApproachRequest(),'apply_approach_ceiling':apply_approach_ceiling}
    exec(compile(ast.Module(body=[turn],type_ignores=[]),str(path),'exec'),scope)
    for kph in [*range(8,46),44.,42.,40.,39.99]:
      v=kph/3.6;radar.leadOne.vLead=v+3.
      sm.logMonoTime['radarState']=stamp;stamp+=50_000_000
      limits=scope['limit_accel_in_turns'](v,curvature,[-2.,1.3],3.)
      limits[1]=max(limits[1],previous_accel-.05)
      scope.update(v_ego=v,evidence_table_maximum=1.3,accel_limits=[-2.,1.3],accel_limits_turns=limits)
      exec(code,scope)
      rows.append((kph,sent[-1]))
    outputs.append(rows)
  for (speed,baseline),(_,boosted) in zip(*outputs):
    if speed>=40. or speed==39.99:assert boosted==baseline
  if curvature==0. and previous_accel==.5:
    assert any(a[1]!=b[1] for a,b in zip(*outputs) if a[0]<20.)


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
  from contextlib import asynccontextmanager
  from pathlib import Path
  from aiohttp import web
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES
  # Execute the actual endpoint without importing the package initializer,
  # which eagerly imports Linux hardware services unrelated to this route.
  path=Path(__file__).resolve().parents[3]/'selfdrive/carrot/server/features/params.py'
  node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body
            if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_param_set')
  writes=[]
  @asynccontextmanager
  async def write_lock(_):
    yield
  async def settled_write(fn,*args):
    fn(*args)
  scope={'web':web,'DRIVING_PREFERENCES':DRIVING_PREFERENCES,'CRUISE_BUTTON_PREFERENCES':{},'RESPONSE_PREFERENCES':{},'HAS_PARAMS':False,
         'get_settings_cached':lambda: ({},{},{},[]),'set_param_value':lambda *args:writes.append(args),
         'settings_write_lock':write_lock,'settled_setting_write':settled_write}
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
  async def body():return {'name':'LaunchFollowStrength','value':10}
  request=NS(json=body)
  async def blocked(_):raise web.HTTPConflict(text='PARKED_REQUIRED')
  scope['require_parked']=blocked
  with pytest.raises(web.HTTPConflict):asyncio.run(scope['api_param_set'](request))
  assert not writes
  checks=[]
  async def parked(_):checks.append(True)
  scope['require_parked']=parked
  assert asyncio.run(scope['api_param_set'](request)).status==200 and writes
  assert len(checks)==2


def test_defaults_reset_virtual_store_and_parked_rejection_precedes_native_reset(tmp_path):
  import asyncio,ast
  from pathlib import Path
  from aiohttp import web
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES
  path=Path(__file__).resolve().parents[3]/'selfdrive/carrot/server/features/system.py'
  node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body
            if isinstance(n,ast.AsyncFunctionDef) and n.name=='api_set_default')
  p=DrivingPreferences(tmp_path/'prefs.json');p.set('YellowLineBiasCm',5);p.set('LateralMarginShadow',0)
  native=[]
  metadata={k:{'default':v[2]} for k,v in DRIVING_PREFERENCES.items()}
  metadata['stock']={'default':1}
  def restore(values):
    native.append(values);return {'result':{'fail_cnt':0},'preview':{'entries':[]}}
  scope={'web':web,'DRIVING_PREFERENCES':DRIVING_PREFERENCES,'CRUISE_BUTTON_PREFERENCES':{},'RESPONSE_PREFERENCES':{},'HAS_PARAMS':True,
    'BORDER_KEY':'NotificationBorderPulse',
    'get_settings_cached':lambda: ({},{},metadata,[]),
    '_is_carrot_default_reset_param':lambda *args:True,
    'driving_preferences':p,'restore_param_values_validated':restore}
  exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
  async def blocked(_):raise web.HTTPConflict(text='PARKED_REQUIRED')
  scope['require_parked']=blocked
  with pytest.raises(web.HTTPConflict):asyncio.run(scope['api_set_default'](None))
  assert native==[] and p.read()['YellowLineBiasCm']==5
  async def parked(_):pass
  scope['require_parked']=parked
  response=asyncio.run(scope['api_set_default'](None))
  assert response.status==200 and native==[{'stock':1}]
  assert p.read()['YellowLineBiasCm']==0 and p.read()['LateralMarginShadow']==1


def test_explicit_defaults_preserves_corrupt_preference_file(tmp_path):
  path=tmp_path/'prefs.json';path.write_bytes(b'broken')
  p=DrivingPreferences(path)
  assert p.read()['YellowLineBiasCm']==0 and p.error
  defaults=p.reset_defaults()
  assert defaults['LaunchFollowStrength']==defaults['YellowLineBiasCm']==0
  assert defaults['LateralMarginShadow']==1 and not p.error
  assert next(tmp_path.glob('prefs.json.invalid.*')).read_bytes()==b'broken'


def test_development_menu_keeps_parameter_contract_and_has_no_duplicate_entries():
  import copy
  from pathlib import Path
  from collections import Counter
  from openpilot.selfdrive.carrot.server.services.settings import inject_driving_preferences,build_menu_categories
  from openpilot.selfdrive.controls.lib.approach_comfort import CRUISE_BUTTON_PREFERENCES,RESPONSE_PREFERENCES
  from openpilot.selfdrive.controls.lib.notification_preferences import KEY as BORDER_KEY
  path=Path(__file__).resolve().parents[3]/'selfdrive/carrot_settings.json'
  original=json.loads(path.read_text(encoding='utf-8'))
  data=inject_driving_preferences(copy.deepcopy(original))
  def names(nodes):
    for node in nodes:
      yield from node.get('params',[])
      yield from names(node.get('groups',[]))
  before=Counter(names(original['menu']));after=Counter(names(data['menu']))
  assert set(after)==set(before)|set(DrivingPreferences().values)|set(CRUISE_BUTTON_PREFERENCES)|set(RESPONSE_PREFERENCES)|{BORDER_KEY}
  assert all(value==1 for value in after.values())
  assert data['menu'][0]['id']=='development_features' and data['menu'][0]['ko']=='개발 중 기능'
  moved=set(names([data['menu'][0]]))
  assert set(DrivingPreferences().values)<=moved
  assert {'ApproachComfortEnabled','ApproachFlexibleGap','ApproachTrafficCalm'}<=moved
  assert {'EnableCornerRadar','ManualSteerWithBlinker'} <= moved
  by_name={p['name']:p for p in data['params']}
  for item in original['params']:
    presentation={'group','egroup','cgroup'}
    if item['name']=='EnableCornerRadar':
      presentation.update(('title','etitle','ctitle','descr','edescr','cdescr'))
      assert by_name[item['name']]['title']=='주변 대상 상시 표시'
    if item['name']=='ManualSteerWithBlinker':
      presentation.update(('descr','edescr','cdescr'))
      assert '0.2초' in by_name[item['name']]['descr']
    if item['name'] in ('C3xLiteBuzzerMute','C3xLiteHapticNotifications'):
      presentation.update(('title','etitle','ctitle','descr','edescr','cdescr'))
    if item['name'] in ('ApproachCoastStrength','ApproachTrafficCalm'):
      presentation.update(('descr','edescr','cdescr'))
      for field in ('descr','edescr','cdescr'):
        assert by_name[item['name']][field].startswith(item[field])
      assert '가속 억제' in by_name[item['name']]['descr']
    assert {k:v for k,v in item.items() if k not in presentation}=={
      k:v for k,v in by_name[item['name']].items() if k not in presentation}
  categories=build_menu_categories(data,by_name)
  assert categories[0]['groups'][0]['id']=='lx3_preferences_group'
  assert categories[0]['groups'][0]['count']==len(moved)
  assert inject_driving_preferences(copy.deepcopy(data))==data
