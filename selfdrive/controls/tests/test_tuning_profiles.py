"""Portable profile regression; real API/helpers with isolated settings and a test parked guard.
No CAN/actuator transport. Tests do not qualify vehicle behavior or real parked detection.
"""
import asyncio
from copy import deepcopy
import json
from pathlib import Path
import threading
import uuid
import pytest
from aiohttp import web
from openpilot.selfdrive.carrot.server.services import setting_profiles as service, params, settings
import importlib.util
from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences

ROOT=Path(__file__).resolve().parents[3]
# Load this production route module directly; features/__init__ eagerly starts
# unrelated native camera/logger imports unavailable in a portable unit test.
_spec=importlib.util.spec_from_file_location('openpilot.selfdrive.carrot.server.features.setting_profiles',ROOT/'selfdrive/carrot/server/features/setting_profiles.py')
routes=importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(routes)
PROFILES=json.loads((ROOT/'selfdrive/carrot/tests/tuning_profiles_v1.json').read_text(encoding='utf8'))['profiles']

class Request:
  content_length=None
  def __init__(self,body,app=None):self.body=deepcopy(body);self.app={} if app is None else app
  async def json(self):return self.body
  async def text(self):return json.dumps(self.body)

def decoded(response):return json.loads(response.body)

@pytest.fixture
def device(tmp_path,monkeypatch):
  monkeypatch.setitem(settings.settings_cache,'path',str(ROOT/'selfdrive/carrot_settings.json'))
  # Cache is empty in this isolated test process, source metadata is authoritative.
  values={k:str(v).encode() for k,v in PROFILES[0]['values'].items() if k!='LaunchFollowStrength'}
  values['ApproachCoastStrength']=b'40';values['ApproachBrakeMargin']=b'40';values['ApproachStopSmooth']=b'40'
  writes=[]
  class Types:
    INT=1
    BOOL=2
  class Params:
    def get_type(self,k):return Types.BOOL if k in ('ApproachComfortEnabled','ApproachFlexibleGap') else Types.INT
    def get(self,k,*a,**kw):raise AssertionError('typed wrapper get must not read tuning values')
    def get_param_path(self,k):
      path=tmp_path/k
      if k in values:path.write_bytes(values[k])
      elif path.exists():path.unlink()
      return str(path)
    def put(self,k,v):values[k]=v if isinstance(v,bytes) else str(v).encode();writes.append((k,v))
    def put_bool(self,k,v):self.put(k,int(v))
    def put_int(self,k,v):self.put(k,v)
    def put_float(self,k,v):self.put(k,v)
  monkeypatch.setattr(params,'HAS_PARAMS',True);monkeypatch.setattr(params,'Params',Params);monkeypatch.setattr(params,'ParamKeyType',Types)
  pref=DrivingPreferences(tmp_path/'preferences.json');pref.set('LaunchFollowStrength',10);monkeypatch.setattr(params,'driving_preferences',pref)
  monkeypatch.setattr(service,'TUNING_STATE_PATH',str(tmp_path/'transaction.json'))
  monkeypatch.setattr(service,'_tuning_serial',lambda:'test-device-A')
  context={'serial':'test-device-A','fingerprint':'LX3-test-only'};monkeypatch.setattr(service,'_tuning_context',lambda:dict(context))
  guard={'parked':True,'calls':0,'fail_at':None}
  async def parked(request):
    guard['calls']+=1
    if guard['fail_at'] is not None and guard['calls']>=guard['fail_at']:guard['parked']=False
    if not guard['parked']:raise web.HTTPConflict(text='test parked guard denied')
  monkeypatch.setattr(routes,'require_parked',parked)
  async def proof(request):
    await parked(request)
    return {'test_proof': True}
  def check(proof):
    if not guard['parked']:raise ValueError('test parked proof denied')
  monkeypatch.setattr(routes,'parked_write_proof',proof)
  monkeypatch.setattr(routes,'check_parked_write_proof',check)
  return values,writes,pref,guard,context

def apply(profile,app=None,**body):
  preview=service.preview_tuning_profile(profile)
  return asyncio.run(routes.apply_tuning_route(Request({'profile':profile,'profile_hash':service.tuning_profile_digest(profile),'revision':preview['revision'],'serial':preview['context']['serial'],'request_id':uuid.uuid4().hex,**body},app)))

@pytest.mark.parametrize('profile',PROFILES,ids=lambda p:p['id'])
def test_shared_full_domain_profiles_validate(profile,device):
  assert set(service.validate_tuning_profile(profile))==set(service.TUNING_UNITS)
  assert len(profile['values'])==27
  assert not {'DisableDM','UseLaneLineSpeed','HardwareC3xLite'} & profile['values'].keys()

@pytest.mark.parametrize('bad',[2,True,'1'])
def test_unknown_or_untyped_schema_rejected(bad,device):
  p=deepcopy(PROFILES[0]);p['schema']=bad
  with pytest.raises(ValueError):service.validate_tuning_profile(p)

@pytest.mark.parametrize('bad',[True,60.0,'60',10000])
def test_noninteger_out_of_range_rejected(bad,device):
  p=deepcopy(PROFILES[0]);p['values']['ApproachCoastStrength']=bad
  with pytest.raises(ValueError):service.validate_tuning_profile(p)

@pytest.mark.parametrize('mutation',['unit','unknown','family','units_missing','pc_pack'])
def test_no_filtering_or_cross_device_or_full_pack(mutation,device):
  p=deepcopy(PROFILES[0])
  if mutation=='unit':p['units']['TFollowGap1']='seconds'
  if mutation=='unknown':p['values']['DisableDM']=0;p['units']['DisableDM']='bool-int'
  if mutation=='family':p['family']='EV9'
  if mutation=='units_missing':p['units'].pop('TFollowGap1')
  if mutation=='pc_pack':p={'Format':'CarrotTuningLab-v1','B':p['values']}
  with pytest.raises(ValueError):service.validate_tuning_profile(p)
  assert not device[1]

def test_missing_native_value_is_not_source_default(device):
  device[0].pop('TFollowGap1')
  with pytest.raises(ValueError,match='저장값'):service.tuning_snapshot()

def test_bad_virtual_store_is_preserved(device):
  path=device[2].path;path.write_text('{bad')
  with pytest.raises(ValueError,match='선호값'):service.tuning_snapshot()
  assert path.read_text()=='{bad'

def test_read_only_preview_and_stale_revision(device):
  p=deepcopy(PROFILES[0]);preview=service.preview_tuning_profile(p);assert not device[1]
  device[0]['MyDrivingMode']=b'2'
  r=asyncio.run(routes.apply_tuning_route(Request({'profile':p,'profile_hash':service.tuning_profile_digest(p),'revision':preview['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex})))
  assert r.status==409 and not device[1] and not Path(service.TUNING_STATE_PATH).exists()

def test_full_domain_apply_readback_and_repeat_request(device):
  p=deepcopy(PROFILES[3]);v=service.preview_tuning_profile(p);body={'profile':p,'profile_hash':service.tuning_profile_digest(p),'revision':v['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex}
  r=asyncio.run(routes.apply_tuning_route(Request(body)));assert r.status==200,decoded(r)
  assert service.tuning_snapshot()['values']==p['values']
  assert service.tuning_snapshot()['current_matches_last']
  count=len(device[1]);again=asyncio.run(routes.apply_tuning_route(Request(body)));assert again.status==200 and len(device[1])==count
  body['profile']=deepcopy(PROFILES[0]);assert asyncio.run(routes.apply_tuning_route(Request(body))).status==409
  assert len(device[1])==count

def test_write_then_raise_rolls_back_and_remembers_attempt(device,monkeypatch):
  old=service._tuning_values();real=service.tuning_write;failed=False
  def write(k,v):
    nonlocal failed
    real(k,v)
    if not failed:failed=True;raise OSError('test after write failure')
  monkeypatch.setattr(service,'tuning_write',write)
  r=apply(PROFILES[0]);assert r.status==409 and decoded(r)['restored']
  assert service._tuning_values()==old and service._tuning_state()['phase']=='rolled_back'

def test_transition_blocks_writes_and_requires_explicit_recovery(device):
  old=service._tuning_values();device[3]['fail_at']=5
  p=deepcopy(PROFILES[0]);r=apply(p);assert r.status==409
  receipt=service._tuning_state();assert receipt['phase']=='recovery_required' and len(device[1])==1
  assert service.tuning_snapshot()['recovery_required']
  device[3].update(parked=True,fail_at=None)
  denied=asyncio.run(routes.apply_tuning_route(Request({'profile':p,'profile_hash':service.tuning_profile_digest(p),'revision':'x','serial':'test-device-A','request_id':uuid.uuid4().hex})));assert denied.status==409
  recovered=asyncio.run(routes.recover_tuning_route(Request({'request_id':receipt['request_id']})))
  assert recovered.status==200 and service._tuning_values()==old

def test_external_writer_not_overwritten_on_restore(device,monkeypatch):
  real=service.tuning_write;first=True
  def write(k,v):
    nonlocal first
    real(k,v)
    if first:first=False;device[0][k]=b'55';raise OSError('external concurrent edit')
  monkeypatch.setattr(service,'tuning_write',write)
  r=apply(PROFILES[0]);assert r.status==409 and not decoded(r)['restored']
  assert device[0]['ApproachCoastStrength']==b'55' and service._tuning_state()['phase']=='recovery_required'

@pytest.mark.parametrize('raw',['{broken','{}','{"schema":2,"phase":"complete"}'])
def test_corrupt_receipt_survives_and_blocks_apply(raw,device):
  path=Path(service.TUNING_STATE_PATH);path.write_text(raw)
  assert service.tuning_snapshot()['recovery_required']
  with pytest.raises(ValueError,match='복구'):service.preview_tuning_profile(PROFILES[0])
  assert path.read_text()==raw and not device[1]

def test_cancellation_waits_for_thread_and_leaves_recoverable_intent(device,monkeypatch):
  entered=threading.Event();release=threading.Event();real=service.tuning_write
  def write(k,v):entered.set();release.wait();real(k,v)
  monkeypatch.setattr(service,'tuning_write',write)
  async def exercise():
    p=PROFILES[0];v=service.preview_tuning_profile(p);req=Request({'profile':p,'profile_hash':service.tuning_profile_digest(p),'revision':v['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex})
    task=asyncio.create_task(routes.apply_tuning_route(req))
    await asyncio.to_thread(entered.wait);task.cancel();await asyncio.sleep(.02)
    assert not task.done() and req.app['settings_write_lock'].locked()
    release.set()
    with pytest.raises(asyncio.CancelledError):await task
    assert service._tuning_state()['phase']=='recovery_required'
    assert service.tuning_snapshot()['recovery_required']
  try:asyncio.run(exercise())
  finally:release.set()

def test_actual_parked_guard_no_state_refuses_apply(device,monkeypatch):
  from openpilot.selfdrive.carrot.server.services.setting_safety import require_parked
  monkeypatch.setattr(routes,'require_parked',require_parked)
  with pytest.raises(web.HTTPConflict):apply(PROFILES[0])
  assert not device[1]


@pytest.mark.parametrize('raw',[b'',b'abc',b' 130',b'130\n',b'+130',b'1_30',b'130.0',b'0130',b'-0'])
def test_raw_numeric_reader_rejects_noncanonical_bytes(raw,device):
  device[0]['CruiseMaxVals0']=raw
  with pytest.raises(ValueError):service._tuning_values()

@pytest.mark.parametrize('raw',[b'2',b'true',b'01'])
def test_raw_boolean_reader_requires_zero_or_one(raw,device):
  device[0]['ApproachComfortEnabled']=raw
  with pytest.raises(ValueError):service._tuning_values()

def test_duplicate_and_nan_request_json_rejected(device):
  for raw in ['{"profile":{},"profile":{}}','{"profile":{"values":{"x":1,"x":2}}}','{"x":NaN}']:
    with pytest.raises(ValueError):service.strict_tuning_json(raw)

def test_virtual_key_written_last(device):
  sequence=[];import unittest.mock as mock
  real=service.tuning_write
  def write(k,v):sequence.append(k);return real(k,v)
  with mock.patch.object(service,'tuning_write',write):assert apply(PROFILES[3]).status==200
  assert sequence[-1]=='LaunchFollowStrength'


def test_native_types_and_virtual_store_contract_are_not_silently_changed(device):
  import re
  declarations=(ROOT/'common/params_keys.h').read_text(encoding='utf8')
  from openpilot.selfdrive.controls.lib.approach_comfort import DRIVING_PREFERENCES
  for k,u in service.TUNING_UNITS.items():
    if k=='LaunchFollowStrength':
      assert DRIVING_PREFERENCES[k]==(0,20,0)
      assert service.tuning_contract()['fields'][k]['min']==0
      assert service.tuning_contract()['fields'][k]['max']==20
    else:
      match=re.search(r'"'+k+r'"\s*,\s*\{[^}]*?\b(INT|BOOL|FLOAT)\b',declarations)
      assert match is not None
      assert match.group(1)==('BOOL' if k in ('ApproachComfortEnabled','ApproachFlexibleGap') else 'INT')


def test_dedicated_settings_executor_not_blocked_by_default_pool(device):
  from concurrent.futures import ThreadPoolExecutor
  from openpilot.selfdrive.carrot.server.services.setting_safety import settled_setting_write
  entered=threading.Event();release=threading.Event()
  async def exercise():
    loop=asyncio.get_running_loop();loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
    def blocked():entered.set();release.wait()
    occupied=loop.run_in_executor(None,blocked)
    while not entered.is_set():await asyncio.sleep(.001)
    try:assert await asyncio.wait_for(settled_setting_write(lambda:42),1)==42
    finally:release.set();await occupied
  asyncio.run(exercise())

def test_stale_parked_proof_never_runs_a_setting_write():
  from openpilot.selfdrive.carrot.server.services import setting_safety
  with pytest.raises(ValueError,match='대기'):
    setting_safety.check_parked_write_proof({'observed':setting_safety.time.monotonic()-.6,'sm':None})

def test_checksum_corruption_blocks_and_preserves_receipt(device):
  assert apply(PROFILES[0]).status==200
  path=Path(service.TUNING_STATE_PATH);row=json.loads(path.read_text())
  row['payload']['after']['ApproachCoastStrength']=61
  raw=json.dumps(row);path.write_text(raw)
  assert service.tuning_snapshot()['recovery_required']
  assert path.read_text()==raw


def test_receipt_is_readable_when_raw_parameter_is_broken(device):
  assert apply(PROFILES[0]).status==200
  device[0]['ApproachCoastStrength']=b'bad'
  with pytest.raises(ValueError):service.tuning_snapshot()
  assert service.tuning_receipt()['receipt']['phase']=='complete'

def test_runtime_registered_type_mismatch_is_rejected_before_write(device,monkeypatch):
  monkeypatch.setattr(params.Params,'get_type',lambda self,key:999)
  with pytest.raises(ValueError,match='등록 타입'):service.tuning_snapshot()
  assert not device[1]


def test_schema_one_declared_bounds_and_defaults_drift_requires_review(device):
  golden=json.loads((ROOT/'selfdrive/carrot/tests/tuning_profiles_v1.json').read_text(encoding='utf8'))['contract']['fields']
  fields=service.tuning_contract()['fields'];_,_,meta,_=service.get_settings_cached()
  assert set(golden)==set(fields)==set(service.TUNING_UNITS)
  for k,expected in golden.items():
    assert fields[k]['unit']==expected['unit']
    assert fields[k]['min']==expected['min'] and fields[k]['max']==expected['max']
    assert meta[k]['default']==expected['default']


def test_committed_apply_never_reads_a_response_snapshot_or_undoes(device,monkeypatch):
  p=PROFILES[0];v=service.preview_tuning_profile(p);real=service.tuning_snapshot;calls=0
  def snapshot():
    nonlocal calls
    calls+=1
    if calls>2:raise ValueError('response-only read failure')
    return real()
  monkeypatch.setattr(service,'tuning_snapshot',snapshot)
  r=asyncio.run(routes.apply_tuning_route(Request({'profile':p,'profile_hash':service.tuning_profile_digest(p),'revision':v['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex})))
  assert r.status==200 and decoded(r)['ok'] and calls==2
  assert service._tuning_state()['phase']=='complete'
  assert len(device[1])==3

@pytest.mark.parametrize('case',['corrupt','foreign_complete','foreign_pending','future_schema'])
def test_explicit_hash_bound_archive_preserves_bytes_and_params(case,device):
  path=Path(service.TUNING_STATE_PATH)
  if case in ('foreign_complete','foreign_pending'):
    assert apply(PROFILES[0]).status==200
    row=service._tuning_state();row['context']['serial']='another-device'
    if case=='foreign_pending':row['phase']='recovery_required'
    service.save_tuning_state(row)
  else:path.write_text('{broken' if case=='corrupt' else '{"journal_schema":2}')
  before=dict(device[0]);count=len(device[1]);raw=path.read_bytes();digest=service.tuning_journal_digest()
  r=asyncio.run(routes.archive_tuning_route(Request({'serial':'test-device-A','journal_sha256':digest})))
  assert r.status==200 and service._tuning_state() is None
  assert dict(device[0])==before and len(device[1])==count
  assert path.with_name(decoded(r)['archived']).read_bytes()==raw

@pytest.mark.parametrize('case',['hash','moving','lock','serial'])
def test_archive_rejects_changed_target_or_movement_without_touching_params(case,device):
  path=Path(service.TUNING_STATE_PATH);path.write_text('{broken');raw=path.read_bytes()
  async def exercise():
    body={'serial':'test-device-A','journal_sha256':service.tuning_journal_digest()};app={}
    if case=='hash':body['journal_sha256']='0'*64
    if case=='serial':body['serial']='other-device'
    if case=='moving':device[3]['parked']=False
    if case=='lock':app['settings_write_lock']=asyncio.Lock();await app['settings_write_lock'].acquire()
    try:
      try:r=await routes.archive_tuning_route(Request(body,app));assert r.status==409
      except web.HTTPConflict:assert case=='moving'
    finally:
      if case=='lock':app['settings_write_lock'].release()
  asyncio.run(exercise());assert path.read_bytes()==raw and not device[1]

def test_legacy_registration_wrapper_uses_verified_maintenance_gate(device,monkeypatch):
  from openpilot.selfdrive.carrot.server.services import setting_safety
  state={'offroad_safe':True};calls=[]
  async def maintenance(req):
    calls.append('gate')
    if not state['offroad_safe']:raise web.HTTPConflict()
  async def handler(req):calls.append('write');return web.json_response({'ok':True})
  monkeypatch.setattr(setting_safety,'legacy_settings_maintenance_safe',maintenance)
  wrapped=setting_safety.serialize_settings_mutation(handler)
  assert asyncio.run(wrapped(Request({}))).status==200
  state['offroad_safe']=False
  with pytest.raises(web.HTTPConflict):asyncio.run(wrapped(Request({})))
  assert calls==['gate','write','gate']

def test_live_progress_is_not_recovery_and_orphan_is(device):
  assert apply(PROFILES[0]).status==200
  row=service._tuning_state();row['phase']='applying';row['server_epoch']=service._TUNING_SERVER_EPOCH;service.save_tuning_state(row)
  request=Request({}, {'tuning_active_request':row['request_id']})
  status=routes._tuning_progress(request,service.tuning_receipt())
  assert status['in_progress'] and not status['recovery_required']
  status=routes._tuning_progress(Request({}),service.tuning_receipt())
  assert not status['in_progress'] and status['recovery_required']


def test_cancellation_during_terminal_commit_never_undoes_verified_values(device,monkeypatch):
  entered=threading.Event();release=threading.Event();real=service.save_tuning_state
  def save(row):
    real(row)
    if row['phase']=='complete':entered.set();assert release.wait(3)
  monkeypatch.setattr(service,'save_tuning_state',save)
  async def exercise():
    p=PROFILES[0];v=service.preview_tuning_profile(p)
    body={'profile':p,'profile_hash':v['profile_hash'],'revision':v['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex}
    task=asyncio.create_task(routes.apply_tuning_route(Request(body)))
    while not entered.is_set():await asyncio.sleep(.001)
    task.cancel();await asyncio.sleep(.01);assert not task.done();release.set()
    response=await task;assert response.status==200 and decoded(response)['ok']
    assert service._tuning_state()['phase']=='complete' and len(device[1])==3
  try:asyncio.run(exercise())
  finally:release.set()

def test_profile_hash_cannot_change_between_preview_and_apply(device):
  p=deepcopy(PROFILES[0]);v=service.preview_tuning_profile(p);p['values']['ApproachCoastStrength']+=1
  response=asyncio.run(routes.apply_tuning_route(Request({'profile':p,'profile_hash':v['profile_hash'],'revision':v['revision'],'serial':'test-device-A','request_id':uuid.uuid4().hex})))
  assert response.status==409 and not device[1]


def test_forward_ota_profile_gate_requires_exact_ci_and_both_client_fixtures():
  import ast,hashlib,io
  from types import SimpleNamespace as NS
  path=ROOT/'tools/publish_hud_release.py';tree=ast.parse(path.read_text(encoding='utf8'));fixture=(ROOT/'selfdrive/carrot/tests/tuning_profiles_v1.json').read_bytes()
  source='a'*40;client='b'*40;fault={'mode':''}
  def public_fixture():
    if fault['mode']!='contract_changed':return fixture
    d=json.loads(fixture);d['contract']['fields']['TFollowGap1']['unit']='seconds'
    for p in d['profiles']:p['units']['TFollowGap1']='seconds'
    return json.dumps(d).encode()
  def git(*args):return source.encode() if args[0]=='rev-parse' else public_fixture()
  def command(args,**kwargs):
    url=args[-1]
    if '/git/ref/' in url:return json.dumps({'object':{'sha':client}})
    names=['windows_profiles','build'] if 'carrot-hud' in url else ['profile-contract (3.11)','profile-contract (3.12)']
    sha=client if 'carrot-hud' in url else source
    checks=[{'id':i,'name':n,'head_sha':sha,'status':'completed','conclusion':'success','html_url':'https://example.invalid/ci'} for i,n in enumerate(names,1)]
    if fault['mode']=='wrong_sha':checks[0]['head_sha']='c'*40
    if fault['mode']=='failed_latest':checks.append({**checks[0],'id':100,'conclusion':'failure'})
    return json.dumps([{'check_runs':checks}])
  def opened(req,**kwargs):return io.BytesIO(b'wrong' if fault['mode']=='client_bytes' else public_fixture())
  scope={'TUNING_V1_CONTRACT_SHA256':'4a0c60f6f9e5a97d05fbee157b3d0cb25fa6101c6e7d603ecc6ff43f09e0d468','git':git,'json':json,'subprocess':NS(check_output=command),'Request':lambda url,**kw:url,'urlopen':opened,'core':NS(sha=lambda v:hashlib.sha256(v).hexdigest())}
  nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('profile_publication_gate','_verified_profile_checks')]
  exec(compile(ast.Module(body=nodes,type_ignores=[]),str(path),'exec'),scope)
  proof=scope['profile_publication_gate'](source)
  assert proof['source_commit']==source and proof['client_commit']==client and len(proof['client_checks'])==2
  for mode in ('wrong_sha','failed_latest','client_bytes','contract_changed'):
    fault['mode']=mode
    with pytest.raises(ValueError):scope['profile_publication_gate'](source)


def test_current_unattempted_recovery_cannot_be_archived(device):
  assert apply(PROFILES[0]).status==200
  row=service._tuning_state();row['phase']='applying';service.save_tuning_state(row)
  with pytest.raises(ValueError):service.archive_tuning_journal(service.tuning_journal_digest(),'test-device-A')
  row['phase']='recovery_required';service.save_tuning_state(row)
  with pytest.raises(ValueError):service.archive_tuning_journal(service.tuning_journal_digest(),'test-device-A')
  row['error']='restore failed';service.save_tuning_state(row)
  service.archive_tuning_journal(service.tuning_journal_digest(),'test-device-A')
  assert service.tuning_receipt()['unverified_previous'] and service.tuning_snapshot()['unverified_previous']
  # A subset cannot clear uncertainty about the rest of the original transaction.
  subset=deepcopy(PROFILES[0]);subset['values']={'MyDrivingMode':3};subset['units']={'MyDrivingMode':service.TUNING_UNITS['MyDrivingMode']}
  assert apply(subset).status==200 and service.tuning_receipt()['unverified_previous']
  assert apply(PROFILES[0]).status==200
  assert not service.tuning_receipt()['unverified_previous']
