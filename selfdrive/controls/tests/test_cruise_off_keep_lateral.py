"""Production MAIN handlers + reconciliation/intent + unmodified native C.

No vehicle/USB/actuator/whole selfdrived loop is simulated. Host delivery is
delayed by0..120ms after native RX; authority is never assigned by tests.
"""
import ast,copy,ctypes,importlib.util,json,os,subprocess,sys
from pathlib import Path
from types import SimpleNamespace as NS
import pytest
ROOT=Path(__file__).resolve().parents[3]

def load(name,path):
  spec=importlib.util.spec_from_file_location(name,ROOT/path);m=importlib.util.module_from_spec(spec);sys.modules[name]=m;spec.loader.exec_module(m);return m

prefs=load('button_preference_source','selfdrive/controls/lib/approach_comfort.py')
authority=load('main_keep_authority','selfdrive/car/lx3_authority.py')
BT=NS(mainCruise='mainCruise',lfaButton='lfaButton',cancel='cancel')

def cruise():
  tree=ast.parse((ROOT/'selfdrive/car/cruise.py').read_text(encoding='utf8'))
  names={'_handle_lx3_main_release','_update_lx3_main_latch'}
  methods=[n for n in ast.walk(tree) if isinstance(n,ast.FunctionDef) and n.name in names]
  cls=ast.ClassDef(name='Cruise',bases=[],keywords=[],body=methods,decorator_list=[])
  scope={'verified':authority.verified,'STATUS_MAX_NS':authority.STATUS_MAX_NS,'ButtonType':BT}
  exec(compile(ast.fix_missing_locations(ast.Module(body=[cls],type_ignores=[])),'production_MAIN_methods','exec'),scope)
  c=scope['Cruise']();c._keep_lateral_on_cruise_off=True;c._lat_enabled=False;c.lx3_main_keep_lateral=None
  c.lx3_lat_key=c.lx3_long_key=c.lx3_lat_time=c.lx3_long_time=0;c.lx3_lat_was_allowed=False;c.lx3_lateral_refused=False
  c.lx3_auto_until=0;c._cruise_cancel_state=False;c.params=NS(get_bool=lambda _:True);c._add_log=lambda _:None
  return c

def test_option_is_off_by_default_and_separate_from_rollback_driving_store(tmp_path):
  driving=tmp_path/'driving.json';raw=b'{"schema":1,"values":{"LaunchFollowStrength":10}}';driving.write_bytes(raw)
  store=prefs.CruiseButtonPreferences(tmp_path/'buttons.json')
  assert store.read()=={'CruiseOffKeepLateral':0}
  assert store.set('CruiseOffKeepLateral',1)==1 and store.read()['CruiseOffKeepLateral']==1
  assert driving.read_bytes()==raw and prefs.DrivingPreferences(driving).read()['LaunchFollowStrength']==10
  assert not list(tmp_path.glob('*.tmp'))

@pytest.mark.parametrize('raw',['null','[]','{"schema":2,"values":{}}','{"schema":1,"values":{"CruiseOffKeepLateral":true}}',
  '{"schema":1,"values":{"CruiseOffKeepLateral":2}}','{"schema":1,"values":{"Unknown":1}}'])
def test_invalid_option_falls_off_without_rewriting_file(tmp_path,raw):
  path=tmp_path/'buttons.json';path.write_text(raw);store=prefs.CruiseButtonPreferences(path)
  assert store.read()=={'CruiseOffKeepLateral':0} and path.read_text()==raw

@pytest.mark.skipif(os.name=='nt',reason='Actual Linux openpilot package path')
def test_setting_get_set_bulk_and_development_menu_use_separate_store(tmp_path,monkeypatch):
  # Real services with no native Params module available must route this key
  # before every native access, just like the existing JSON preferences.
  from openpilot.selfdrive.carrot.server.services import params,settings
  store=prefs.CruiseButtonPreferences(tmp_path/'buttons.json')
  monkeypatch.setattr(params,'cruise_button_preferences',store)
  monkeypatch.setattr(params,'HAS_PARAMS',False)
  params.set_param_value('CruiseOffKeepLateral',1)
  assert params.get_param_value('CruiseOffKeepLateral',0)==1
  assert params.get_param_values(['CruiseOffKeepLateral'])=={'CruiseOffKeepLateral':1}
  data=settings.inject_driving_preferences({'params':[],'menu':[]})
  entry=next(r for r in data['params'] if r['name']=='CruiseOffKeepLateral')
  assert entry['default']==0 and entry['min']==0 and entry['max']==1 and entry['virtual']
  assert data['menu'][0]['id']=='development_features'
  assert json.dumps(data['menu']).count('CruiseOffKeepLateral')==1

@pytest.fixture
def native(tmp_path):
  if os.name=='nt':pytest.skip('Linux actual native production C fixture')
  lib=tmp_path/'authority.so'
  subprocess.run(['cc','-shared','-fPIC','-std=c11','-Wall','-Wextra','-Werror',
    '-I'+str(ROOT/'opendbc_repo/opendbc/safety'),str(ROOT/'selfdrive/car/tests/lx3_main_keep_native.c'),'-o',str(lib)],check=True)
  n=ctypes.CDLL(str(lib));n.fixture_status.argtypes=[ctypes.POINTER(ctypes.c_uint64)]
  n.fixture_event.argtypes=[ctypes.c_int,ctypes.POINTER(ctypes.c_uint64)]
  n.fixture_reset(1);return n

class Host:
  def __init__(self,n,keep=True,delay=0):
    self.n=n;self.c=cruise();self.c._keep_lateral_on_cruise_off=keep
    self.available=False;self.delay=delay;self.events=[];self.frame=0;self.trace=[]
  def snapshot(self):
    out=(ctypes.c_uint64*16)();self.n.fixture_status(out)
    return NS(version=4,profile=1,epoch=out[0],allowed=out[1],armed=out[2],lateralGeneration=out[3],longitudinalGeneration=out[4],
      pendingKey=out[5],pendingGeneration=out[6],pendingAgeMs=out[7],pendingAxes=out[8],longPendingKey=out[9],longPendingGeneration=out[10],
      longPendingAgeMs=out[11],longPendingAxes=out[12],reason=out[13],longitudinalRevision=out[14],inputReady=bool(out[15]),
      heartbeatAgeMs=0,buttonHealthy=bool(out[15]),config=1,statusMonoTime=self.n.fixture_now()*1000,oemEmergency=False,
      lateralDecisionKey=self.c.lx3_lat_key,lateralDecisionTime=self.c.lx3_lat_time,
      longitudinalDecisionKey=self.c.lx3_long_key,longitudinalDecisionTime=self.c.lx3_long_time,
      autoResume=False,refuseLong=False,refuseAfterSequence=0,lateralRefused=False)
  def step(self,key=0,fault=False):
    self.frame+=1;count=self.n.fixture_frame(key);now=self.n.fixture_now()*1000
    if fault:self.n.fixture_fault()
    for i in range(count):
      event=(ctypes.c_uint64*3)();self.n.fixture_event(i,event)
      b=NS(type={8:BT.mainCruise,128:BT.lfaButton,4:BT.cancel}.get(event[0],'other'),pressed=bool(event[1]),
        physical=True,physicalKey=(event[0]<<8)|event[2],observedMonoTime=now)
      self.events.append((self.frame+self.delay,b))
    incoming=[b for frame,b in self.events if frame<=self.frame];self.events=[r for r in self.events if r[0]>self.frame]
    a=self.snapshot();cs=NS(lx3Authority=a,buttonEvents=incoming,gearShifter='drive',canValid=True,canTimeout=False,
      steerFaultTemporary=fault,steerFaultPermanent=False,brakePressed=False,gasPressed=False)
    self.c._update_lx3_main_latch(cs,now);authority.reconcile_lateral(self.c,cs,True,now)
    for b in incoming:
      if b.type==BT.mainCruise and not b.pressed:
        self.available=not self.available;cs.cruiseState=NS(available=self.available);self.c._handle_lx3_main_release(cs,b)
    a.lateralDecisionKey,a.lateralDecisionTime=self.c.lx3_lat_key,self.c.lx3_lat_time
    a.longitudinalDecisionKey,a.longitudinalDecisionTime=self.c.lx3_long_key,self.c.lx3_long_time
    cs.latEnabled=self.c._lat_enabled
    cc=NS()
    lat,long=authority.configure_control(cc,cs,[],self.available,True,True,True,now)
    self.n.fixture_heartbeat(cc.lx3Authority.intent,cc.lx3Authority.pendingGeneration,cc.lx3Authority.decisionKey)
    state=self.snapshot();self.trace.append({'available':self.available,'lat_intent':cs.latEnabled,'allowed':state.allowed,
      'intent':cc.lx3Authority.intent,'lat':lat,'long':long,'refused':self.c.lx3_lateral_refused,'generation':state.lateralGeneration})
    return self.trace[-1]
  def main(self):
    self.step(8)
    for _ in range(12+self.delay):self.step(0)
  def engage(self):
    for _ in range(3):self.step()
    self.main();assert self.snapshot().allowed==3 and self.available

@pytest.mark.parametrize('delay',[0,1,2,3])
def test_physical_main_off_keeps_only_lateral_across_status_before_host_release(native,delay):
  h=Host(native,delay=delay);h.engage();old=h.snapshot().lateralGeneration;h.main()
  a=h.snapshot();assert a.allowed==1 and a.armed==1 and not h.available and a.lateralGeneration>old
  assert h.c._cruise_cancel_state and h.c.lx3_auto_until==0
  assert native.fixture_identity(old)==0 and native.fixture_identity(a.lateralGeneration)==1
  assert not any(r['refused'] for r in h.trace)
  assert all(not r['intent']&2 and not r['allowed']&2 for r in h.trace if not r['available'] and r['lat_intent'])
  for _ in range(25):h.step()
  assert h.snapshot().allowed==1  # Left-over LONG pending expiring cannot enable it.
  h.main();assert h.available and h.snapshot().allowed==3

@pytest.mark.parametrize('delay',[0,1,3])
def test_option_off_keeps_legacy_main_off_all_axes(native,delay):
  h=Host(native,keep=False,delay=delay);h.engage();h.main()
  assert not h.c._lat_enabled and h.snapshot().allowed==0 and not h.available

def test_fault_during_main_hold_cannot_be_resurrected_by_release(native):
  h=Host(native,delay=2);h.engage();h.step(8);h.step(0,fault=True)
  for _ in range(15):h.step()
  assert not h.c._lat_enabled and h.snapshot().allowed==0

def test_pending_or_refused_intent_is_not_an_accepted_lateral_to_keep(native):
  h=Host(native);h.c._lat_enabled=True;h.available=True
  for _ in range(3):h.step()
  h.main();assert not h.c._lat_enabled and h.snapshot().allowed==0

@pytest.mark.parametrize('change',['stale','timeout','switched_off','epoch','observed_other_revoke','other_button'])
def test_latched_request_does_not_survive_aborted_main_attempt(native,change):
  h=Host(native);h.engage();h.step(8);c=h.c
  assert c.lx3_main_keep_lateral is not None
  a=h.snapshot();now=native.fixture_now()*1000
  cs=NS(lx3Authority=a,buttonEvents=[],gearShifter='drive',canValid=True,canTimeout=False,
        steerFaultTemporary=False,steerFaultPermanent=False,brakePressed=False,gasPressed=False)
  if change=='stale':a.statusMonoTime=now-authority.STATUS_MAX_NS-1
  elif change=='timeout':now=c.lx3_main_keep_lateral[2]+2_000_000_000;a.statusMonoTime=now
  elif change=='switched_off':c._keep_lateral_on_cruise_off=False
  elif change=='epoch':a.epoch+=1
  elif change=='observed_other_revoke':a.allowed=0;a.lateralGeneration=a.longitudinalGeneration=0;a.reason=2
  else:cs.buttonEvents=[NS(physical=True,type=BT.lfaButton,pressed=True)]
  c._update_lx3_main_latch(cs,now);assert c.lx3_main_keep_lateral is None
  cs.cruiseState=NS(available=False)
  c._handle_lx3_main_release(cs,NS(physicalKey=8<<8,observedMonoTime=now))
  assert not c._lat_enabled
