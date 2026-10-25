"""Signed original-only feature transaction, with S0/D1 and motor haptics intact.

This is NOT HUD source OTA. The native registry and append-only schema require
their own stopped transaction. Roll this layer back BEFORE S0/D1 recovery.
No CAN transmit, firmware flashing, live controller or monitoring bypass.
"""
from pathlib import Path
import argparse,base64,hashlib,importlib.util,json,os,subprocess,sys,time
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

RID='lx3-original-feature-parity-20261025-03'
SERIAL='80acfe79'
PUBLIC_SHA='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
CORE_SHA='d08e4062811ec1099cd3afc9b84bda2174f712a72f3ef5db0a0a6dc0d7ba6a03'
POLICY_SHA='61b30bb7d8924023d2d88f029f4726d37099c360b407f6fc8e934e92c8e18d15'
PARAMS_BEFORE='8aa96c0a392bc9f10d640164ae9262b149851fe1976c50ab3569244a14beeb3b'
SCHEMA_LINK='../opendbc_repo/opendbc/car/car.capnp'
BOOL_KEYS={'ApproachComfortEnabled','ApproachFlexibleGap','C3xLiteBuzzerMute','CruiseStartCurrentSpeed'}
INT_KEYS={'ApproachCoastStrength','ApproachBrakeMargin','ApproachStopSmooth','ApproachTrafficCalm','ApproachFlexibleGapMin'}
JSON_PREF_KEYS={'LaunchFollowStrength':20,'YellowLineBiasCm':10,'NearbyVehicleBiasCm':10,'LateralMarginShadow':1}
sha=lambda raw:hashlib.sha256(raw).hexdigest()
LEARNED_KEYS={'CalibrationParams','LiveTorqueParameters'}


def freeze_stopped_settings(params_dir,record,writers_stopped):
  """Retain this device's latest learned values after their writers have exited.

  These caches update periodically even while parked; a staging-time hash is
  not a stable preference. Never replace their bytes, and never relax static
  identity, monitoring, recording or haptic preference checks.
  """
  assert writers_stopped,'Learned parameter writers still running'
  staged=record['protected_settings']
  current={}
  for key,expected in staged.items():
    path=params_dir/key
    assert path.is_file() and not path.is_symlink(),key
    current[key]=sha(path.read_bytes())
    assert key in LEARNED_KEYS or current[key]==expected,'Foreign protected preference change: '+key
  record['protected_settings_at_stage']=dict(staged)
  record['learned_updates_before_writes']={k:{'stage':staged[k],'stopped':current[k]}
    for k in LEARNED_KEYS&staged.keys() if staged[k]!=current[k]}
  record['protected_settings']=current


def learned_writers_stopped(proc=Path('/proc')):
  for path in proc.iterdir():
    if not path.name.isdigit():continue
    try:
      comm=(path/'comm').read_text().strip();args=(path/'cmdline').read_bytes()
      if comm in ('torqued','calibrationd') or any(n in args for n in (b'locationd.torqued',b'locationd.calibrationd')):return False
    except (FileNotFoundError,ProcessLookupError):continue
    except PermissionError:return False
  return True

def support(folder):
  core_file=folder/'haptic_core.py';policy_file=folder/'feature-policy.json'
  assert sha(core_file.read_bytes())==CORE_SHA and sha(policy_file.read_bytes())==POLICY_SHA
  policy=json.loads(policy_file.read_bytes())
  spec=importlib.util.spec_from_file_location('feature_transaction_core',core_file)
  core=importlib.util.module_from_spec(spec);spec.loader.exec_module(core)
  prerequisites=['common/params_keys.h','common/params_pyx.so','opendbc_repo/opendbc/car/car.capnp',
    'selfdrive/controls/lib/approach_comfort.py','selfdrive/controls/lib/flexible_gap.py','selfdrive/controls/lib/notification_preferences.py',
    'opendbc_repo/opendbc/car/hyundai/manual_steering.py','selfdrive/carrot/manual_steering.py','opendbc_repo/opendbc/car/interfaces.py']
  core.ORDER=prerequisites+[n for n in policy['files'] if n not in prerequisites]
  core.PROTECTED=policy['protected']
  return core,policy

def schema_link(root):
  path=root/'cereal/car.capnp'
  assert path.is_symlink() and os.readlink(path)==SCHEMA_LINK,'Schema alias changed'

def migration(folder,manifest):
  raw=(folder/'settings-migration.json').read_bytes()
  assert len(raw)<8192 and sha(raw)==manifest['settings_migration_sha256']
  m=json.loads(raw);assert m['schema']==1 and m['target_serial']==SERIAL
  assert set(m)=={'schema','target_serial','params','driving_preferences'}
  assert set(m['params'])==BOOL_KEYS|INT_KEYS
  for k,v in m['params'].items():
    if k in BOOL_KEYS:assert type(v) is bool
    else:assert type(v) is int and (50 if k=='ApproachFlexibleGapMin' else 0)<=v<=100
  assert set(m['driving_preferences'])==set(JSON_PREF_KEYS)
  assert all(type(v) is int and 0<=v<=JSON_PREF_KEYS[k] for k,v in m['driving_preferences'].items())
  return m

def verify(root,folder,core,policy,config=Path('/data/community/hud_updates/config.json')):
  public=base64.b64decode(json.loads(config.read_text())['public_key'],validate=True)
  assert sha(public)==PUBLIC_SHA
  raw=(folder/'package.signed.json').read_bytes();assert len(raw)<20_000_000
  envelope=json.loads(raw);payload=base64.b64decode(envelope['payload'],validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),payload)
  m=json.loads(payload)
  assert m['release_id']==RID and m['target_serial']==SERIAL
  assert m['installer_sha256']==sha(Path(__file__).read_bytes())
  rows=m['files'];assert len(rows)==len(core.ORDER) and {v['path'] for v in rows}==set(core.ORDER)
  files={v['path']:v for v in rows}
  for name,row in files.items():
    core.checked(root,name);data=base64.b64decode(row['data'],validate=True)
    assert sha(data)==row['sha256'] and len(data)==row['bytes']
    expected=policy['files'].get(name)
    assert row['before']==(PARAMS_BEFORE if name=='common/params_pyx.so' else expected['before_sha256'])
    if expected:assert row['sha256']==expected['after_sha256']
    if name.endswith('.py'):compile(data,name,'exec')
  native=m['native_build']
  assert native['key_count']==333 and native['original_324_attributes_preserved']
  assert native['old_flags_retention_equivalent'] and native['new_keys_survive_manager_clear']
  assert native['ABI_subset_of_original_backup'] and native['machine']=='aarch64'
  assert native['module_sha256']==files['common/params_pyx.so']['sha256']
  assert native['registry_header_sha256']==files['common/params_keys.h']['sha256']
  elf=base64.b64decode(files['common/params_pyx.so']['data'],validate=True)
  assert len(elf)>64 and elf[:6]==b'\x7fELF\x02\x01' and int.from_bytes(elf[18:20],'little')==183
  assert set(m['settings_before_sha256'])==BOOL_KEYS|INT_KEYS|{'driving_preferences.json'}
  migration(folder,m)
  return m,files,sha(raw)

def settings_paths(params_dir,community):
  return {**{k:params_dir/k for k in sorted(BOOL_KEYS|INT_KEYS)},'driving_preferences.json':community/'driving_preferences.json'}

def after_preferences(m):
  out={k:('1' if v is True else '0' if v is False else str(v)).encode() for k,v in m['params'].items()}
  out['driving_preferences.json']=(json.dumps({'schema':1,'values':m['driving_preferences']},separators=(',',':'))+'\n').encode()
  return out

def validate_settings_restore(record,paths):
  for key,row in record['settings'].items():
    p=paths[key];current=sha(p.read_bytes()) if p.is_file() else None
    assert current in (row['before_sha256'],row['after_sha256']),'Foreign preference change: '+key
    old=base64.b64decode(row['before']) if row['before'] is not None else None
    assert (sha(old) if old is not None else None)==row['before_sha256']

def restore_settings(core,record,paths):
  validate_settings_restore(record,paths)
  for key,row in record['settings'].items():
    p=paths[key]
    if row['before'] is None:p.unlink(missing_ok=True)
    else:core.atomic(p,base64.b64decode(row['before']),0o600)

def validate_record(manifest,files,record,bundle_sha,core):
  assert record['release_id']==RID and record['bundle_sha256']==bundle_sha
  assert record['source_commit']==manifest['source_commit'] and record['schema_link']==SCHEMA_LINK
  assert set(record['files'])==set(core.ORDER)
  for name,row in record['files'].items():
    assert all(row[k]==files[name][k] for k in ('before','sha256','bytes'))
    assert type(row['mode']) is int and 0<=row['mode']<=0o777
  assert set(record['settings'])==set(manifest['settings_before_sha256'])
  proposed=after_preferences(migration(Path(core._feature_folder),manifest))
  for name,row in record['settings'].items():
    assert row['before_sha256']==manifest['settings_before_sha256'][name]
    assert row['after_sha256']==sha(proposed[name])

def make_smoke_view(root,overlay,dest,order):
  assert not dest.exists();dest.mkdir(parents=True)
  prefixes={str(Path(n).parent) for n in order}|{'cereal'}
  def needed(rel):return rel=='.' or any(p==rel or p.startswith(rel+'/') for p in prefixes)
  def visit(rel):
    source=root/rel;target=dest/rel
    if rel in order:
      target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes((overlay/rel).read_bytes());return
    if source.is_dir() and needed(rel):
      target.mkdir(exist_ok=True)
      children={p.name for p in source.iterdir()}|{Path(n).relative_to(rel).parts[0] for n in order if rel=='.' or n.startswith(rel+'/')}
      for name in sorted(children):visit(name if rel=='.' else rel+'/'+name)
    elif source.is_symlink():
      target.symlink_to(dest if source.resolve()==root.resolve() else os.readlink(source),target_is_directory=source.is_dir())
    elif source.is_file() and source.suffix in ('.py','.capnp'):target.write_bytes(source.read_bytes())
    elif source.exists():target.symlink_to(source,target_is_directory=source.is_dir())
  visit('.')
  # The installed fork has a nested regular openpilot package. A namespace
  # candidate loses to that later regular package even with parent first.
  # Make the isolated view an explicit package; never edit the live shim.
  (dest/'__init__.py').write_text('"""Isolated feature qualification package."""\n')

def smoke_environment(parent,view):
  return {**os.environ,'PYTHONPATH':str(parent)+':'+str(view)+':'+str(view/'opendbc_repo')+':'+os.environ.get('PYTHONPATH',''),
    'PYTHONDONTWRITEBYTECODE':'1','PWD':str(parent)}

def import_smoke(root,folder,core):
  parent=folder/'smoke-v3';view=parent/'openpilot'
  if not view.exists():make_smoke_view(root,folder/'new',view,core.ORDER)
  program=r'''
from pathlib import Path
import sys,tempfile,json,inspect
from openpilot.common.params import Params
from opendbc.car import structs
from opendbc.car.hyundai import carcontroller,hyundaicanfd,lx3_haptic_notifications
from openpilot.selfdrive.car import card,cruise
from openpilot.selfdrive.controls import controlsd
from openpilot.selfdrive.controls.lib import longitudinal_planner,approach_comfort,flexible_gap
from openpilot.selfdrive.carrot.server.services import settings
import cereal,opendbc.car.structs,openpilot.common.params_pyx
view=Path(sys.argv[1]).resolve()
for m in (cereal,opendbc.car.structs,openpilot.common.params_pyx,carcontroller,hyundaicanfd,card,controlsd,cruise,longitudinal_planner,approach_comfort,flexible_gap):
 assert Path(m.__file__).resolve().is_relative_to(view),(m.__name__,m.__file__)
assert (view/'cereal/car.capnp').resolve()==(view/'opendbc_repo/opendbc/car/car.capnp').resolve()
probe=cereal.car.CarControl.new_message();probe.manualSteeringResume=True
with tempfile.TemporaryDirectory(prefix='feature-smoke-') as temp:
 p=Params(temp);assert len(p.all_keys())==333
 p.put_int('DisableDM',1);assert p.get_int('DisableDM')==1
 p.put_bool('CruiseStartCurrentSpeed',True);assert p.get_bool('CruiseStartCurrentSpeed')
 p.put_bool('ApproachComfortEnabled',True);assert p.get_bool('ApproachComfortEnabled')
 p.put_int('ApproachTrafficCalm',30);assert p.get_int('ApproachTrafficCalm')==30
 p.put_int('C3xLiteHapticPattern',2)
 h=lx3_haptic_notifications.HapticNotifications();h.configure_from_params(p);assert h.pattern==2
 desired=json.loads(Path(sys.argv[2]).read_text())['driving_preferences']
 path=Path(temp)/'driving_preferences.json';path.write_text(json.dumps({'schema':1,'values':desired}))
 preferences=approach_comfort.DrivingPreferences(path);assert preferences.read()==desired and preferences.error is None
c=structs.CarControl.new_message();c.manualSteeringResume=True;c.manualSteeringScale=.5;c.lx3Authority.epoch=42
with structs.CarControl.from_bytes(c.to_bytes()) as d:assert d.manualSteeringResume and d.lx3Authority.epoch==42
assert 'display_context' in inspect.signature(__import__('opendbc.car.interfaces',fromlist=['CarInterfaceBase']).CarInterfaceBase.apply).parameters
print(json.dumps({'key_count':333,'new_schema_roundtrip':True,'current_speed_and_planner_imports':True,
 'haptic_configuration_retained':True,'display_context_dependency':True,'driving_preferences_schema_valid':True,'no_CAN_publisher_instantiated':True}))
'''
  env=smoke_environment(parent,view)
  result=subprocess.run([sys.executable,'-c',program,str(view),str(folder/'settings-migration.json')],cwd=parent,env=env,capture_output=True,text=True,timeout=60)
  core.atomic(folder/'import-smoke.stdout',result.stdout.encode());core.atomic(folder/'import-smoke.stderr',result.stderr.encode())
  assert result.returncode==0,result.stderr[-3000:]
  report=json.loads(result.stdout.splitlines()[-1]);core.save(folder/'import-smoke.json',report);return report

def main():
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('phase',choices=['stage','install','rollback']);p.add_argument('--folder',type=Path,required=True)
  args=p.parse_args();root=Path('/data/openpilot');folder=args.folder
  assert root.resolve()==root and folder==Path('/data/lx3_separate_installs')/RID and folder.resolve()==folder
  import fcntl
  lock=(folder/'installer.lock').open('a+b');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  assert json.loads(Path('/data/community/hud_updates/state.json').read_text())['phase']=='complete'
  core,policy=support(folder);core._feature_folder=str(folder)
  from openpilot.system.hardware import HARDWARE
  assert HARDWARE.get_serial()==SERIAL and __import__('platform').machine()=='aarch64' and sys.version_info[:2]==(3,12)
  m,files,bundle=verify(root,folder,core,policy);schema_link(root)
  params_dir=Path('/data/params/d');community=Path('/data/community');paths=settings_paths(params_dir,community)
  assert params_dir.resolve().is_relative_to(Path('/data/params'))
  assert all(not path.is_symlink() for path in paths.values())
  record_file=folder/'transaction.json'
  if args.phase=='stage':
    assert not record_file.exists();core.before(root,files)
    proposed=after_preferences(migration(folder,m));settings={}
    for k,path in paths.items():
      old=path.read_bytes() if path.is_file() else None
      settings[k]={'before':base64.b64encode(old).decode() if old is not None else None,'before_sha256':sha(old) if old is not None else None,
        'after_sha256':sha(proposed[k])}
      assert settings[k]['before_sha256']==m['settings_before_sha256'][k]
    protected_settings={}
    for k in ('DisableDM','HardwareC3xLite','CalibrationParams','LiveTorqueParameters','DongleId','CarSelected3','RecordRoadCam',
              'CanfdDebug','PandaSignatures','C3xLiteHapticNotifications','C3xLiteHapticEngage','C3xLiteHapticTraffic',
              'C3xLiteHapticLaneChange','C3xLiteHapticStopping','C3xLiteHapticNavigation','C3xLiteHapticReverse','C3xLiteHapticPattern'):
      q=params_dir/k
      if q.is_file():protected_settings[k]=sha(q.read_bytes())
    record={'phase':'staging','release_id':RID,'source_commit':m['source_commit'],'bundle_sha256':bundle,
      'boot_before':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'schema_link':SCHEMA_LINK,
      'files':{},'settings':settings,'protected_settings':protected_settings,'runtime_modified':False,
      'settings_attempted':False,'preferences':{}}
    core.save(record_file,record)
    for name in core.ORDER:
      row=files[name];q=core.checked(root,name);mode=q.stat().st_mode&0o777 if q.exists() else 0o644
      if row['before'] is not None:core.atomic(folder/'before'/name,q.read_bytes(),mode)
      core.atomic(folder/'new'/name,base64.b64decode(row['data']),mode)
      record['files'][name]={k:row[k] for k in ('before','sha256','bytes')};record['files'][name]['mode']=mode
    record['smoke']=import_smoke(root,folder,core);record['phase']='staged';core.save(record_file,record)
  else:
    record=json.loads(record_file.read_text());validate_record(m,files,record,bundle,core)
    if args.phase=='install':
      assert record['phase']=='staged';core.before(root,files)
      assert all(sha((folder/'new'/n).read_bytes())==files[n]['sha256'] for n in core.ORDER)
      assert all((sha(paths[k].read_bytes()) if paths[k].is_file() else None)==v['before_sha256'] for k,v in record['settings'].items())
    else:
      assert record['phase'] in ('complete','installed_pending_reboot_verification','recovery_required')
      core.validate_restore(root,folder,record);validate_settings_restore(record,paths)
    prior_phase=record['phase'];record['gate']=core.parked();record['phase']='stopping';core.save(record_file,record)
    changed=False
    try:
      subprocess.run(['sudo','-n','systemctl','stop','comma.service'],check=True)
      core.stopped();schema_link(root)
      assert json.loads(Path('/data/community/hud_updates/state.json').read_text())['phase']=='complete'
      if args.phase=='rollback':
        core.validate_restore(root,folder,record);validate_settings_restore(record,paths);changed=True
        core.restore(root,folder,record,restore_preferences=False);restore_settings(core,record,paths)
        core.before(root,files);schema_link(root);record.update(phase='restored_feature_layer_before_S0',runtime_modified=False)
      else:
        core.before(root,files)
        freeze_stopped_settings(params_dir,record,learned_writers_stopped())
        core.save(record_file,record)
        changed=True;record['phase']='installing';core.save(record_file,record)
        for n in core.ORDER:
          core.atomic(core.checked(root,n),(folder/'new'/n).read_bytes(),record['files'][n]['mode']);record['last_file']=n;core.save(record_file,record)
        assert all(sha(core.checked(root,n).read_bytes())==files[n]['sha256'] for n in core.ORDER)
        assert all(sha((root/n).read_bytes())==h for n,h in core.PROTECTED.items());schema_link(root)
        assert all(sha((params_dir/k).read_bytes())==h for k,h in record['protected_settings'].items())
        record['settings_attempted']=True;core.save(record_file,record)
        for k,raw in after_preferences(migration(folder,m)).items():core.atomic(paths[k],raw,0o600)
        command="import sys,json;from openpilot.common.params import Params;from openpilot.selfdrive.controls.lib.approach_comfort import DrivingPreferences;from cereal import car;p=Params();assert len(p.all_keys())==333 and p.get_int('DisableDM')==1;assert all(p.get(k)==v for k,v in json.loads(sys.argv[1]).items());prefs=DrivingPreferences();assert prefs.read()==json.loads(sys.argv[2]) and prefs.error is None;c=car.CarControl.new_message();c.manualSteeringResume=True"
        expected=migration(folder,m)
        subprocess.run([sys.executable,'-c',command,json.dumps(expected['params']),json.dumps(expected['driving_preferences'])],cwd=root,check=True,timeout=10)
        assert time.monotonic()-record['gate']['mono']<15,'Fresh stop window exceeded'
        assert learned_writers_stopped()
        assert all(sha((params_dir/k).read_bytes())==h for k,h in record['protected_settings'].items())
        record['pre_reboot_protected_settings']={'hashes':dict(record['protected_settings']),
          'boot':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
          'boot_seconds':time.clock_gettime(time.CLOCK_BOOTTIME)}
        record.update(phase='installed_pending_reboot_verification',runtime_modified=True,source_layer_before_rollback='S0/D1')
      core.save(record_file,record)
    except BaseException:
      if changed and args.phase=='install':
        try:
          core.restore(root,folder,record,restore_preferences=False)
          if record['settings_attempted']:restore_settings(core,record,paths)
          core.before(root,files);schema_link(root);record.update(phase='restored_after_failure',runtime_modified=False);core.save(record_file,record)
        except BaseException:
          record['phase']='recovery_required';core.save(record_file,record);raise
      elif changed:
        record['phase']='recovery_required';core.save(record_file,record);raise
      else:
        record['phase']=prior_phase;record['last_attempt']='aborted_before_writes';core.save(record_file,record)
      subprocess.run(['sudo','-n','systemctl','start','comma.service'],check=True);raise
    subprocess.Popen(['sudo','-n','sh','-c','sleep 3; reboot'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
  print('RESULT_JSON='+json.dumps({k:v for k,v in record.items() if k not in ('settings','protected_settings')}),flush=True)

if __name__=='__main__':main()
