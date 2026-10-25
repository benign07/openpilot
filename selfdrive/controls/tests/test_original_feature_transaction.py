"""Offline signature/migration rejection and layered source recovery."""
from pathlib import Path
import base64,hashlib,importlib.util,json,os,subprocess
import sys
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat

ROOT=Path(__file__).resolve().parents[3]
def load(name,path):
  spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m
deploy=load('feature_transaction',ROOT/'tools/install_original_lx3_feature_parity.py')
profile=load('feature_composer',ROOT/'tools/prepare_original_lx3_feature_parity.py')
issuer=load('feature_issuer',ROOT/'tools/issue_original_feature_recovery.py')

def fixture(tmp_path,monkeypatch):
  root=tmp_path/'root';root.mkdir();stage=tmp_path/'stage';stage.mkdir()
  (stage/'haptic_core.py').write_bytes((ROOT/'tools/install_original_lx3_haptics.py').read_bytes())
  policy_bytes=subprocess.check_output(['git','show','a78ede02:tools/original_lx3_feature_parity/profile.json'],cwd=ROOT)
  (stage/'feature-policy.json').write_bytes(policy_bytes)
  core,policy=deploy.support(stage)
  _,before,after=profile.sources()
  native=bytearray(80);native[:6]=b'\x7fELF\x02\x01';native[18:20]=(183).to_bytes(2,'little')
  after['common/params_pyx.so']=bytes(native)
  files=[{'path':n,'before':deploy.PARAMS_BEFORE if n=='common/params_pyx.so' else policy['files'][n]['before_sha256'],
    'data':base64.b64encode(after[n]).decode(),'bytes':len(after[n]),'sha256':deploy.sha(after[n])} for n in core.ORDER]
  prefs={'schema':1,'target_serial':deploy.SERIAL,'params':{k:True for k in deploy.BOOL_KEYS},
    'driving_preferences':{'LaunchFollowStrength':10,'YellowLineBiasCm':10,'NearbyVehicleBiasCm':10,'LateralMarginShadow':0}}
  prefs['params'].update({k:70 if k=='ApproachFlexibleGapMin' else 30 for k in deploy.INT_KEYS})
  (stage/'settings-migration.json').write_text(json.dumps(prefs))
  key=Ed25519PrivateKey.generate();pub=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
  monkeypatch.setattr(deploy,'PUBLIC_SHA',deploy.sha(pub));config=tmp_path/'config.json';config.write_text(json.dumps({'public_key':base64.b64encode(pub).decode()}))
  m={'release_id':deploy.RID,'target_serial':deploy.SERIAL,'source_commit':'fixture','installer_sha256':deploy.sha(Path(deploy.__file__).read_bytes()),
    'settings_migration_sha256':deploy.sha((stage/'settings-migration.json').read_bytes()),
    'settings_before_sha256':{k:None for k in deploy.BOOL_KEYS|deploy.INT_KEYS|{'driving_preferences.json'}},'files':files,
    'native_build':{'key_count':333,'original_324_attributes_preserved':True,'old_flags_retention_equivalent':True,
      'new_keys_survive_manager_clear':True,'ABI_subset_of_original_backup':True,'machine':'aarch64',
      'module_sha256':deploy.sha(bytes(native)),'registry_header_sha256':deploy.sha(after['common/params_keys.h'])}}
  def sign():
    raw=json.dumps(m).encode();(stage/'package.signed.json').write_text(json.dumps({'payload':base64.b64encode(raw).decode(),'signature':base64.b64encode(key.sign(raw)).decode()}))
  sign();return root,stage,core,policy,m,sign,config,prefs,before,after

@pytest.mark.parametrize('change',['serial','path','old_keys','flags','migration_tamper','migration_unknown','migration_range','schema','native_arch'])
def test_bad_signed_feature_package_or_private_migration_never_touches_source(tmp_path,monkeypatch,change):
  root,stage,core,policy,m,sign,config,prefs,_,_=fixture(tmp_path,monkeypatch)
  deploy.verify(root,stage,core,policy,config)
  if change=='serial':m['target_serial']='fb6d3f3f'
  elif change=='path':m['files'][0]['path']='../escape'
  elif change=='old_keys':m['native_build']['key_count']=326
  elif change=='flags':m['native_build']['old_flags_retention_equivalent']=False
  elif change=='migration_tamper':(stage/'settings-migration.json').write_bytes(b'{}')
  elif change in ('migration_unknown','migration_range'):
    if change=='migration_unknown':prefs['params']['DisableDM']=0
    else:prefs['driving_preferences']['NearbyVehicleBiasCm']=100
    (stage/'settings-migration.json').write_text(json.dumps(prefs));m['settings_migration_sha256']=deploy.sha((stage/'settings-migration.json').read_bytes())
  elif change=='schema':m['files'][2]['sha256']='0'*64
  elif change=='native_arch':m['native_build']['machine']='x86_64'
  sign()
  with pytest.raises(Exception):deploy.verify(root,stage,core,policy,config)
  assert not list(root.iterdir())

@pytest.mark.parametrize('prefix',[1,2,3,6,12,20,32,33])
@pytest.mark.skipif(os.name=='nt',reason='Directory fsync and actual schema symlink exercised on Linux')
def test_partial_feature_restore_restores_S0_protected_layer_first(tmp_path,monkeypatch,prefix):
  root,stage,core,policy,m,sign,config,prefs,before,after=fixture(tmp_path,monkeypatch)
  record={'files':{},'preferences':{}}
  old_extension=b'original 324 native extension fixture'
  for n in core.ORDER:
    old=old_extension if n=='common/params_pyx.so' else before.get(n)
    record['files'][n]={'before':deploy.sha(old) if old is not None else None,'sha256':deploy.sha(after[n]),'mode':0o644}
    if old is not None:core.atomic(root/n,old);core.atomic(stage/'before'/n,old)
  (root/'cereal').mkdir();(root/'cereal/car.capnp').symlink_to(deploy.SCHEMA_LINK);deploy.schema_link(root)
  immutable_S0={'selfdrive/car/card.py':record['files']['selfdrive/car/card.py']['before'],
    'cereal/car.capnp':record['files']['opendbc_repo/opendbc/car/car.capnp']['before'],
    'common/params_keys.h':record['files']['common/params_keys.h']['before'],
    'common/params_pyx.so':deploy.sha(old_extension)}
  for n in core.ORDER[:prefix]:core.atomic(root/n,after[n])
  assert any(deploy.sha((root/n).read_bytes())!=h for n,h in immutable_S0.items())
  core.restore(root,stage,record,restore_preferences=False);deploy.schema_link(root)
  assert all(deploy.sha((root/n).read_bytes())==h for n,h in immutable_S0.items())
  assert all((root/n).is_file()==(v['before'] is not None) for n,v in record['files'].items())

@pytest.mark.skipif(os.name=='nt',reason='Directory fsync exercised on Linux')
def test_migration_and_partial_preference_restore_do_not_touch_haptics_or_DM(tmp_path,monkeypatch):
  root,stage,core,policy,m,sign,config,prefs,_,_=fixture(tmp_path,monkeypatch)
  params=tmp_path/'params';params.mkdir();community=tmp_path/'community';community.mkdir()
  (params/'DisableDM').write_bytes(b'1');(params/'C3xLiteHapticPattern').write_bytes(b'2');(params/'RecordRoadCam').write_bytes(b'1')
  paths=deploy.settings_paths(params,community);values=deploy.after_preferences(prefs)
  record={'settings':{k:{'before':None,'before_sha256':None,'after_sha256':deploy.sha(v)} for k,v in values.items()}}
  for k in list(values)[:4]:core.atomic(paths[k],values[k])
  deploy.restore_settings(core,record,paths)
  assert all(not p.exists() for p in paths.values())
  assert (params/'DisableDM').read_bytes()==b'1' and (params/'C3xLiteHapticPattern').read_bytes()==b'2' and (params/'RecordRoadCam').read_bytes()==b'1'

@pytest.mark.parametrize('change',['old_owner','old_device','roundtrip','serial'])
def test_recovery_authorization_requires_fresh_exact_owner_and_verified_boot_clock(change):
  owner={'confirmed_by':'owner','confirmed_at_pc_unix':1000.,'physical_condition':'P_zero_control_off','exact_message':'P, zero speed, control off'}
  device={'serial':'80acfe79','roundtrip_seconds':1.,'observed_at_pc_unix':1000.,'device_boot_s':50.,'device_unix':8000.,'boot_id':'a'}
  assert issuer.confirmation(owner,device,'f'*64,1001.)['expires_device_boot_s']==170.
  if change=='old_owner':owner['confirmed_at_pc_unix']=900.
  elif change=='old_device':device['observed_at_pc_unix']=950.
  elif change=='roundtrip':device['roundtrip_seconds']=11.
  else:device['serial']='fb6d3f3f'
  with pytest.raises(AssertionError):issuer.confirmation(owner,device,'f'*64,1001.)

@pytest.mark.skipif(os.name=='nt',reason='Actual source alias and POSIX module search path exercised on Linux')
@pytest.mark.parametrize('absolute_alias',[False,True])
def test_staged_namespace_cannot_fall_back_to_live_source_alias(tmp_path,absolute_alias):
  root=tmp_path/'live';root.mkdir();overlay=tmp_path/'new';overlay.mkdir()
  for folder in ('selfdrive','selfdrive/controls','selfdrive/controls/lib','cereal'):
    p=root/folder;p.mkdir(exist_ok=True);(p/'__init__.py').write_text('')
  (root/'openpilot').symlink_to(root if absolute_alias else '.',target_is_directory=True)
  name='selfdrive/controls/lib/approach_comfort.py';p=overlay/name;p.parent.mkdir(parents=True);p.write_text("VALUE='candidate'\n")
  (root/'cereal/__init__.py').write_text("VALUE='private_cereal'\n")
  parent=tmp_path/'smoke';view=parent/'openpilot';deploy.make_smoke_view(root,overlay,view,[name])
  program="from openpilot.selfdrive.controls.lib import approach_comfort as m; import cereal; from pathlib import Path; import sys; assert m.VALUE=='candidate'; assert Path(m.__file__).resolve().is_relative_to(Path(sys.argv[1])); assert Path(cereal.__file__).resolve().is_relative_to(Path(sys.argv[1])); print('PASS private source namespace')"
  result=subprocess.run([sys.executable,'-c',program,str(view)],cwd=parent,env=deploy.smoke_environment(parent,view),capture_output=True,text=True)
  assert result.returncode==0,result.stderr
  assert not (root/name).exists()
