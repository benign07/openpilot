"""Separate signed source-only deployment: cruise.py/authority.py aren't HUD OTA.

Never run while driving. No preference, firmware, schema or native writes.
The default remains OFF; the owner selects it after installation in the HUD.
"""
from pathlib import Path
import argparse,base64,hashlib,importlib.util,json,os,subprocess,sys,time
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
RID='lx3-cruise-keep-lateral-20261026-01'
SERIAL='80acfe79'
PUBLIC_SHA='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
CORE_SHA='d08e4062811ec1099cd3afc9b84bda2174f712a72f3ef5db0a0a6dc0d7ba6a03'
ORDER=['selfdrive/controls/lib/approach_comfort.py','selfdrive/car/lx3_authority.py','selfdrive/car/cruise.py',
 'selfdrive/carrot/server/services/params.py','selfdrive/carrot/server/features/params.py',
 'selfdrive/carrot/server/features/system.py','selfdrive/carrot/server/services/settings.py']
FIXED_NATIVE={
 'selfdrive/pandad/pandad':'4b1f395024b2fb570f000491013981e6c1838d8553ab92ac282cf8d8533dfb0c',
 'common/params_pyx.so':'5ddd33516911f918e714bb94712b1e5932267b24f1e83a45d225453a668b6e48',
 'common/params_keys.h':'5f68a38eb84937fd5a4c8565c0d86e87966ea7c8682bc92ddff3750149a08ec0',
 'panda/board/obj/panda_h7.bin.signed':'075aba179ae30aad23235779e77f946eb0d24d56edb3c0553467e0e7928fbdb6',
 'opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py':'19419019764695a94ca87cdf10a3beeec9fe0e5f79ab10e5bedbe3cc470d673c',
}
sha=lambda raw:hashlib.sha256(raw).hexdigest()

def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m);return m

def verify(folder,root,config=Path('/data/community/hud_updates/config.json')):
 public=base64.b64decode(json.loads(config.read_text())['public_key'],validate=True);assert sha(public)==PUBLIC_SHA
 raw=(folder/'package.signed.json').read_bytes();assert len(raw)<2_000_000
 envelope=json.loads(raw);payload=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),payload)
 m=json.loads(payload);assert m['release_id']==RID and m['target_serial']==SERIAL
 assert m['car_fingerprint']=='HYUNDAI_PALISADE_LX3_HEV'
 assert m['installer_sha256']==sha(Path(__file__).read_bytes())
 assert m['support_sha256']['haptic_core.py']==CORE_SHA
 for n,h in m['support_sha256'].items():
  assert n in ('haptic_core.py','feature_core.py') and sha((folder/n).read_bytes())==h
 assert set(m['support_sha256'])=={'haptic_core.py','feature_core.py'}
 core=load('keep_lateral_transaction_core',folder/'haptic_core.py');core.ORDER=ORDER;core.PROTECTED=m['protected']
 assert all(core.PROTECTED.get(n)==h for n,h in FIXED_NATIVE.items())
 for n,h in core.PROTECTED.items():
  path=__import__('pathlib').PurePosixPath(n)
  assert not path.is_absolute() and '..' not in path.parts and '\\' not in n and len(h)==64
 assert not set(core.PROTECTED)&set(ORDER)
 files={v['path']:v for v in m['files']};assert len(m['files'])==len(ORDER) and set(files)==set(ORDER)
 for n,v in files.items():
  core.checked(root,n);data=base64.b64decode(v['data'],validate=True)
  assert len(data)==v['bytes'] and sha(data)==v['sha256'] and len(v['before'])==64
  compile(data,n,'exec')
 return m,files,sha(raw),core

def validate_record(m,files,record,bundle):
 assert record['bundle_sha256']==bundle and record['release_id']==RID
 assert record['source_commit']==m['source_commit'] and set(record['files'])==set(ORDER)
 assert all(all(v[k]==files[n][k] for k in ('before','sha256','bytes')) for n,v in record['files'].items())

def smoke(root,folder,core):
 # Reuse the hash-bound namespace handling qualified on this exact device.
 feature=load('private_feature_namespace',folder/'feature_core.py')
 with __import__('tempfile').TemporaryDirectory(prefix='lx3-main-smoke-') as temp:
  parent=Path(temp);view=parent/'openpilot';feature.make_smoke_view(root,folder/'new',view,ORDER)
  program="""import sys,tempfile
from pathlib import Path
from openpilot.common.params import Params
from openpilot.selfdrive.car.cruise import VCruiseCarrot
from openpilot.selfdrive.controls.lib.approach_comfort import CruiseButtonPreferences
from openpilot.selfdrive.carrot.server.services import params,settings
root=Path(sys.argv[1]);assert len(Params().all_keys())==333
assert Path(sys.modules[VCruiseCarrot.__module__].__file__).resolve().is_relative_to(root)
assert callable(VCruiseCarrot._handle_lx3_main_release) and callable(VCruiseCarrot._update_lx3_main_latch)
with tempfile.TemporaryDirectory() as d:
 p=CruiseButtonPreferences(Path(d)/'buttons.json');assert p.read()=={'CruiseOffKeepLateral':0}
 assert p.set('CruiseOffKeepLateral',1)==1 and p.set('CruiseOffKeepLateral',0)==0
data=settings.get_settings_cached()[2];assert data['CruiseOffKeepLateral']['default']==0
print('PASS private MAIN option imports, 333 registry, separate preference and HUD menu')
"""
  r=subprocess.run([sys.executable,'-c',program,str(view)],cwd=parent,env=feature.smoke_environment(parent,view),capture_output=True,text=True,timeout=45)
  core.atomic(folder/'smoke.stdout',r.stdout.encode());core.atomic(folder/'smoke.stderr',r.stderr.encode());assert r.returncode==0,r.stderr[-2000:]

def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('phase',choices=['stage','install','rollback']);parser.add_argument('--folder',type=Path,required=True)
 args=parser.parse_args();folder=args.folder;root=Path('/data/openpilot')
 assert root.resolve()==root and folder==Path('/data/lx3_separate_installs')/RID and folder.resolve()==folder
 from openpilot.system.hardware import HARDWARE
 assert HARDWARE.get_serial()==SERIAL
 from openpilot.common.params import Params
 from cereal import car
 with car.CarParams.from_bytes(Params().get('CarParams')) as cp:
  assert cp.carFingerprint=='HYUNDAI_PALISADE_LX3_HEV' and not cp.passive and not cp.dashcamOnly
  assert any(s.safetyParam==2238 for s in cp.safetyConfigs)
 import fcntl
 lock=(folder/'installer.lock').open('a+b');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
 assert json.loads(Path('/data/community/hud_updates/state.json').read_text())['phase']=='complete'
 m,files,bundle,core=verify(folder,root);record_path=folder/'transaction.json'
 if args.phase=='stage':
  assert not record_path.exists();core.parked();core.before(root,files)
  record={'phase':'staging','release_id':RID,'source_commit':m['source_commit'],'bundle_sha256':bundle,
   'boot_before':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'files':{},'preferences':{}}
  core.save(record_path,record)
  for n in ORDER:
   p=core.checked(root,n);mode=p.stat().st_mode&0o777
   core.atomic(folder/'before'/n,p.read_bytes(),mode);core.atomic(folder/'new'/n,base64.b64decode(files[n]['data']),mode)
   record['files'][n]={**{k:files[n][k] for k in ('before','sha256','bytes')},'mode':mode}
  smoke(root,folder,core);record['phase']='staged';core.save(record_path,record)
 else:
  record=json.loads(record_path.read_text());validate_record(m,files,record,bundle)
  if args.phase=='install':
   assert record['phase']=='staged';core.before(root,files)
   assert all(sha((folder/'new'/n).read_bytes())==files[n]['sha256'] for n in ORDER)
  else:
   assert record['phase'] in ('complete','installed_pending_reboot_verification','recovery_required');core.validate_restore(root,folder,record)
  gate=core.parked();previous=record['phase'];record['phase']='stopping';core.save(record_path,record);changed=False
  try:
   subprocess.run(['sudo','-n','systemctl','stop','comma.service'],check=True);core.stopped()
   assert json.loads(Path('/data/community/hud_updates/state.json').read_text())['phase']=='complete'
   if args.phase=='install':
    core.before(root,files);changed=True;record['phase']='installing';core.save(record_path,record)
    for n in ORDER:core.atomic(core.checked(root,n),(folder/'new'/n).read_bytes(),record['files'][n]['mode'])
    assert all(sha(core.checked(root,n).read_bytes())==files[n]['sha256'] for n in ORDER)
    assert all(sha((root/n).read_bytes())==h for n,h in core.PROTECTED.items())
    assert time.monotonic()-gate['mono']<15
    record['phase']='installed_pending_reboot_verification'
   else:
    core.validate_restore(root,folder,record);changed=True;core.restore(root,folder,record,restore_preferences=False)
    core.before(root,files);record['phase']='restored_before_feature_parity_layer'
   core.save(record_path,record)
  except BaseException:
   if changed and args.phase=='install':
    try:core.restore(root,folder,record,restore_preferences=False);core.before(root,files);record['phase']='restored_after_failure';core.save(record_path,record)
    except BaseException:record['phase']='recovery_required';core.save(record_path,record);raise
   elif changed:
    record['phase']='recovery_required';core.save(record_path,record);raise
   else:record['phase']=previous;core.save(record_path,record)
   subprocess.run(['sudo','-n','systemctl','start','comma.service'],check=True);raise
  subprocess.Popen(['sudo','-n','sh','-c','sleep 3; reboot'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 print('RESULT_JSON='+json.dumps({'release_id':RID,'phase':record['phase'],'preference_changed':False,'road_qualified':False}))

if __name__=='__main__':main()
