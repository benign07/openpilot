"""Separate, signed, parked original-LX3 haptic deployment; NOT source OTA.

Stage/import verification never changes executing source. Install stops the
manager first, applies prerequisite-first compatible files, and restores in
reverse order on failure. No CAN transmit/publisher or full native build.
"""
from pathlib import Path
import argparse,base64,hashlib,json,os,subprocess,sys,tempfile,time
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

RID='lx3-original-haptic-20261023-01'
SERIAL='80acfe79'
PUBLIC_SHA='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
ORDER=['opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py',
 'common/params_pyx.so','common/params_keys.h',
 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py',
 'opendbc_repo/opendbc/car/hyundai/carcontroller.py',
 'selfdrive/controls/controlsd.py','selfdrive/carrot_settings.json']
PROTECTED={'selfdrive/pandad/pandad':'02302e2a712168d4ed6532b7f2f589ccf91abacebc63e26b79ee034763019d64',
 'selfdrive/car/card.py':'139d6cfc396148170f980ccbf68bb810542a360fbe3be15906103d4fed30598c',
 'cereal/car.capnp':'9322ea6f777daf556e362b5f87e6f555db53c2a433c05a05f09913c4ee3a98ec'}
PREFS=('OpenpilotEnabledToggle','C3xLiteHapticNotifications')
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def atomic(path,raw,mode=0o600):
 path.parent.mkdir(parents=True,exist_ok=True)
 with tempfile.NamedTemporaryFile(prefix='.lx3-haptic-',dir=path.parent,delete=False) as f:
  temp=Path(f.name);f.write(raw);f.flush();os.fsync(f.fileno());os.chmod(temp,mode)
 os.replace(temp,path)
 fd=os.open(path.parent,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)


def save(path,data):atomic(path,(json.dumps(data,indent=2)+'\n').encode())


def checked(root,name):
 assert name in ORDER
 p=root/name
 assert not p.is_symlink() and p.parent.resolve().is_relative_to(root.resolve())
 return p


def verify(root,folder,config_path=Path('/data/community/hud_updates/config.json')):
 config=json.loads(config_path.read_text())
 public=base64.b64decode(config['public_key'],validate=True);assert sha(public)==PUBLIC_SHA
 raw=(folder/'package.signed.json').read_bytes();assert len(raw)<16_000_000
 envelope=json.loads(raw);payload=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),payload)
 manifest=json.loads(payload)
 assert manifest['release_id']==RID and manifest['target_serial']==SERIAL
 assert manifest['installer_sha256']==sha(Path(__file__).read_bytes())
 assert len(manifest['files'])==len(ORDER) and {x['path'] for x in manifest['files']}==set(ORDER)
 assert manifest['native_build']['key_count']==317 and manifest['native_build']['original316_descriptors_preserved']
 assert manifest['native_build']['ABI_subset_of_original_backup']
 files={x['path']:x for x in manifest['files']}
 for name,row in files.items():
  checked(root,name);data=base64.b64decode(row['data'],validate=True)
  assert len(data)==row['bytes'] and sha(data)==row['sha256']
  if name.endswith('.py'):compile(data,name,'exec')
 assert files['common/params_pyx.so']['sha256']==manifest['native_build']['module_sha256']
 assert files['common/params_keys.h']['sha256']==manifest['native_build']['registry_header_sha256']
 return manifest,files,sha(raw)


def before(root,files):
 assert (root/'prebuilt').is_file() and not (root/'prebuilt').is_symlink()
 for name,digest in PROTECTED.items():assert sha((root/name).read_bytes())==digest,name
 for name,row in files.items():
  path=checked(root,name)
  assert (sha(path.read_bytes()) if path.is_file() else None)==row['before'],name


def parked(seconds=10):
 from cereal import messaging
 sm=messaging.SubMaster(['carState','carControl','selfdriveState','pandaStates'])
 start=time.monotonic();since=None
 while time.monotonic()-start<seconds+8:
  sm.update(100);now=time.monotonic()
  fresh=all(sm.alive[n] and sm.valid[n] and 0<=now-sm.logMonoTime[n]/1e9<1 for n in sm.services)
  cc,cs,ss=sm['carControl'],sm['carState'],sm['selfdriveState']
  good=(fresh and str(cs.gearShifter)=='park' and abs(cs.vEgo)<.01 and abs(cs.vEgoRaw)<.01 and cs.canValid
    and not any((cc.enabled,cc.latActive,cc.longActive,ss.active,ss.enabled))
    and len(sm['pandaStates'])>0 and not any(p.controlsAllowed for p in sm['pandaStates']))
  if not good:since=None
  elif since is None:since=now
  elif now-since>=seconds:return {'continuous_seconds':now-since,'mono':now,'gear':'park','speed':float(cs.vEgo)}
 raise RuntimeError('Fresh continuous P/zero/inactive control required')


def stopped():
 assert subprocess.run(['systemctl','is-active','comma.service'],capture_output=True,text=True).stdout.strip()=='inactive'
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit() or int(proc.name)==os.getpid():continue
  try:
   comm=(proc/'comm').read_text().strip();args=(proc/'cmdline').read_bytes().split(b'\0')
   assert comm not in ('pandad','controlsd','card','loggerd'),(proc.name,comm)
   assert not any(a in (b'./manager.py',b'./pandad',b'./controlsd',b'./card',
     b'openpilot.system.manager.manager',b'openpilot.selfdrive.car.card',
     b'openpilot.selfdrive.controls.controlsd',b'selfdrive.pandad.pandad') for a in args),(proc.name,comm)
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass


def import_smoke(root,folder):
 # Fresh process: don't let a previously loaded old Params extension hide a
 # bad native deployment. All writes use a temporary Params directory.
 program=r'''
from pathlib import Path
import importlib.util,sys,tempfile,json
base=Path(sys.argv[1]);root=Path('/data/openpilot')
def load(name,relative):
 spec=importlib.util.spec_from_file_location(name,base/relative)
 module=importlib.util.module_from_spec(spec);sys.modules[name]=module;spec.loader.exec_module(module)
 return module
native=load('openpilot.common.params_pyx','common/params_pyx.so')
with tempfile.TemporaryDirectory(prefix='lx3-haptic-import-') as temp:
 p=native.Params(temp);assert len(p.all_keys())==317
 assert p.check_key('C3xLiteHapticNotifications') and not p.get_bool('C3xLiteHapticNotifications')
 p.put_bool('C3xLiteHapticNotifications',True);assert p.get_bool('C3xLiteHapticNotifications')
helper=load('opendbc.car.hyundai.lx3_haptic_notifications','opendbc_repo/opendbc/car/hyundai/lx3_haptic_notifications.py')
factory=load('opendbc.car.hyundai.hyundaicanfd','opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py')
import opendbc.car.hyundai as package
package.hyundaicanfd=factory
controller=load('opendbc.car.hyundai.carcontroller','opendbc_repo/opendbc/car/hyundai/carcontroller.py')
from opendbc.car import structs
cc=structs.CarControl.new_message();cc.hudControl.audibleAlert='engage'
with structs.CarControl.from_bytes(cc.to_bytes()) as decoded:
 assert helper.HapticNotifications().update(decoded.hudControl.audibleAlert,eligible=True,frame=0)
assert controller.CarController and factory.create_ccnc_messages
print(json.dumps({'native_keys':317,'native_key_roundtrip':True,'original_schema_enum':True,
'candidate_controller_imported':True,'factory_keyword':'notification_vibrate' in __import__('inspect').signature(factory.create_ccnc_messages).parameters,
'live_CAN_TX':False}))
'''
 result=subprocess.run([sys.executable,'-c',program,str(folder/'new')],capture_output=True,text=True,cwd=root,timeout=40)
 atomic(folder/'import-smoke.stdout',result.stdout.encode());atomic(folder/'import-smoke.stderr',result.stderr.encode())
 if result.returncode:raise RuntimeError('Staged import failed: '+result.stderr[-1500:])
 report=json.loads(result.stdout.splitlines()[-1]);assert report['factory_keyword']
 save(folder/'import-smoke.json',report);return report


def restore(root,folder,record,params_dir=Path('/data/params/d')):
 for name in reversed(ORDER):
  path=checked(root,name);row=record['files'][name]
  if row['before'] is None:
   if path.exists():
    assert sha(path.read_bytes())==row['sha256'];path.unlink()
  else:atomic(path,(folder/'before'/name).read_bytes(),row['mode'])
 for key,old in record['preferences'].items():
  path=params_dir/key
  if old is None:path.unlink(missing_ok=True)
  else:atomic(path,base64.b64decode(old),0o600)


def main():
 parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('phase',choices=['stage','install'])
 parser.add_argument('--folder',type=Path,required=True);args=parser.parse_args()
 root=Path('/data/openpilot');folder=args.folder
 assert root.resolve()==root and folder==Path('/data/lx3_separate_installs')/RID and folder.resolve()==folder
 from openpilot.system.hardware import HARDWARE
 from openpilot.common.params import Params
 assert HARDWARE.get_serial()==SERIAL
 assert sys.version_info[:2]==(3,12) and __import__('platform').machine()=='aarch64'
 state=json.loads(Path('/data/community/hud_updates/state.json').read_text());assert state['phase']=='complete'
 p=Params();assert not p.get_bool('OpenpilotEnabledToggle') and p.get_int('CanfdDebug')==0
 manifest,files,bundle_sha=verify(root,folder);before(root,files)
 record_path=folder/'transaction.json'
 if args.phase=='stage':
  assert not record_path.exists();gate=parked()
  record={'phase':'staging','release_id':RID,'source_commit':manifest['source_commit'],'bundle_sha256':bundle_sha,
    'boot_before':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'gate':gate,'files':{},
    'preferences':{k:base64.b64encode((Path('/data/params/d')/k).read_bytes()).decode() if (Path('/data/params/d')/k).exists() else None for k in PREFS},
    'preserved_settings':{k:sha((Path('/data/params/d')/k).read_bytes()) for k in ('DisableDM','HardwareC3xLite','CalibrationParams','DongleId','CarSelected3','HapticFeedbackWhenSpeedCamera','CanfdDebug') if (Path('/data/params/d')/k).is_file()},
    'runtime_modified':False,'mode_changed':False}
  save(record_path,record)
  for name in ORDER:
   row=files[name];path=checked(root,name);mode=path.stat().st_mode&0o777 if path.exists() else 0o644
   if row['before'] is not None:atomic(folder/'before'/name,path.read_bytes(),mode)
   atomic(folder/'new'/name,base64.b64decode(row['data']),mode)
   record['files'][name]={k:row[k] for k in ('before','sha256','bytes')};record['files'][name]['mode']=mode
  record['smoke']=import_smoke(root,folder);record['phase']='staged';save(record_path,record)
 else:
  record=json.loads(record_path.read_text());assert record['phase']=='staged' and record['bundle_sha256']==bundle_sha
  for name in ORDER:assert sha((folder/'new'/name).read_bytes())==files[name]['sha256']
  import_smoke(root,folder);record['gate_before_stop']=parked();record['phase']='stopping';save(record_path,record)
  subprocess.run(['sudo','-n','systemctl','stop','comma.service'],check=True)
  try:
   stopped();before(root,files);record['phase']='installing';save(record_path,record)
   for name in ORDER:
    atomic(checked(root,name),(folder/'new'/name).read_bytes(),record['files'][name]['mode'])
    record['last_file']=name;save(record_path,record)
   assert all(sha(checked(root,n).read_bytes())==files[n]['sha256'] for n in ORDER)
   assert all(sha((root/n).read_bytes())==h for n,h in PROTECTED.items())
   assert all(sha((Path('/data/params/d')/k).read_bytes())==h for k,h in record['preserved_settings'].items())
   assert time.monotonic()-record['gate_before_stop']['mono']<15,'Installation exceeded fresh-stop window'
   command="from openpilot.common.params import Params;p=Params();assert len(p.all_keys())==317;p.put_bool('C3xLiteHapticNotifications',True);p.put_bool('OpenpilotEnabledToggle',True);assert p.get_bool('C3xLiteHapticNotifications') and p.get_bool('OpenpilotEnabledToggle')"
   subprocess.run([sys.executable,'-c',command],cwd=root,check=True)
   record.update(phase='installed_pending_reboot_verification',runtime_modified=True,mode_changed=True)
   save(record_path,record)
  except BaseException:
   restore(root,folder,record);record.update(phase='restored_after_failure',runtime_modified=False,mode_changed=False);save(record_path,record)
   raise
  finally:subprocess.run(['sudo','-n','systemctl','start','comma.service'],check=True)
  subprocess.Popen(['sudo','-n','sh','-c','sleep 3; reboot'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 print('RESULT_JSON='+json.dumps({k:v for k,v in record.items() if k not in ('preferences',)}),flush=True)


if __name__=='__main__':main()
