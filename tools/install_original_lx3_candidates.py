"""Signed original-LX3 diagnostic/SPI-observation deployment, not source OTA.

No setting writes, CAN publishers, Panda clients, firmware flash or device build.
The experimental ready profile is deliberately absent from the install allowlist.
"""
from pathlib import Path
import argparse,base64,hashlib,importlib.util,json,os,struct,subprocess,sys,tempfile,time
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

DIAG=('selfdrive/carrot/can_diagnostics/automatic.py','selfdrive/carrot/can_diagnostics/automatic_runtime.py')
NATIVE=('selfdrive/pandad/spi_diagnostics.h','selfdrive/pandad/spi.cc','selfdrive/pandad/pandad')
RELEASES={'lx3-original-fault-capture-20261025-01':DIAG,'lx3-original-spi-observe-20261025-01':DIAG+NATIVE}
SERIAL='80acfe79'
PUBLIC_SHA='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
BASE_NATIVE='02302e2a712168d4ed6532b7f2f589ccf91abacebc63e26b79ee034763019d64'
PROTECTED={'selfdrive/car/card.py':'139d6cfc396148170f980ccbf68bb810542a360fbe3be15906103d4fed30598c',
 'cereal/car.capnp':'9322ea6f777daf556e362b5f87e6f555db53c2a433c05a05f09913c4ee3a98ec',
 'common/params_pyx.so':'8aa96c0a392bc9f10d640164ae9262b149851fe1976c50ab3569244a14beeb3b',
 'common/params_keys.h':'210e60d225d1843aff35d3af6dbede63652ce46fb4fe475629281e43688470ce'}
PRESERVE=('DisableDM','HardwareC3xLite','CalibrationParams','CarSelected3','OpenpilotEnabledToggle',
 'C3xLiteHapticNotifications','HapticFeedbackWhenSpeedCamera','CanfdDebug','MyDrivingMode')
sha=lambda raw:hashlib.sha256(raw).hexdigest()


def atomic(path,raw,mode=0o600):
 path.parent.mkdir(parents=True,exist_ok=True)
 with tempfile.NamedTemporaryFile(prefix='.lx3-candidate-',dir=path.parent,delete=False) as f:
  temp=Path(f.name);f.write(raw);f.flush();os.fsync(f.fileno());os.chmod(temp,mode)
 os.replace(temp,path)
 fsync_directory(path.parent)


def fsync_directory(path):
 fd=os.open(path,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)


def save(path,value):atomic(path,(json.dumps(value,indent=2)+'\n').encode())


def checked(root,name,allowed):
 assert name in allowed
 path=root/name
 assert not path.is_symlink() and path.parent.resolve().is_relative_to(root.resolve())
 return path


def verify(folder,public):
 assert sha(public)==PUBLIC_SHA
 raw=(folder/'package.signed.json').read_bytes();assert len(raw)<=64*1024**2
 envelope=json.loads(raw);body=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),body)
 manifest=json.loads(body);rid=manifest['release_id'];assert rid in RELEASES
 assert manifest['target_serial']==SERIAL and manifest['installer_sha256']==sha(Path(__file__).read_bytes())
 assert manifest['firmware_changed'] is False and manifest['actuation_source_changed'] is False
 assert manifest['road_qualified'] is False and manifest['baseline_native']==BASE_NATIVE
 assert manifest['protected']==PROTECTED
 order=RELEASES[rid];rows=manifest['files'];assert len(rows)==len(order)
 assert {r['path'] for r in rows}==set(order)
 files={r['path']:r for r in rows}
 for name,row in files.items():
  raw=base64.b64decode(row['data'],validate=True)
  assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
  assert isinstance(row['allowed_before'],list) and row['allowed_before']
  if name.endswith('.py'):compile(raw,name,'exec')
  if name=='selfdrive/pandad/pandad':
   assert raw[:6]==b'\x7fELF\x02\x01' and struct.unpack_from('<H',raw,18)[0]==183
   native=manifest['native_build']
   assert native['profile']=='observed' and native['spi_ready_profile']==0
   assert native['runtime_sha256']==row['sha256'] and native['protocol_pass']
   assert native['schema_sha256']==PROTECTED['cereal/car.capnp'] and not native['working_tree_dirty']
 return manifest,files,sha((folder/'package.signed.json').read_bytes())


def before(root,manifest,files):
 assert (root/'prebuilt').is_file() and not (root/'prebuilt').is_symlink()
 for name,digest in PROTECTED.items():
  path=root/name
  assert path.resolve().is_relative_to(root.resolve()) and sha(path.read_bytes())==digest,name
 assert sha((root/'selfdrive/pandad/pandad').read_bytes())==BASE_NATIVE
 for name,row in files.items():
  path=checked(root,name,files)
  assert (sha(path.read_bytes()) if path.is_file() else None) in row['allowed_before'],name


def no_pending_ota(state=Path('/data/community/hud_updates/state.json')):
 if not state.is_file():return
 assert state.stat().st_size<2*1024**2
 phase=json.loads(state.read_text()).get('phase')
 assert phase not in ('armed','applying','rollback_armed','rolling_back','recovery_required'),phase


def parked(seconds=10):
 from cereal import messaging
 sm=messaging.SubMaster(['carState','carControl','selfdriveState','pandaStates'])
 start=time.monotonic();since=None
 while time.monotonic()-start<seconds+8:
  sm.update(100);now=time.monotonic();native_now=time.clock_gettime(time.CLOCK_BOOTTIME)
  fresh=all(sm.alive[n] and sm.valid[n] and
    0<=(native_now if n=='pandaStates' else now)-sm.logMonoTime[n]/1e9<1 for n in sm.services)
  cc,cs,ss=sm['carControl'],sm['carState'],sm['selfdriveState']
  good=(fresh and str(cs.gearShifter)=='park' and abs(cs.vEgo)<.01 and abs(cs.vEgoRaw)<.01 and cs.canValid
    and not any((cc.enabled,cc.latActive,cc.longActive,ss.active,ss.enabled))
    and len(sm['pandaStates'])>0 and not any(p.controlsAllowed for p in sm['pandaStates']))
  if not good:since=None
  elif since is None:since=now
  elif now-since>=seconds:return {'mono':now,'continuous_seconds':now-since,'gear':'park','speed':float(cs.vEgo)}
 raise RuntimeError('Fresh continuous P/zero/inactive control required')


def stopped():
 assert subprocess.run(['systemctl','is-active','comma.service'],capture_output=True,text=True).stdout.strip()=='inactive'
 for proc in Path('/proc').iterdir():
  if not proc.name.isdigit() or int(proc.name)==os.getpid():continue
  try:
   comm=(proc/'comm').read_text().strip();args=(proc/'cmdline').read_bytes().split(b'\0')
   assert comm not in ('pandad','controlsd','card','loggerd'),comm
   assert not any(a in (b'./manager.py',b'./pandad',b'./controlsd',b'./card',
     b'openpilot.system.manager.manager',b'openpilot.selfdrive.car.card',
     b'openpilot.selfdrive.controls.controlsd',b'selfdrive.pandad.pandad',
     b'selfdrive.controls.controlsd',b'selfdrive.car.card') for a in args),comm
  except (FileNotFoundError,ProcessLookupError,PermissionError):pass


def import_smoke(folder):
 # Imports only staged passive modules. No controller startup or real CAN.
 program=r"""
from pathlib import Path
import importlib,importlib.util,importlib.machinery,sys,binascii,json
directory=Path(sys.argv[1])/'new/selfdrive/carrot/can_diagnostics'
spec=importlib.machinery.ModuleSpec('candidate_diag',loader=None,is_package=True)
spec.submodule_search_locations=[str(directory)]
sys.modules['candidate_diag']=importlib.util.module_from_spec(spec)
auto=importlib.import_module('candidate_diag.automatic')
runtime=importlib.import_module('candidate_diag.automatic_runtime')
class Sink:
 errors=queue_dropped=0
 def __init__(self):self.rows=[]
 def submit(self,*args):self.rows.append(args);return True
sink=Sink();capture=auto.FirstFaultCapsule(sink)
capture.begin('inert',{'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV'})
for value,now in ((0,100),(1,100.1)):
 data=bytearray((value<<219).to_bytes(32,'little'))
 data[:2]=(binascii.crc_hqx(data[2:]+b'\x62\x01',0)^0x9f5b).to_bytes(2,'little')
 capture.can_frame(2,0x162,bytes(data),int(now*1e9),now,'rx')
capture.context({},109)
assert len(sink.rows)==2 and runtime.AutomaticController
print(json.dumps({'candidate_import':True,'inert_lss_capture':True,'CAN_TX':False}))
"""
 result=subprocess.run([sys.executable,'-c',program,str(folder)],capture_output=True,text=True,timeout=40)
 atomic(folder/'smoke.stdout',result.stdout.encode());atomic(folder/'smoke.stderr',result.stderr.encode())
 if result.returncode:raise RuntimeError('Staged diagnostic import failed: '+result.stderr[-1000:])
 return json.loads(result.stdout.splitlines()[-1])


def validate_restore(root,folder,record):
 # Validate all live/backup files before the first rollback write.
 for name,row in record['files'].items():
  path=checked(root,name,record['files']);current=sha(path.read_bytes()) if path.is_file() else None
  assert current in (row['before'],row['sha256']),name
  if row['before'] is not None:assert sha((folder/'before'/name).read_bytes())==row['before']
 for name,digest in PROTECTED.items():assert sha((root/name).read_bytes())==digest,name


def restore(root,folder,record):
 validate_restore(root,folder,record)
 for name in reversed(list(record['files'])):
  row=record['files'][name];path=checked(root,name,record['files'])
  if (sha(path.read_bytes()) if path.is_file() else None)==row['before']:continue
  if row['before'] is None:path.unlink()
  else:atomic(path,(folder/'before'/name).read_bytes(),row['mode'])


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('phase',choices=['stage','install','rollback'])
 p.add_argument('--folder',type=Path,required=True);p.add_argument('--root',type=Path,default=Path('/data/openpilot'))
 args=p.parse_args();root=args.root.resolve();folder=args.folder.resolve()
 from openpilot.system.hardware import HARDWARE
 assert HARDWARE.get_serial()==SERIAL
 config=json.loads(Path('/data/community/hud_updates/config.json').read_text())
 manifest,files,bundle_sha=verify(folder,base64.b64decode(config['verification_public_key'],validate=True))
 rid=manifest['release_id'];record_path=folder/'transaction.json';order=RELEASES[rid]
 if args.phase=='stage':
  assert not record_path.exists();no_pending_ota();before(root,manifest,files)
  record={'phase':'staging','release_id':rid,'source_commit':manifest['source_commit'],
    'bundle_sha256':bundle_sha,'files':{},'runtime_modified':False,
    'boot_before':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
    'preserved_settings':{k:sha((Path('/data/params/d')/k).read_bytes()) for k in PRESERVE if (Path('/data/params/d')/k).is_file()}}
  save(record_path,record)
  for name in order:
   path=checked(root,name,files);row=files[name];mode=path.stat().st_mode&0o777 if path.is_file() else 0o644
   old=path.read_bytes() if path.is_file() else None
   if old is not None:atomic(folder/'before'/name,old,mode)
   if name.endswith('/pandad'):mode=0o755
   atomic(folder/'new'/name,base64.b64decode(row['data']),mode)
   record['files'][name]={'before':sha(old) if old is not None else None,'sha256':row['sha256'],'bytes':row['bytes'],'mode':mode}
  record['smoke']=import_smoke(folder);record['phase']='staged';save(record_path,record)
 else:
  record=json.loads(record_path.read_text());assert record['bundle_sha256']==bundle_sha
  if args.phase=='install':
   assert record['phase']=='staged'
   before(root,manifest,files)
   for name in order:assert sha((folder/'new'/name).read_bytes())==files[name]['sha256']
   import_smoke(folder)
  no_pending_ota()
  gate=parked();record['gate_before_stop']=gate;save(record_path,record)
  subprocess.run(['sudo','-n','systemctl','stop','comma.service'],check=True)
  writes_started=False
  try:
   stopped()
   no_pending_ota()
   if args.phase=='rollback':
    validate_restore(root,folder,record);writes_started=True
    restore(root,folder,record);record.update(phase='restored_by_explicit_rollback',runtime_modified=False)
   else:
    before(root,manifest,files);record['phase']='installing';save(record_path,record);writes_started=True
    for name in order:
     atomic(checked(root,name,files),(folder/'new'/name).read_bytes(),record['files'][name]['mode'])
     record['last_file']=name;save(record_path,record)
    assert all(sha((root/n).read_bytes())==files[n]['sha256'] for n in order)
    assert all(sha((root/n).read_bytes())==h for n,h in PROTECTED.items())
    assert all(sha((Path('/data/params/d')/n).read_bytes())==h for n,h in record['preserved_settings'].items())
    assert time.monotonic()-gate['mono']<15,'Installation exceeded fresh-stop window'
    record.update(phase='installed_pending_reboot_verification',runtime_modified=True)
   save(record_path,record)
  except BaseException:
   if writes_started and args.phase=='rollback':
    record['phase']='recovery_required';save(record_path,record);raise
   if writes_started:
    try:restore(root,folder,record);record.update(phase='restored_after_failure',runtime_modified=False);save(record_path,record)
    except BaseException:record['phase']='recovery_required';save(record_path,record);raise
   subprocess.run(['sudo','-n','systemctl','start','comma.service'],check=True);raise
  # Reboot only after the validated parked transaction. No manager/control
  # process starts briefly between native replacement and reboot.
  subprocess.Popen(['sudo','-n','sh','-c','sleep 3; reboot'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
 print('RESULT_JSON='+json.dumps(record),flush=True)


if __name__=='__main__':main()
