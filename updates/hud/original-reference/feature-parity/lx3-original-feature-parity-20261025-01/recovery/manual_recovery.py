"""Manual file recovery only: signed fresh owner condition, stopped, NO restart.

Restore this feature layer first. Only then can the immutable S0/D1 recovery
tools recognize their protected original hashes. Never fabricate confirmation.
"""
from pathlib import Path
import argparse,base64,hashlib,json,time,types
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

PIN='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
sha=lambda b:hashlib.sha256(b).hexdigest()

def signed(path,public):
  raw=path.read_bytes();assert len(raw)<65536
  envelope=json.loads(raw);body=base64.b64decode(envelope['payload'],validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),body)
  return json.loads(body)

def recover(folder,root,authorization,serial):
  import fcntl
  lock=(folder/'installer.lock').open('a+b');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
  public=base64.b64decode(json.loads(Path('/data/community/hud_updates/config.json').read_text())['public_key'],validate=True)
  assert sha(public)==PIN
  descriptor=signed(Path(__file__).parent/'RECOVERY.signed.json',public)
  assert descriptor['kind']=='original-lx3-feature-file-recovery-v1' and descriptor['target_serial']==serial=='80acfe79'
  raw=(folder/'installer.py').read_bytes()
  assert sha(raw)==descriptor['installer_sha256'] and sha(Path(__file__).read_bytes())==descriptor['helper_sha256']
  deploy=types.ModuleType('pinned_feature_installer');deploy.__file__=str(folder/'installer.py')
  exec(compile(raw,deploy.__file__,'exec'),deploy.__dict__)
  core,policy=deploy.support(folder);core._feature_folder=str(folder)
  manifest,files,bundle=deploy.verify(root,folder,core,policy)
  assert bundle==descriptor['bundle_sha256'] and manifest['release_id']==descriptor['release_id']
  auth=signed(authorization,public);now=time.clock_gettime(time.CLOCK_BOOTTIME)
  assert auth['kind']=='physical-recovery-confirmation-v1' and auth['target_serial']==serial and auth['bundle_sha256']==bundle
  assert auth['boot_id']==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
  assert 0<=now-auth['confirmed_device_boot_s']<=120
  assert now<=auth['expires_device_boot_s']<=auth['confirmed_device_boot_s']+120
  assert auth['physical_condition'] in ('P_zero_control_off','physically_CAN_disconnected_bench')
  assert auth['confirmed_by']=='owner' and len(auth['source_user_message_sha256'])==64
  record_path=folder/'transaction.json';record=json.loads(record_path.read_text())
  deploy.validate_record(manifest,files,record,bundle,core)
  pending=Path('/data/community/hud_updates/state.json')
  assert json.loads(pending.read_text()).get('phase')=='complete'
  core.stopped();deploy.schema_link(root);core.validate_restore(root,folder,record)
  paths=deploy.settings_paths(Path('/data/params/d'),Path('/data/community'))
  deploy.validate_settings_restore(record,paths)
  record['phase']='manual_feature_file_recovery_in_progress';record['physical_recovery_confirmation']=auth;core.save(record_path,record)
  try:
    assert time.clock_gettime(time.CLOCK_BOOTTIME)<=auth['expires_device_boot_s']
    core.restore(root,folder,record,restore_preferences=False)
    if record.get('settings_attempted'):deploy.restore_settings(core,record,paths)
    core.before(root,files);deploy.schema_link(root);core.stopped()
  except BaseException:
    record['phase']='recovery_required';core.save(record_path,record);raise
  record.update(phase='manual_feature_files_restored_no_restart',runtime_modified=False);core.save(record_path,record)
  return {'restored':True,'services_started':False,'rebooted':False,'S0_D1_remain_installed':True,'bundle_sha256':bundle}

if __name__=='__main__':
  p=argparse.ArgumentParser(description=__doc__);p.add_argument('--folder',type=Path,required=True)
  p.add_argument('--root',type=Path,default=Path('/data/openpilot'));p.add_argument('--authorization',type=Path,required=True)
  a=p.parse_args()
  from openpilot.system.hardware import HARDWARE
  print(json.dumps(recover(a.folder.resolve(),a.root.resolve(),a.authorization,HARDWARE.get_serial())))
