"""Manual signed file recovery only, with fresh physical confirmation. No start/reboot."""
from pathlib import Path
import argparse,base64,hashlib,json,time,types
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
PIN='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
sha=lambda b:hashlib.sha256(b).hexdigest()

def signed(path,public):
 raw=path.read_bytes();assert len(raw)<65536;e=json.loads(raw);body=base64.b64decode(e['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(e['signature'],validate=True),body);return json.loads(body)

def recover(folder,authorization,serial):
 import fcntl
 root=Path('/data/openpilot');assert root.resolve()==root
 assert folder==Path('/data/lx3_separate_installs/lx3-cruise-keep-lateral-20261026-01') and folder.resolve()==folder
 public=base64.b64decode(json.loads(Path('/data/community/hud_updates/config.json').read_text())['public_key'],validate=True);assert sha(public)==PIN
 descriptor=signed(Path(__file__).parent/'RECOVERY.signed.json',public)
 assert descriptor['kind']=='lx3-cruise-keep-source-recovery-v1' and descriptor['target_serial']==serial=='80acfe79'
 assert sha(Path(__file__).read_bytes())==descriptor['helper_sha256']
 raw=(folder/'installer.py').read_bytes();assert sha(raw)==descriptor['installer_sha256']
 deploy=types.ModuleType('pinned_main_installer');deploy.__file__=str(folder/'installer.py');exec(compile(raw,deploy.__file__,'exec'),deploy.__dict__)
 lock=(folder/'installer.lock').open('a+b');fcntl.flock(lock.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
 m,files,bundle,core=deploy.verify(folder,root)
 assert bundle==descriptor['bundle_sha256'] and m['release_id']==descriptor['release_id']
 auth=signed(authorization,public);now=time.clock_gettime(time.CLOCK_BOOTTIME)
 assert auth['kind']=='physical-recovery-confirmation-v1' and auth['target_serial']==serial and auth['bundle_sha256']==bundle
 assert auth['boot_id']==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 assert 0<=now-auth['confirmed_device_boot_s']<=120 and now<=auth['expires_device_boot_s']<=auth['confirmed_device_boot_s']+120
 assert auth['confirmed_by']=='owner' and len(auth['source_user_message_sha256'])==64
 assert auth['physical_condition'] in ('P_zero_control_off','physically_CAN_disconnected_bench')
 record_path=folder/'transaction.json';record=json.loads(record_path.read_text());deploy.validate_record(m,files,record,bundle)
 assert json.loads(Path('/data/community/hud_updates/state.json').read_text())['phase']=='complete'
 core.stopped();core.validate_restore(root,folder,record)
 record['phase']='manual_MAIN_recovery_in_progress';record['physical_confirmation']=auth;core.save(record_path,record)
 try:
  assert time.clock_gettime(time.CLOCK_BOOTTIME)<=auth['expires_device_boot_s']
  core.restore(root,folder,record,restore_preferences=False);core.before(root,files);core.stopped()
 except BaseException:record['phase']='recovery_required';core.save(record_path,record);raise
 record['phase']='manual_MAIN_files_restored_no_restart';core.save(record_path,record)
 return {'restored':True,'services_started':False,'rebooted':False,'preferences_changed':False}

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--folder',type=Path,required=True);p.add_argument('--authorization',type=Path,required=True);a=p.parse_args()
 from openpilot.system.hardware import HARDWARE
 print(json.dumps(recover(a.folder,a.authorization,HARDWARE.get_serial())))
