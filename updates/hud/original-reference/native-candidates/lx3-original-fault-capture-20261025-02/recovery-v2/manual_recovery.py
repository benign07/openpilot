"""Stopped original-device file restoration; NEVER starts service or reboots.

Requires a short-lived signed record of fresh physical P/zero/control-off or
physically CAN-disconnected bench confirmation. Not an automatic parked bypass.
"""
from pathlib import Path
import argparse,base64,hashlib,json,time,types
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

def signed(path,public):
 content=path.read_bytes();assert len(content)<65536
 envelope=json.loads(content);raw=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),raw)
 return json.loads(raw)

def recover(folder,root,authorization,serial):
 config=json.loads(Path('/data/community/hud_updates/config.json').read_text())
 public=base64.b64decode(config['public_key'],validate=True)
 assert hashlib.sha256(public).hexdigest()=='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
 descriptor=signed(Path(__file__).resolve().parent/'RECOVERY.signed.json',public)
 assert descriptor['kind']=='original-lx3-manual-file-recovery-v1'
 assert serial==descriptor['target_serial']=='80acfe79'
 installer=(folder/'installer.py').read_bytes()
 assert hashlib.sha256(installer).hexdigest()==descriptor['installer_sha256']
 assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==descriptor['script_sha256']
 # Import only already verified bytes; don't re-open executable source after
 # checking its hash. No controller/device module is started by the installer.
 deploy=types.ModuleType('pinned_original_installer');deploy.__file__=str(folder/'installer.py')
 exec(compile(installer,deploy.__file__,'exec'),deploy.__dict__)
 manifest,files,bundle_sha=deploy.verify(folder,public)
 assert descriptor['bundle_sha256']==bundle_sha and descriptor['target_serial']==serial==deploy.SERIAL
 assert descriptor['installer_sha256']==manifest['installer_sha256']
 assert descriptor['script_sha256']==deploy.sha(Path(__file__).read_bytes())
 auth=signed(authorization,public);now=time.clock_gettime(time.CLOCK_BOOTTIME)
 assert auth['kind']=='physical-recovery-confirmation-v1'
 assert auth['bundle_sha256']==bundle_sha and auth['target_serial']==serial
 assert auth['boot_id']==Path('/proc/sys/kernel/random/boot_id').read_text().strip()
 # Boot ID plus BOOTTIME avoids PC/device wall-clock or NTP differences.
 assert 0<=now-auth['confirmed_device_boot_s']<=120
 assert now<=auth['expires_device_boot_s']<=auth['confirmed_device_boot_s']+120
 assert auth['physical_condition'] in ('P_zero_control_off','physically_CAN_disconnected_bench')
 assert isinstance(auth['confirmed_by'],str) and 0<len(auth['confirmed_by'])<=128
 assert len(auth['source_user_message_sha256'])==64
 record_path=folder/'transaction.json';record=json.loads(record_path.read_text())
 deploy.validate_record(manifest,files,record,bundle_sha)
 # Fixed signed order ensures native first even if journal key order changes.
 record['files']={n:record['files'][n] for n in deploy.RELEASES[manifest['release_id']]}
 deploy.no_pending_ota();deploy.stopped();deploy.validate_restore(root,folder,record)
 record['physical_recovery_confirmation']={k:auth[k] for k in ('confirmed_by','confirmed_at_unix','confirmed_at_pc_unix',
  'confirmed_device_boot_s','expires_device_boot_s','physical_condition','boot_id','source_user_message_sha256')}
 record['phase']='manual_file_recovery_in_progress';deploy.save(record_path,record)
 try:
  assert time.clock_gettime(time.CLOCK_BOOTTIME)<=auth['expires_device_boot_s']
  deploy.restore(root,folder,record)
  for name,row in record['files'].items():
   path=root/name;assert (deploy.sha(path.read_bytes()) if path.is_file() else None)==row['before']
  deploy.stopped()
 except BaseException:
  record['phase']='recovery_required';deploy.save(record_path,record);raise
 record.update(phase='manual_files_restored_no_restart',runtime_modified=False)
 deploy.save(record_path,record)
 return {'restored':True,'source_commit':manifest['source_commit'],'bundle_sha256':bundle_sha,
  'physical_confirmation':record['physical_recovery_confirmation'],'services_started':False,'rebooted':False}

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--folder',type=Path,required=True);parser.add_argument('--authorization',type=Path,required=True)
 parser.add_argument('--root',type=Path,default=Path('/data/openpilot'));args=parser.parse_args()
 from openpilot.system.hardware import HARDWARE
 print(json.dumps(recover(args.folder.resolve(),args.root.resolve(),args.authorization,HARDWARE.get_serial())))

if __name__=='__main__':main()
