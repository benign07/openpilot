"""PC-only issuer. Requires a NEW explicit physical confirmation; never infer it.

Archive the exact owner message privately. Read target boot/clock over SSH;
BOOTTIME, not PC/device wall-time agreement, bounds token validity. No token
is issued merely because a package or old parked observation exists.
"""
from pathlib import Path
import argparse,base64,hashlib,json,time,types

def load_package(folder,public):
 from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
 assert hashlib.sha256(public).hexdigest()=='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
 raw=(folder/'package.signed.json').read_bytes();assert len(raw)<=64*1024**2
 envelope=json.loads(raw);body=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),body)
 manifest=json.loads(body);installer=(folder/'installer.py').read_bytes()
 assert hashlib.sha256(installer).hexdigest()==manifest['installer_sha256']
 deploy=types.ModuleType('package_pinned_installer');deploy.__file__=str(folder/'installer.py')
 exec(compile(installer,deploy.__file__,'exec'),deploy.__dict__)
 manifest,_,digest=deploy.verify(folder,public)
 return deploy,manifest,digest

def confirmation(owner,device,bundle_sha,pc_now):
 assert 0<=pc_now-owner['confirmed_at_pc_unix']<=60,'New physical confirmation required'
 assert device['serial']=='80acfe79' and device['roundtrip_seconds']<10
 assert owner['physical_condition'] in ('P_zero_control_off','physically_CAN_disconnected_bench')
 assert isinstance(owner['exact_message'],str) and owner['exact_message'].strip()
 return {'kind':'physical-recovery-confirmation-v1','target_serial':device['serial'],'bundle_sha256':bundle_sha,
  'boot_id':device['boot_id'],'confirmed_at_unix':device['device_unix'],'confirmed_at_pc_unix':owner['confirmed_at_pc_unix'],
  'confirmed_device_boot_s':device['device_boot_s'],'expires_device_boot_s':device['device_boot_s']+120,
  'confirmed_by':owner['confirmed_by'],'physical_condition':owner['physical_condition'],
  'source_user_message_sha256':hashlib.sha256(owner['exact_message'].encode()).hexdigest()}

def sign(value,key):
 body=json.dumps(value,sort_keys=True,separators=(',',':')).encode()
 return json.dumps({'payload':base64.b64encode(body).decode(),'signature':base64.b64encode(key.sign(body)).decode()},sort_keys=True,separators=(',',':')).encode()

def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--folder',type=Path,required=True);parser.add_argument('--owner-confirmation',type=Path)
 parser.add_argument('--out',type=Path);parser.add_argument('--workspace',type=Path);parser.add_argument('--dry-run',action='store_true');args=parser.parse_args()
 O=Path(__file__).resolve().parent;W=(args.workspace or args.folder.resolve().parents[1]).resolve()
 import sys
 sys.path.insert(0,str(W/'can_inventory_work/lx3_original_diagnostics_20261023'))
 import access
 from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
 from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
 if not args.dry_run:
  assert args.owner_confirmation is not None and args.out is not None and not args.out.exists()
  assert not args.out.with_suffix('.private-audit.json').exists()
  owner=json.loads(args.owner_confirmation.read_text(encoding='utf8'))
  assert owner['confirmed_by']=='owner' and 0<=time.time()-owner['confirmed_at_pc_unix']<=60
 access.OUT=args.out.parent if args.out is not None else W/'can_inventory_work/lx3_warning_spi_candidates_20261025'
 access.OUT.mkdir(parents=True,exist_ok=True)
 public=base64.b64decode(json.loads((W/'update_ready_20260928/op_update_private/release-public.json').read_text())['public_key'],validate=True)
 deploy,manifest,digest=load_package(args.folder,public)
 before=time.monotonic()
 device=access.remote('''from pathlib import Path
import json,time
from openpilot.system.hardware import HARDWARE
assert HARDWARE.get_serial()=='80acfe79'
print('RESULT_JSON='+json.dumps({'serial':HARDWARE.get_serial(),'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
 'device_unix':time.time(),'device_boot_s':time.clock_gettime(time.CLOCK_BOOTTIME)}))
''','RECOVERY_DEVICE_CLOCK',timeout=10)
 device['roundtrip_seconds']=time.monotonic()-before
 if args.dry_run:
  assert device['roundtrip_seconds']<10
  print(json.dumps({'dry_run':True,'serial_verified':True,'package_verified':True,'pinned_installer_loaded':True,
   'device_clock_read':True,'token_created':False,'device_modified':False}));return
 key=Ed25519PrivateKey.from_private_bytes((W/'update_ready_20260928/op_update_private/release-signing.key').read_bytes())
 assert key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)==public
 token=confirmation(owner,device,digest,time.time())
 raw_token=sign(token,key)
 audit={'exact_owner_confirmation':owner,'device_observation':device,'token':token,'token_sha256':deploy.sha(raw_token),
  'package_release':manifest['release_id'],'services_started':False,'rebooted':False}
 with args.out.with_suffix('.private-audit.json').open('x',encoding='utf8') as stream:json.dump(audit,stream,ensure_ascii=False,indent=2)
 with args.out.open('xb') as stream:stream.write(raw_token)
 print(json.dumps({'issued':True,'token_path':str(args.out),'boot_time_validity_seconds':120,'device_modified':False}))

if __name__=='__main__':main()
