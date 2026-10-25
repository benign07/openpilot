"""Issue only from a NEW exact owner message and freshly read SSH boot clock.

This prepares a short-lived file-restoration authorization, not an automatic
parked fallback. Never fabricate owner-confirmation or device-observation files.
"""
from pathlib import Path
import argparse,base64,hashlib,json,tempfile,time,types
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey,Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat

PIN='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
sha=lambda raw:hashlib.sha256(raw).hexdigest()

def confirmation(owner,device,bundle,now):
  assert owner['confirmed_by']=='owner' and 0<=now-owner['confirmed_at_pc_unix']<=60
  assert device['serial']=='80acfe79' and device['roundtrip_seconds']<10 and 0<=now-device['observed_at_pc_unix']<=10
  assert owner['physical_condition'] in ('P_zero_control_off','physically_CAN_disconnected_bench')
  assert isinstance(owner['exact_message'],str) and owner['exact_message'].strip()
  return {'kind':'physical-recovery-confirmation-v1','target_serial':'80acfe79','bundle_sha256':bundle,
    'boot_id':device['boot_id'],'confirmed_device_boot_s':device['device_boot_s'],
    'expires_device_boot_s':device['device_boot_s']+120,'confirmed_by':'owner',
    'confirmed_at_unix':device['device_unix'],'confirmed_at_pc_unix':owner['confirmed_at_pc_unix'],
    'physical_condition':owner['physical_condition'],'source_user_message_sha256':sha(owner['exact_message'].encode())}

def main():
  p=argparse.ArgumentParser(description=__doc__)
  for n in ('folder','owner-confirmation','device-observation','signing-key','out'):p.add_argument('--'+n,type=Path,required=True)
  a=p.parse_args();assert not a.out.exists()
  key=Ed25519PrivateKey.from_private_bytes(a.signing_key.read_bytes());public=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
  assert sha(public)==PIN
  envelope=json.loads((a.folder/'package.signed.json').read_bytes());body=base64.b64decode(envelope['payload'],validate=True)
  Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),body)
  manifest=json.loads(body);installer=(a.folder/'installer.py').read_bytes();assert sha(installer)==manifest['installer_sha256']
  module=types.ModuleType('pinned_feature_issuer_check');module.__file__=str(a.folder/'installer.py')
  exec(compile(installer,module.__file__,'exec'),module.__dict__);core,policy=module.support(a.folder)
  with tempfile.TemporaryDirectory(prefix='feature-proof-check-') as temp:
    config=Path(temp)/'public.json';config.write_text(json.dumps({'public_key':base64.b64encode(public).decode()}))
    _,_,bundle=module.verify(Path(temp)/'root',a.folder,core,policy,config)
  owner=json.loads(a.owner_confirmation.read_text());device=json.loads(a.device_observation.read_text())
  record=confirmation(owner,device,bundle,time.time());raw=json.dumps(record,sort_keys=True,separators=(',',':')).encode()
  token=json.dumps({'payload':base64.b64encode(raw).decode(),'signature':base64.b64encode(key.sign(raw)).decode()}).encode()
  with a.out.open('xb') as stream:stream.write(token)
  audit=a.out.with_suffix('.private-audit.json');assert not audit.exists()
  with audit.open('x',encoding='utf8') as stream:json.dump({'owner':owner,'device':device,'token':record,'token_sha256':sha(token)},stream,ensure_ascii=False,indent=2)
  print(json.dumps({'issued':True,'valid_for_device_boot_seconds':120,'vehicle_modified':False,'services_started':False}))

if __name__=='__main__':main()
