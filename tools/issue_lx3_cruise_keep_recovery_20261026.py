"""Issue from a NEW owner message and fresh SSH boot observation, never fabricate either."""
from pathlib import Path
import argparse,base64,hashlib,importlib.util,json,tempfile,time
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey,Ed25519PublicKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
PIN='56d7f915ed4d802bccf46db83f0d7b147579466ed060abdf1dab96759babd223'
sha=lambda b:hashlib.sha256(b).hexdigest()

def load(name,path):
 s=importlib.util.spec_from_file_location(name,path);m=importlib.util.module_from_spec(s);s.loader.exec_module(m);return m

def main():
 p=argparse.ArgumentParser(description=__doc__)
 for n in ('folder','owner-confirmation','device-observation','signing-key','out'):p.add_argument('--'+n,type=Path,required=True)
 a=p.parse_args();assert not a.out.exists()
 key=Ed25519PrivateKey.from_private_bytes(a.signing_key.read_bytes());public=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw);assert sha(public)==PIN
 raw_descriptor=(a.folder/'recovery/RECOVERY.signed.json').read_bytes();assert len(raw_descriptor)<65536
 envelope=json.loads(raw_descriptor);body=base64.b64decode(envelope['payload'],validate=True)
 Ed25519PublicKey.from_public_bytes(public).verify(base64.b64decode(envelope['signature'],validate=True),body)
 descriptor=json.loads(body)
 assert descriptor['kind']=='lx3-cruise-keep-source-recovery-v1' and descriptor['target_serial']=='80acfe79'
 assert descriptor['issuer_sha256']==sha(Path(__file__).read_bytes())
 assert descriptor['helper_sha256']==sha((a.folder/'recovery/manual_recovery.py').read_bytes())
 assert descriptor['installer_sha256']==sha((a.folder/'installer.py').read_bytes())
 assert descriptor['confirmation_core_sha256']==sha((a.folder/'recovery/confirmation_core.py').read_bytes())
 deploy=load('bound_MAIN_installer',a.folder/'installer.py')
 with tempfile.TemporaryDirectory() as d:
  config=Path(d)/'public.json';config.write_text(json.dumps({'public_key':base64.b64encode(public).decode()}))
  _,_,bundle,_=deploy.verify(a.folder,Path(d)/'root',config)
 assert bundle==descriptor['bundle_sha256']
 confirmation=load('bound_original_confirmation',a.folder/'recovery/confirmation_core.py')
 owner=json.loads(a.owner_confirmation.read_text());device=json.loads(a.device_observation.read_text())
 record=confirmation.confirmation(owner,device,bundle,time.time());raw=json.dumps(record,sort_keys=True,separators=(',',':')).encode()
 token=json.dumps({'payload':base64.b64encode(raw).decode(),'signature':base64.b64encode(key.sign(raw)).decode()}).encode()
 with a.out.open('xb') as f:f.write(token)
 with a.out.with_suffix('.private-audit.json').open('x',encoding='utf8') as f:json.dump({'owner':owner,'device':device,'token':record},f,ensure_ascii=False)
 print(json.dumps({'issued':True,'expires_after_boot_seconds':120,'services_started':False,'vehicle_modified':False}))

if __name__=='__main__':main()
