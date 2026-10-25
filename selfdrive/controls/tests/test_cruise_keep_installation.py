"""Signed seven-file source scope and partial-layer restoration, without device IO."""
from pathlib import Path
import base64,importlib.util,json,os,subprocess
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat
ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('MAIN_installer_source',ROOT/'tools/install_lx3_cruise_keep_lateral_20261026.py')
deploy=importlib.util.module_from_spec(spec);spec.loader.exec_module(deploy)

def fixture(tmp_path,monkeypatch):
 folder=tmp_path/'package';folder.mkdir();root=tmp_path/'root';root.mkdir()
 git=lambda name:subprocess.check_output(['git','show','e617fa51:'+name],cwd=ROOT)
 (folder/'haptic_core.py').write_bytes(git('tools/install_original_lx3_haptics.py'))
 (folder/'feature_core.py').write_bytes(git('tools/install_original_lx3_feature_parity.py'))
 rows=[]
 for n in deploy.ORDER:
  data=(ROOT/n).read_bytes();old=git(n);rows.append({'path':n,'before':deploy.sha(old),'bytes':len(data),'sha256':deploy.sha(data),'data':base64.b64encode(data).decode()})
 key=Ed25519PrivateKey.generate();public=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
 monkeypatch.setattr(deploy,'PUBLIC_SHA',deploy.sha(public));config=tmp_path/'public.json';config.write_text(json.dumps({'public_key':base64.b64encode(public).decode()}))
 m={'release_id':deploy.RID,'target_serial':deploy.SERIAL,'car_fingerprint':'HYUNDAI_PALISADE_LX3_HEV','source_commit':'fixture','installer_sha256':deploy.sha(Path(deploy.__file__).read_bytes()),
  'support_sha256':{n:deploy.sha((folder/n).read_bytes()) for n in ('haptic_core.py','feature_core.py')},'files':rows,'protected':dict(deploy.FIXED_NATIVE)}
 def sign():
  raw=json.dumps(m).encode();(folder/'package.signed.json').write_text(json.dumps({'payload':base64.b64encode(raw).decode(),'signature':base64.b64encode(key.sign(raw)).decode()}))
 sign();return folder,root,config,m,sign,git

@pytest.mark.parametrize('change',['serial','path','missing_protected','wrong_native','support_tamper','payload_tamper'])
def test_signed_incremental_update_rejects_wrong_scope(tmp_path,monkeypatch,change):
 folder,root,config,m,sign,_=fixture(tmp_path,monkeypatch)
 deploy.verify(folder,root,config)
 if change=='serial':m['target_serial']='fb6d3f3f'
 elif change=='path':m['files'][0]['path']='../outside.py'
 elif change=='missing_protected':m['protected'].pop('selfdrive/pandad/pandad')
 elif change=='wrong_native':m['protected']['common/params_pyx.so']='0'*64
 elif change=='support_tamper':(folder/'haptic_core.py').write_text('#changed')
 else:m['files'][0]['data']=base64.b64encode(b'#different').decode()
 sign()
 with pytest.raises(Exception):deploy.verify(folder,root,config)
 assert not list(root.iterdir())

@pytest.mark.parametrize('prefix',[0,1,3,7])
@pytest.mark.skipif(os.name=='nt',reason='Actual Linux directory fsync during atomic restore')
def test_incremental_layer_restores_every_known_source_before_previous_feature_recovery(tmp_path,monkeypatch,prefix):
 folder,root,config,m,_,git=fixture(tmp_path,monkeypatch)
 _,files,_,core=deploy.verify(folder,root,config)
 # This unit proves source restoration only, not a device-native health check.
 core.PROTECTED={};record={'files':{},'preferences':{}}
 for n,row in files.items():
  old=git(n);core.atomic(root/n,old);core.atomic(folder/'before'/n,old)
  record['files'][n]={**{k:row[k] for k in ('before','sha256','bytes')},'mode':0o644}
 for n in deploy.ORDER[:prefix]:core.atomic(root/n,base64.b64decode(files[n]['data']))
 core.restore(root,folder,record,restore_preferences=False)
 assert all(deploy.sha((root/n).read_bytes())==row['before'] for n,row in files.items())
