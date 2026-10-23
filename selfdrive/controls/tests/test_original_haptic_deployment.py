"""Offline signature rejection and source/native rollback, no device process I/O."""
from pathlib import Path
import base64,hashlib,importlib.util,json,os
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('original_haptic_installer',ROOT/'tools/install_original_lx3_haptics.py')
deploy=importlib.util.module_from_spec(spec);spec.loader.exec_module(deploy)


def fixture(tmp_path,monkeypatch):
 root=tmp_path/'root';root.mkdir();stage=tmp_path/'stage';stage.mkdir()
 private=Ed25519PrivateKey.generate();pub=private.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
 monkeypatch.setattr(deploy,'PUBLIC_SHA',deploy.sha(pub))
 config=tmp_path/'config.json';config.write_text(json.dumps({'public_key':base64.b64encode(pub).decode()}))
 files=[]
 for name in deploy.ORDER:
  raw=b'# fixture\n' if name.endswith('.py') else b'fixture'
  files.append({'path':name,'before':None,'data':base64.b64encode(raw).decode(),'sha256':deploy.sha(raw),'bytes':len(raw)})
 values={r['path']:r for r in files}
 manifest={'release_id':deploy.RID,'target_serial':deploy.SERIAL,
  'installer_sha256':deploy.sha(Path(deploy.__file__).read_bytes()),'files':files,
  'native_build':{'key_count':317,'original316_descriptors_preserved':True,'ABI_subset_of_original_backup':True,
    'module_sha256':values['common/params_pyx.so']['sha256'],'registry_header_sha256':values['common/params_keys.h']['sha256']}}
 def sign():
  payload=json.dumps(manifest).encode()
  (stage/'package.signed.json').write_text(json.dumps({'payload':base64.b64encode(payload).decode(),
    'signature':base64.b64encode(private.sign(payload)).decode()}))
 sign();return root,stage,config,manifest,sign


@pytest.mark.parametrize('mutation',['serial','missing_native','unknown_path','native_digest','header_digest','installer_digest','unsigned_tamper'])
def test_invalid_signed_or_unpaired_package_rejected_before_any_install(tmp_path,monkeypatch,mutation):
 root,stage,config,manifest,sign=fixture(tmp_path,monkeypatch)
 deploy.verify(root,stage,config)
 if mutation=='serial':manifest['target_serial']='fb6d3f3f'
 elif mutation=='missing_native':manifest['files']=[r for r in manifest['files'] if r['path']!='common/params_pyx.so']
 elif mutation=='unknown_path':manifest['files'][0]['path']='../escape.py'
 elif mutation=='native_digest':manifest['native_build']['module_sha256']='0'*64
 elif mutation=='header_digest':manifest['native_build']['registry_header_sha256']='0'*64
 elif mutation=='installer_digest':manifest['installer_sha256']='0'*64
 else:
  value=json.loads((stage/'package.signed.json').read_text());raw=bytearray(base64.b64decode(value['payload']));raw[0]^=1
  value['payload']=base64.b64encode(raw).decode();(stage/'package.signed.json').write_text(json.dumps(value))
 if mutation!='unsigned_tamper':sign()
 with pytest.raises(Exception):deploy.verify(root,stage,config)
 assert not list(root.iterdir())


@pytest.mark.parametrize('failure_after',range(1,8))
@pytest.mark.skipif(os.name=='nt',reason='Linux directory fsync and rollback are exercised on Linux CI')
def test_reverse_restore_recovers_every_partial_native_source_prefix(tmp_path,failure_after):
 root=tmp_path/'root';root.mkdir();stage=tmp_path/'stage';prefs=tmp_path/'prefs';prefs.mkdir()
 record={'files':{},'preferences':{'OpenpilotEnabledToggle':base64.b64encode(b'0').decode(),
  'C3xLiteHapticNotifications':None}}
 for index,name in enumerate(deploy.ORDER):
  old=None if index==0 else ('original:'+name).encode();new=('updated:'+name).encode()
  record['files'][name]={'before':deploy.sha(old) if old is not None else None,'sha256':deploy.sha(new),'mode':0o644}
  if old is not None:
   deploy.atomic(root/name,old);deploy.atomic(stage/'before'/name,old)
 for name in deploy.ORDER[:failure_after]:deploy.atomic(root/name,('updated:'+name).encode())
 deploy.atomic(prefs/'OpenpilotEnabledToggle',b'1');deploy.atomic(prefs/'C3xLiteHapticNotifications',b'1')
 deploy.restore(root,stage,record,prefs)
 for index,name in enumerate(deploy.ORDER):
  if index==0:assert not (root/name).exists()
  else:assert (root/name).read_bytes()==('original:'+name).encode()
 assert (prefs/'OpenpilotEnabledToggle').read_bytes()==b'0'
 assert not (prefs/'C3xLiteHapticNotifications').exists()


@pytest.mark.skipif(os.name=='nt',reason='Linux directory fsync semantics verified on Linux CI')
def test_rollback_does_not_clobber_foreign_change_or_unattempted_preferences(tmp_path):
 root=tmp_path/'root';root.mkdir();stage=tmp_path/'stage';prefs=tmp_path/'prefs';prefs.mkdir()
 record={'files':{},'preferences':{'OpenpilotEnabledToggle':base64.b64encode(b'0').decode()}}
 for index,name in enumerate(deploy.ORDER):
  old=None if index==0 else ('old:'+name).encode();new=('new:'+name).encode()
  record['files'][name]={'before':deploy.sha(old) if old is not None else None,'sha256':deploy.sha(new),'mode':0o644}
  if old is not None:deploy.atomic(root/name,old);deploy.atomic(stage/'before'/name,old)
 deploy.atomic(root/deploy.ORDER[0],('new:'+deploy.ORDER[0]).encode())
 deploy.atomic(root/deploy.ORDER[3],b'foreign concurrent change')
 deploy.atomic(prefs/'OpenpilotEnabledToggle',b'1')
 with pytest.raises(AssertionError):deploy.restore(root,stage,record,prefs,restore_preferences=False)
 assert (root/deploy.ORDER[3]).read_bytes()==b'foreign concurrent change'
 assert (root/deploy.ORDER[0]).read_bytes()==('new:'+deploy.ORDER[0]).encode()
 assert (prefs/'OpenpilotEnabledToggle').read_bytes()==b'1'
 deploy.atomic(root/deploy.ORDER[3],('old:'+deploy.ORDER[3]).encode())
 deploy.restore(root,stage,record,prefs,restore_preferences=False)
 assert not (root/deploy.ORDER[0]).exists()
 assert (prefs/'OpenpilotEnabledToggle').read_bytes()==b'1'
