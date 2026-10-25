"""Pure signed-package/staging/rollback tests, never invokes installer main."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import os
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat

from tools import install_original_lx3_candidates as deploy


class CandidateDeploymentTests(unittest.TestCase):
  def setUp(self):
    if os.name=='nt':
      # Windows cannot open a directory with POSIX O_RDONLY. Linux CI exercises
      # the real directory fsync; do not weaken the Linux installer.
      mocked=patch.object(deploy,'fsync_directory');mocked.start();self.addCleanup(mocked.stop)

  def signed(self,folder,mutate=None):
    key=Ed25519PrivateKey.generate();public=key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
    files=[]
    for name in deploy.DIAG:
      data=b'inert = True\n';files.append({'path':name,'bytes':len(data),'sha256':deploy.sha(data),
        'data':base64.b64encode(data).decode(),'allowed_before':[None]})
    manifest={'release_id':next(iter(deploy.RELEASES)),'target_serial':deploy.SERIAL,
      'installer_sha256':deploy.sha(Path(deploy.__file__).read_bytes()),'files':files,
      'firmware_changed':False,'actuation_source_changed':False,'road_qualified':False,
      'baseline_native':deploy.BASE_NATIVE,'protected':deploy.PROTECTED}
    if mutate:mutate(manifest)
    body=json.dumps(manifest).encode()
    raw=json.dumps({'payload':base64.b64encode(body).decode(),'signature':base64.b64encode(key.sign(body)).decode()}).encode()
    (folder/'package.signed.json').write_bytes(raw)
    return public

  def test_signed_original_only_diagnostics_are_accepted(self):
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp);public=self.signed(folder)
      with patch.object(deploy,'PUBLIC_SHA',deploy.sha(public)):
        manifest,files,digest=deploy.verify(folder,public)
      self.assertEqual(set(files),set(deploy.DIAG));self.assertEqual(len(digest),64)
      self.assertFalse(manifest['road_qualified'])

  def test_ready_profile_other_serial_missing_files_and_changed_protection_rejected(self):
    mutations=(lambda m:m.update(release_id='lx3-original-spi-ready-20261025-01'),
      lambda m:m.update(target_serial='fb6d3f3f'),lambda m:m['files'].pop(),
      lambda m:m.update(protected={}),lambda m:m.update(firmware_changed=True))
    for mutate in mutations:
      with self.subTest(mutate=mutate),tempfile.TemporaryDirectory() as tmp:
        folder=Path(tmp);public=self.signed(folder,mutate)
        with patch.object(deploy,'PUBLIC_SHA',deploy.sha(public)),self.assertRaises(AssertionError):
          deploy.verify(folder,public)

  def test_payload_tampering_cannot_pass_signature(self):
    from cryptography.exceptions import InvalidSignature
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp);public=self.signed(folder)
      path=folder/'package.signed.json';envelope=json.loads(path.read_bytes())
      body=json.loads(base64.b64decode(envelope['payload']));body['target_serial']='fb6d3f3f'
      envelope['payload']=base64.b64encode(json.dumps(body).encode()).decode();path.write_text(json.dumps(envelope))
      with patch.object(deploy,'PUBLIC_SHA',deploy.sha(public)),self.assertRaises(InvalidSignature):
        deploy.verify(folder,public)

  def test_staged_actual_diagnostic_import_opens_no_can(self):
    root=Path(__file__).resolve().parents[3]
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp)
      for name in deploy.DIAG:deploy.atomic(folder/'new'/name,(root/name).read_bytes(),0o644)
      result=deploy.import_smoke(folder)
      self.assertTrue(result['inert_lss_capture']);self.assertFalse(result['CAN_TX'])

  def test_restore_validates_every_file_before_first_write(self):
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp);root=folder/'root';backup=folder/'tx';name=deploy.DIAG[0]
      deploy.atomic(root/name,b'new',0o644);deploy.atomic(backup/'before'/name,b'bad-backup',0o644)
      record={'files':{name:{'before':deploy.sha(b'old'),'sha256':deploy.sha(b'new'),'mode':0o644}}}
      with patch.object(deploy,'PROTECTED',{}),self.assertRaises(AssertionError):deploy.restore(root,backup,record)
      self.assertEqual((root/name).read_bytes(),b'new')

  def test_restore_and_foreign_change(self):
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp);root=folder/'root';backup=folder/'tx';name=deploy.DIAG[0]
      deploy.atomic(root/name,b'new',0o644);deploy.atomic(backup/'before'/name,b'old',0o644)
      record={'files':{name:{'before':deploy.sha(b'old'),'sha256':deploy.sha(b'new'),'mode':0o644}}}
      with patch.object(deploy,'PROTECTED',{}):deploy.restore(root,backup,record)
      self.assertEqual((root/name).read_bytes(),b'old')
      (root/name).write_bytes(b'foreign')
      with patch.object(deploy,'PROTECTED',{}),self.assertRaises(AssertionError):deploy.restore(root,backup,record)
      self.assertEqual((root/name).read_bytes(),b'foreign')

  def test_pending_ota_is_not_silently_overwritten(self):
    with tempfile.TemporaryDirectory() as tmp:
      state=Path(tmp)/'state.json';state.write_text('{"phase":"armed"}')
      with self.assertRaises(AssertionError):deploy.no_pending_ota(state)


if __name__=='__main__':unittest.main()
