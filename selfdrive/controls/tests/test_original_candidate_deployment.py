"""Inert signed-package/staging/rollback tests; no real service, CAN or device."""
import base64
import hashlib
import json
from pathlib import Path
import tempfile
import unittest
import os
import sys
import time
import subprocess
from types import SimpleNamespace
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
      'source_commit':'c'*40,
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
      state=Path(tmp)/'state.json'
      with self.assertRaises(AssertionError):deploy.no_pending_ota(state)
      for phase in ('armed','downloading','verifying','countdown','waiting_parked',None):
        state.write_text(json.dumps({'phase':phase}))
        with self.assertRaises(AssertionError):deploy.no_pending_ota(state)
      state.write_text('{"phase":"complete"}');deploy.no_pending_ota(state)

  def run_inert_main(self,folder,root,public,phase,run=None):
    config=folder/'config.json';config.write_text(json.dumps({'public_key':base64.b64encode(public).decode()}))
    state=folder/'ota.json';state.write_text('{"phase":"complete"}')
    load=deploy.public_from_config;ota=deploy.no_pending_ota;read_text=Path.read_text
    def read(path,*args,**kwargs):
      if path.as_posix().endswith('/proc/sys/kernel/random/boot_id'):return 'inert-boot'
      return read_text(path,*args,**kwargs)
    hardware=SimpleNamespace(HARDWARE=SimpleNamespace(get_serial=lambda:deploy.SERIAL))
    with patch.dict(sys.modules,{'openpilot.system.hardware':hardware}), \
      patch.object(sys,'argv',['installer',phase,'--folder',str(folder),'--root',str(root)]), \
      patch.object(deploy,'public_from_config',side_effect=lambda:load(config)), \
      patch.object(deploy,'no_pending_ota',side_effect=lambda:ota(state)), \
      patch.object(deploy,'import_smoke',return_value={'inert':True}), \
      patch.object(deploy,'parked',return_value={'mono':time.monotonic(),'continuous_seconds':10}), \
      patch.object(Path,'read_text',read),patch.object(deploy.subprocess,'run',side_effect=run), \
      patch.object(deploy.subprocess,'Popen') as reboot:
      try:deploy.main()
      finally:reboot.assert_not_called()

  def test_main_reads_actual_public_key_config_and_stop_failure_preserves_runtime(self):
    with tempfile.TemporaryDirectory() as tmp:
      folder=Path(tmp)/'tx';folder.mkdir();root=Path(tmp)/'root';root.mkdir()
      (root/'prebuilt').touch();baseline=b'original-native'
      (root/'selfdrive/pandad').mkdir(parents=True);(root/'selfdrive/pandad/pandad').write_bytes(baseline)
      with patch.object(deploy,'BASE_NATIVE',deploy.sha(baseline)),patch.object(deploy,'PROTECTED',{}):
        public=self.signed(folder)
        with patch.object(deploy,'PUBLIC_SHA',deploy.sha(public)):
          self.run_inert_main(folder,root,public,'stage')
          self.assertEqual(json.loads((folder/'transaction.json').read_text())['phase'],'staged')
          calls=[]
          def fail_stop(args,**kwargs):
            calls.append(args)
            if 'stop' in args:raise subprocess.CalledProcessError(1,args)
            return subprocess.CompletedProcess(args,0)
          with self.assertRaises(subprocess.CalledProcessError):self.run_inert_main(folder,root,public,'install',fail_stop)
          record=json.loads((folder/'transaction.json').read_text())
          self.assertEqual(record['phase'],'aborted_before_write')
          self.assertFalse(record['runtime_modified']);self.assertIn('start',calls[-1])
          self.assertFalse((root/deploy.DIAG[0]).exists())
          self.assertEqual((root/'selfdrive/pandad/pandad').read_bytes(),baseline)

  def test_transaction_paths_are_bound_back_to_signed_files(self):
    files={name:{'allowed_before':[None],'sha256':'a'*64,'bytes':1} for name in deploy.DIAG}
    manifest={'release_id':next(iter(deploy.RELEASES)),'source_commit':'c'*40}
    record={'bundle_sha256':'d'*64,**manifest,'files':{name:{'before':None,'sha256':'a'*64,'bytes':1,'mode':0o644} for name in files}}
    deploy.validate_record(manifest,files,record,'d'*64)
    record['files']['system/launch.sh']=record['files'][deploy.DIAG[0]]
    with self.assertRaises(AssertionError):deploy.validate_record(manifest,files,record,'d'*64)


if __name__=='__main__':unittest.main()
