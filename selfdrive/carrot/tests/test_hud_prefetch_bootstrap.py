"""Separate bootstrap signature/scope/rollback tests with no system service I/O."""
import base64
import copy
import importlib.util
import json
from pathlib import Path
import tempfile
import types
import unittest
from unittest.mock import patch
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding,PublicFormat

ROOT=Path(__file__).resolve().parents[3]
spec=importlib.util.spec_from_file_location('hud_bootstrap',ROOT/'tools/install_hud_prefetch_20261029.py')
tool=importlib.util.module_from_spec(spec);spec.loader.exec_module(tool)


class BootstrapTests(unittest.TestCase):
  def setUp(self):
    self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
    p=Path(self.tmp.name);self.folder=p/'package';self.root=p/'root';self.state=p/'state'
    for x in (self.folder,self.root,self.state):x.mkdir()
    self.key=Ed25519PrivateKey.generate();public=self.key.public_key().public_bytes(Encoding.Raw,PublicFormat.Raw)
    patcher=patch.object(tool,'PUBLIC_SHA',tool.sha(public));patcher.start();self.addCleanup(patcher.stop)
    self.config={'public_key':base64.b64encode(public).decode(),'token':'keep-token','phone_ip':'127.0.0.1'}
    (self.state/'config.json').write_text(json.dumps(self.config))
    (self.state/'state.json').write_text('{"phase":"complete"}')
    self.old=b'VALUE = 1\n';self.new=b'class UpdateService:\n  def prefetch_tick(self): pass\n'
    self.files={}
    for name,blob in zip(tool.ORDER,(b'VALUE=2\n',self.new)):
      path=self.root/name;path.parent.mkdir(parents=True,exist_ok=True)
      before=None
      if name.endswith('/service.py'):path.write_bytes(self.old);before=tool.sha(self.old)
      self.files[name]={'before':before,'sha256':tool.sha(blob),'bytes':len(blob),'data':base64.b64encode(blob).decode()}
    protected={}
    for name in tool.PROTECTED:
      p=self.root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(b'VALUE=1\n');protected[name]=tool.digest(p)
    helper=self.folder/'parked_atomic.py'
    helper.write_text('''import json,time\nfrom pathlib import Path\ndef atomic(path,raw,mode=0o600):\n path=Path(path);path.parent.mkdir(parents=True,exist_ok=True);path.write_bytes(raw)\ndef save(path,value):atomic(path,json.dumps(value).encode())\ndef parked():return {'mono':time.monotonic(),'continuous_seconds':10}\ndef stopped():pass\n''')
    self.manifest={'schema':1,'kind':'HudPrefetchBootstrap','installer_sha256':tool.sha(Path(tool.__file__).read_bytes()),
                   'helper_sha256':tool.sha(helper.read_bytes()),'files':self.files,'protected':protected,'automatic_enabled':True}
    self.sign()

  def sign(self):
    raw=json.dumps(self.manifest).encode()
    (self.folder/'bootstrap.signed.json').write_text(json.dumps({'payload':base64.b64encode(raw).decode(),'signature':base64.b64encode(self.key.sign(raw)).decode()}))

  def verify(self):return tool.verify(self.folder,self.root,self.state)

  def stage(self):
    m,c,b=self.verify();tool.stage(self.folder,self.root,self.state,m,c,b);return m,c,b

  def modules(self):
    params=types.ModuleType('openpilot.common.params');params.Params=lambda:types.SimpleNamespace(get=lambda _:b'carparams')
    class CP:
      def __enter__(self):return types.SimpleNamespace(carFingerprint='HYUNDAI_PALISADE_LX3_HEV')
      def __exit__(self,*_):pass
    cereal=types.ModuleType('cereal');cereal.car=types.SimpleNamespace(CarParams=types.SimpleNamespace(from_bytes=lambda _:CP()))
    return {'openpilot.common.params':params,'cereal':cereal}

  def apply(self,m,c,b,restore=False):
    with patch.dict('sys.modules',self.modules()),patch.object(tool,'smoke'),patch.object(tool,'updater_stopped'),patch.object(tool.subprocess,'run') as run:
      result=tool.transaction(self.folder,self.root,self.state,m,c,b,restore)
    self.assertEqual(run.call_count,2)
    return result

  def test_signature_and_fixed_two_file_scope_reject_before_any_runtime_write(self):
    self.verify();self.manifest['files']['selfdrive/carrot/hud_update/core.py']=self.files[tool.ORDER[0]];self.sign()
    with self.assertRaises(ValueError):self.verify()
    self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.old)

  def test_staging_does_not_create_dependency_in_runtime_and_rejects_active_update(self):
    self.stage();self.assertFalse((self.root/tool.ORDER[0]).exists())
    self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.old)
    (self.state/'state.json').write_text('{"phase":"armed"}')
    with self.assertRaises(ValueError):tool.idle_state(self.state)

  def test_fresh_parked_gate_failure_does_not_stop_services(self):
    m,c,b=self.stage()
    with patch.object(c,'parked',side_effect=RuntimeError('not parked')),patch.object(tool,'smoke'),patch.object(tool.subprocess,'run') as run:
      with self.assertRaises(RuntimeError):tool.transaction(self.folder,self.root,self.state,m,c,b)
      run.assert_not_called()
    self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.old)

  def test_install_and_restore_preserve_configuration_and_installed_receipts(self):
    m,c,b=self.stage();before=(self.state/'config.json').read_bytes();state=(self.state/'state.json').read_bytes()
    self.apply(m,c,b);self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.new)
    self.assertTrue(json.loads((self.state/'automatic.json').read_bytes())['enabled'])
    self.assertEqual((self.state/'config.json').read_bytes(),before);self.assertEqual((self.state/'state.json').read_bytes(),state)
    self.apply(m,c,b,True);self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.old)
    self.assertFalse((self.root/tool.ORDER[0]).exists());self.assertFalse((self.state/'automatic.json').exists())

  def test_partial_write_failure_restores_importer_and_removes_new_dependency(self):
    m,c,b=self.stage();atomic=c.atomic
    def fail(path,raw,mode=0o600):
      atomic(path,raw,mode)
      if Path(path)==self.root/tool.ORDER[1] and raw==self.new:raise OSError('interrupted')
    with patch.object(c,'atomic',side_effect=fail),patch.dict('sys.modules',self.modules()),patch.object(tool,'smoke'),patch.object(tool,'updater_stopped'),patch.object(tool.subprocess,'run'):
      with self.assertRaises(OSError):tool.transaction(self.folder,self.root,self.state,m,c,b)
    self.assertEqual((self.root/tool.ORDER[1]).read_bytes(),self.old);self.assertFalse((self.root/tool.ORDER[0]).exists())
    self.assertEqual(json.loads((self.folder/'transaction.json').read_bytes())['phase'],'restored_after_failure')

  def test_staged_record_cannot_redefine_signed_before_hashes(self):
    m,c,b=self.stage();p=self.folder/'transaction.json';d=json.loads(p.read_bytes());d['files'][tool.ORDER[1]]['before']='0'*64;p.write_text(json.dumps(d))
    with self.assertRaises(ValueError):tool.transaction(self.folder,self.root,self.state,m,c,b)

  def test_user_policy_change_after_staging_is_not_overwritten(self):
    m,c,b=self.stage();(self.state/'automatic.json').write_text('{"schema":1,"enabled":false}')
    with patch.object(tool,'smoke'),patch.object(tool.subprocess,'run') as run:
      with self.assertRaises(ValueError):tool.transaction(self.folder,self.root,self.state,m,c,b)
      run.assert_not_called()

  def test_actual_subprocess_import_smoke_checks_staged_dependency_and_service(self):
    m,c,b=self.stage();tool.smoke(self.folder,self.root,m)
    (self.folder/'new'/tool.ORDER[1]).write_bytes(b'raise RuntimeError("import failed")\n')
    with self.assertRaises(Exception):tool.smoke(self.folder,self.root,m)

  def test_foreign_protected_boot_source_is_rejected(self):
    (self.root/tool.PROTECTED[0]).write_bytes(b'foreign')
    with self.assertRaises(ValueError):self.verify()
