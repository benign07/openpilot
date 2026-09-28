import asyncio
import base64
import copy
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, AsyncMock

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from selfdrive.carrot.hud_update import boot_apply, core, service


class ReleaseTests(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
    self.root = Path(self.tmp.name) / 'repo'; self.folder = Path(self.tmp.name) / 'state'
    self.folder.mkdir(); self.private = Ed25519PrivateKey.generate()
    self.public = base64.b64encode(self.private.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)).decode()
    self.release = {'schema': 1, 'release_id': 'test-1', 'sequence': 1, 'notes': ['변경점'],
                    'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV', 'source_commit': 'a' * 40, 'files': []}
    for i in range(2):
      name = f'selfdrive/carrot/example{i}.py'
      original = b'value = 1\n'; new = b'value = 2\n'
      target = self.root / name; target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(original)
      self.release['files'].append({'path': name, 'before': core.sha(original), 'sha256': core.sha(new),
                                    'bytes': len(new), 'data': base64.b64encode(new).decode()})

  def signed(self, release=None):
    release = release or self.release; payload = core.canonical(release)
    raw = core.canonical({'payload': base64.b64encode(payload).decode(), 'signature': base64.b64encode(self.private.sign(payload)).decode()})
    index = {k: release[k] for k in ('schema', 'release_id', 'sequence', 'notes')}
    index.update(bundle_commit='b' * 40, bundle_sha256=core.sha(raw), bundle_bytes=len(raw))
    return raw, index

  def arm(self):
    raw, index = self.signed(); release = core.verify_bundle(raw, index, self.public)
    core.stage(self.root, self.folder, release)
    state = {'phase': 'armed', 'armed_at': 1000, 'armed_boot': 'old', 'release': release, 'previous_installed': {}}
    core.save(self.folder / 'state.json', state)
    return state

  def test_signed_release_staged_without_touching_runtime_then_applied_at_boot(self):
    self.arm()
    self.assertEqual((self.root / self.release['files'][0]['path']).read_text(), 'value = 1\n')
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'applied')
    self.assertEqual((self.root / self.release['files'][0]['path']).read_text(), 'value = 2\n')
    self.assertEqual(core.load(self.folder / 'state.json')['phase'], 'verifying')

  def test_tampering_signature_hash_and_notes_are_rejected(self):
    raw, index = self.signed()
    with self.assertRaises(ValueError): core.verify_bundle(raw + b' ', index, self.public)
    altered = copy.deepcopy(self.release); altered['notes'] = ['unreviewed']
    second, idx = self.signed(altered); idx['notes'] = ['different displayed notes']
    with self.assertRaises(ValueError): core.verify_bundle(second, idx, self.public)
    other = Ed25519PrivateKey.generate().public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
    with self.assertRaises(Exception): core.verify_bundle(raw, index, base64.b64encode(other).decode())

  def test_old_release_duplicate_path_escape_and_updater_self_change_rejected(self):
    raw, index = self.signed()
    with self.assertRaises(ValueError): core.verify_bundle(raw, index, self.public, 1)
    for path in ('../x.py', '/data/a.py', 'selfdrive/carrot/../../x.py', 'selfdrive/carrot/hud_update/core.py', 'selfdrive/carrot/a.so'):
      changed = copy.deepcopy(self.release); changed['files'][0]['path'] = path
      data, idx = self.signed(changed)
      with self.assertRaises(ValueError, msg=path): core.verify_bundle(data, idx, self.public)
    changed = copy.deepcopy(self.release); changed['files'].append(changed['files'][0])
    data, idx = self.signed(changed)
    with self.assertRaises(ValueError): core.verify_bundle(data, idx, self.public)

  def test_local_changes_and_stage_damage_leave_runtime_untouched(self):
    self.arm()
    target = self.root / self.release['files'][0]['path']; target.write_bytes(b'local = 3\n')
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'rejected')
    self.assertEqual(target.read_bytes(), b'local = 3\n')

  def test_mid_transaction_error_rolls_back_every_file(self):
    self.arm(); count = 0
    def fail(path, data, mode):
      nonlocal count
      core.atomic(path, data, mode); count += 1
      if count == 2: raise OSError('simulated power/write failure')
    with self.assertRaises(OSError): core.apply_at_boot(self.root, self.folder, 'new', 1020, write=fail)
    for row in self.release['files']: self.assertEqual(core.sha((self.root / row['path']).read_bytes()), row['before'])
    self.assertEqual(core.load(self.folder / 'state.json')['phase'], 'rolled_back')
    self.assertEqual(core.load(self.folder / 'installed.json'), {})

  def test_interrupted_boot_transaction_recovers_before_launch(self):
    state = self.arm(); state['phase'] = 'applying'; core.save(self.folder / 'state.json', state)
    (self.root / self.release['files'][0]['path']).write_bytes(b'value = 2\n')
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'rolled_back')
    self.assertEqual((self.root / self.release['files'][0]['path']).read_bytes(), b'value = 1\n')

  def test_corrupt_backup_stops_recovery(self):
    state = self.arm(); state['phase'] = 'applying'; core.save(self.folder / 'state.json', state)
    backup = self.folder / 'releases/test-1/original' / self.release['files'][0]['path']; backup.write_bytes(b'bad')
    with self.assertRaises(ValueError): core.apply_at_boot(self.root, self.folder, 'new', 1020)

  def test_same_boot_or_stale_parked_authorization_cannot_install(self):
    for boot, now in [('old', 1020), ('new', 1300), ('new', 999)]:
      self.arm()
      self.assertEqual(core.apply_at_boot(self.root, self.folder, boot, now), 'deferred')
      self.assertEqual((self.root / self.release['files'][0]['path']).read_bytes(), b'value = 1\n')
      self.assertEqual(core.load(self.folder/'state.json')['phase'], 'failed')

  def test_unsynchronized_boot_preserves_originals_and_stops_retrying(self):
    self.arm()
    with patch.object(boot_apply, 'wait_for_synchronized_clock', return_value=False), patch.object(core, 'apply_at_boot') as apply:
      self.assertEqual(boot_apply.apply_with_clock(self.root, self.folder), 'clock_unavailable')
      apply.assert_not_called()
    self.assertEqual(core.load(self.folder/'state.json')['phase'], 'failed')
    self.assertEqual((self.root/self.release['files'][0]['path']).read_bytes(), b'value = 1\n')

  def test_interrupted_transaction_recovers_without_network_clock(self):
    state = self.arm(); state['phase'] = 'applying'; core.save(self.folder/'state.json', state)
    (self.root/self.release['files'][0]['path']).write_bytes(b'value = 2\n')
    with patch.object(boot_apply, 'wait_for_synchronized_clock') as clock:
      self.assertEqual(boot_apply.apply_with_clock(self.root, self.folder), 'rolled_back')
      clock.assert_not_called()
    self.assertEqual((self.root/self.release['files'][0]['path']).read_bytes(), b'value = 1\n')


class BootClockTests(unittest.TestCase):
  def test_waits_for_current_boot_sync_marker_with_monotonic_deadline(self):
    elapsed = [0]
    def sleep(seconds): elapsed[0] += seconds
    marker = SimpleNamespace(is_file=lambda: elapsed[0] >= 3)
    self.assertTrue(boot_apply.wait_for_synchronized_clock(marker, 5, lambda: elapsed[0], sleep))
    self.assertEqual(elapsed[0], 3)

  def test_missing_sync_marker_times_out(self):
    elapsed = [0]
    def sleep(seconds): elapsed[0] += seconds
    marker = SimpleNamespace(is_file=lambda: False)
    self.assertFalse(boot_apply.wait_for_synchronized_clock(marker, 5, lambda: elapsed[0], sleep))
    self.assertEqual(elapsed[0], 5)


class ApiTests(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
    folder = Path(self.tmp.name)
    core.save(folder / 'config.json', {'phone_ip': '127.0.0.1', 'token': 'x' * 43, 'public_key': ''})
    with patch.object(Path, 'read_text', autospec=True) as reader:
      real_read = Path.open
      def read(path, *args, **kwargs):
        if path.as_posix() == '/proc/sys/kernel/random/boot_id': return 'test-boot'
        with real_read(path, 'r', encoding='utf-8') as f: return f.read()
      reader.side_effect = read
      self.svc = service.UpdateService({}, root=folder/'repo', state_root=folder)
    self.svc.latest = {'schema': 1, 'release_id': 'test-1', 'sequence': 1, 'notes': ['notes'], 'bundle_commit': 'b'*40,
                       'bundle_sha256': 'a'*64, 'bundle_bytes': 100}
    app = web.Application(); app['hud_update_service'] = self.svc
    app.router.add_post('/api/hud_update/action', service.action); app.router.add_get('/api/hud_update/status', service.status)
    self.client = TestClient(TestServer(app)); await self.client.start_server()
    self.headers = {'Authorization': 'Bearer ' + 'x'*43}

  async def asyncTearDown(self): await self.client.close()

  async def post(self, body): return await self.client.post('/api/hud_update/action', json=body, headers=self.headers)

  async def queue(self):
    return await self.post({'action': 'queue', 'release_id': 'test-1', 'bundle_sha256': 'a'*64})

  async def test_auth_required_and_no_credentials_in_response(self):
    response = await self.client.get('/api/hud_update/status')
    self.assertEqual(response.status, 401)
    response = await self.client.get('/api/hud_update/status', headers=self.headers)
    self.assertNotIn('x'*43, await response.text())

  async def test_queue_while_moving_never_downloads_writes_or_reboots(self):
    response = await self.queue(); self.assertEqual(response.status, 200)
    self.svc.parked = AsyncMock(return_value=False)
    with patch.object(service, 'fetch') as fetch, patch.object(service.asyncio, 'create_subprocess_exec') as reboot:
      await self.svc.tick(); await self.svc.tick()
      fetch.assert_not_called(); reboot.assert_not_called()
    self.assertEqual(self.svc.state['phase'], 'waiting_parked')

  async def test_duplicate_queue_is_idempotent_and_cancel_survives_restart(self):
    await self.queue(); await self.queue()
    self.assertEqual(len(self.svc.history), 1)
    response = await self.post({'action': 'cancel'}); self.assertEqual(response.status, 200)
    self.assertEqual(core.load(self.svc.folder/'state.json')['phase'], 'cancelled')

  async def test_changed_release_refuses_queue(self):
    response = await self.post({'action': 'queue', 'release_id': 'new-unseen', 'bundle_sha256': 'a'*64})
    self.assertEqual(response.status, 409)

  async def test_leaving_park_during_countdown_returns_to_wait_without_reboot(self):
    await self.queue(); self.svc.state['phase'] = 'countdown'
    self.svc.parked = AsyncMock(return_value=False)
    with patch.object(service.asyncio, 'create_subprocess_exec') as reboot:
      await self.svc.tick(); reboot.assert_not_called()
    self.assertEqual(self.svc.state['phase'], 'waiting_parked')

  async def test_state_change_during_download_prevents_staging(self):
    await self.queue()
    self.svc.parked = AsyncMock(side_effect=[True, False])
    self.svc.vehicle_matches = lambda: True
    self.svc.parked_since = service.time.monotonic() - 20
    with patch.object(service, 'fetch', return_value=b'release'), patch.object(core, 'verify_bundle', return_value={'files': []}), patch.object(core, 'stage') as stage:
      await self.svc.tick()
      stage.assert_not_called()
    self.assertEqual(self.svc.state['phase'], 'waiting_parked')

  async def test_state_change_at_final_reboot_gate_never_arms(self):
    await self.queue(); self.svc.state.update(phase='countdown', release={'files': []})
    self.svc.parked = AsyncMock(side_effect=[True, False]); self.svc.vehicle_matches = lambda: True
    self.svc.parked_since = self.svc.countdown_since = service.time.monotonic()-20
    with patch.object(core, 'validate_staged'), patch.object(service.asyncio, 'create_subprocess_exec') as reboot:
      await self.svc.tick(); reboot.assert_not_called()
    self.assertEqual(self.svc.state['phase'], 'waiting_parked')


class RecorderFieldsTests(unittest.TestCase):
  def test_recorded_params_exist_and_required_context_is_present(self):
    from selfdrive.carrot.can_diagnostics.automatic_runtime import PARAMS, FIELDS
    keys = (Path(__file__).resolve().parents[3] / 'common/params_keys.h').read_text(encoding='utf-8')
    for name in PARAMS: self.assertIn('"'+name+'"', keys, name)
    self.assertIn('seatbeltUnlatched', FIELDS['carState'])
    self.assertIn('myDrivingMode', FIELDS['longitudinalPlan'])
    self.assertIn('tFollow', FIELDS['longitudinalPlan'])


if __name__ == '__main__': unittest.main()
