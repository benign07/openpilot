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
from openpilot.selfdrive.carrot.hud_update import boot_apply, core, service


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

  def test_every_permitted_runtime_source_requires_onroad_verification(self):
    for name in ('selfdrive/selfdrived/selfdrived.py', 'selfdrive/controls/controlsd.py',
                 'selfdrive/controls/lib/latcontrol.py', 'selfdrive/carrot/carrot_controls.py',
                 'selfdrive/carrot/server/services/settings.py',
                 'opendbc_repo/opendbc/car/hyundai/carstate.py'):
      self.assertTrue(service.requires_onroad_verification({'files': [{'path': name}]}), name)
    self.assertFalse(service.requires_onroad_verification({'files': [{'path': 'selfdrive/carrot/web/js/widget_pwa.js'}]}))
    self.assertTrue(service.requires_onroad_verification({'files': [{'path': 'selfdrive/carrot/web/runtime.py'}]}))

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

  def test_exact_control_sources_are_allowed_after_bootstrap_only(self):
    old = b'active = False\n'; new = b'active = True\n'
    changed = copy.deepcopy(self.release)
    changed['files'] = []
    for name in ('selfdrive/selfdrived/selfdrived.py', 'selfdrive/controls/controlsd.py',
                 'selfdrive/carrot/carrot_controls.py'):
      target = self.root / name
      target.parent.mkdir(parents=True, exist_ok=True)
      target.write_bytes(old)
      changed['files'].append({'path': name, 'before': core.sha(old), 'sha256': core.sha(new),
                               'bytes': len(new), 'data': base64.b64encode(new).decode()})
    raw, index = self.signed(changed)
    verified = core.verify_bundle(raw, index, self.public)
    core.stage(self.root, self.folder, verified)
    for row in changed['files']:
      self.assertEqual((self.root / row['path']).read_bytes(), old)
    core.save(self.folder / 'state.json', {'phase': 'armed', 'armed_at': 1000, 'armed_boot': 'old',
                                          'release': verified, 'previous_installed': {}})
    writes = 0
    def interrupted_write(path, data, mode):
      nonlocal writes
      core.atomic(path, data, mode)
      writes += 1
      if writes == 2:
        raise OSError('simulated interruption between control sources')
    with self.assertRaises(OSError):
      core.apply_at_boot(self.root, self.folder, 'new', 1020, write=interrupted_write)
    for row in changed['files']:
      self.assertEqual((self.root / row['path']).read_bytes(), old)
    for forbidden in ('selfdrive/selfdrived/state.py', 'selfdrive/selfdrived/tests/test_lx3_engagement.py',
                      'selfdrive/controls/controlsd2.py', 'selfdrive/carrot/hud_update/core.py'):
      with self.assertRaises(ValueError, msg=forbidden):
        core.checked_path(self.root, forbidden)

  def test_local_changes_and_stage_damage_leave_runtime_untouched(self):
    self.arm()
    target = self.root / self.release['files'][0]['path']; target.write_bytes(b'local = 3\n')
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'rejected')
    self.assertEqual(target.read_bytes(), b'local = 3\n')

  def test_modern_layout_has_the_same_signed_apply_and_restore_contract(self):
    changed = copy.deepcopy(self.release)
    for row in changed['files']:
      old = self.root / row['path']
      row['path'] = 'openpilot/' + row['path']
      target = self.root / row['path']
      target.parent.mkdir(parents=True, exist_ok=True)
      target.write_bytes(old.read_bytes())
      old.unlink()
    raw, index = self.signed(changed)
    release = core.verify_bundle(raw, index, self.public)
    core.stage(self.root, self.folder, release)
    core.save(self.folder / 'state.json', {'phase': 'armed', 'armed_at': 1000,
                                         'armed_boot': 'old', 'release': release,
                                         'previous_installed': {}})
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'applied')
    for row in release['files']:
      self.assertEqual((self.root / row['path']).read_bytes(), b'value = 2\n')
    # Recovery still restores exact originals when a transaction is interrupted.
    state = core.load(self.folder / 'state.json')
    core.rollback(self.root, self.folder, state)
    for row in release['files']:
      self.assertEqual((self.root / row['path']).read_bytes(), b'value = 1\n')

  def test_legacy_release_cannot_write_into_a_modern_only_tree(self):
    for row in self.release['files']:
      old = self.root / row['path']
      new = self.root / ('openpilot/' + row['path'])
      new.parent.mkdir(parents=True, exist_ok=True)
      new.write_bytes(old.read_bytes())
      old.unlink()
    raw, index = self.signed()
    release = core.verify_bundle(raw, index, self.public)
    with self.assertRaises(ValueError): core.stage(self.root, self.folder, release)
    for row in release['files']:
      self.assertEqual((self.root / ('openpilot/' + row['path'])).read_bytes(), b'value = 1\n')

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

  async def test_control_source_release_waits_for_real_onroad_health(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b'validated source\n')
    release = {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]}
    self.svc.state = {'phase': 'verifying', 'release': release, 'applied_at': service.time.time() - 300,
                      'message': '기기 시작 확인 중'}
    now = service.time.monotonic()
    fake_states = {'managerState': SimpleNamespace(processes=[SimpleNamespace(name=n, running=True)
                                                      for n in ('carrot_server', 'ui')]),
                   'deviceState': SimpleNamespace(started=False),
                   'carState': SimpleNamespace(canValid=True)}
    class FakeHealth(dict):
      def update(self, _):
        # This fixture represents publishers that remain healthy/fresh. CI disk
        # or event-loop delays must not age one static sample into a stale state.
        self.logMonoTime = {name: int(service.time.monotonic() * 1e9) for name in self}
    health = FakeHealth(fake_states)
    health.alive = health.valid = {'managerState': True, 'deviceState': True, 'carState': True}
    health.logMonoTime = {name: int(now * 1e9) for name in ('managerState', 'deviceState', 'carState')}
    self.svc.health_sm = health
    self.svc.state['release'] = {'files': [{'path': 'selfdrive/carrot/web/example.js'}]}
    self.assertTrue(await self.svc.healthy())  # Legacy source updates keep offroad verification.
    self.svc.state['release'] = release
    self.assertIsNone(await self.svc.healthy())
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.assertNotIn('onroad_verify_started_at', self.svc.state)

    health['deviceState'].started = True
    health['managerState'].processes = [SimpleNamespace(name=n, running=True, pid=i + 100) for i, n in enumerate(service.REQUIRED)]
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.svc.onroad_healthy_since = service.time.monotonic() - service.ONROAD_HEALTH_SECONDS - 1
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'complete')

  async def test_failed_onroad_control_source_health_warns_after_deadline(self):
    name = 'selfdrive/selfdrived/selfdrived.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(b'validated source\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]},
                      'applied_at': service.time.time() - 300}
    self.svc.healthy = AsyncMock(return_value=False)
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.assertIn('onroad_verify_started_at', self.svc.state)
    self.svc.state['onroad_verify_started_at'] = service.time.time() - 181
    self.svc.onroad_verify_started_mono = service.time.monotonic() - service.ONROAD_HEALTH_DEADLINE - 1
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'health_warning')

  async def test_stale_boot_and_long_offroad_wait_do_not_consume_onroad_deadline(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'validated source\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]},
                      'applied_at': service.time.time() - 3600}
    class FakeHealth(dict):
      def update(self, _):
        self.logMonoTime = {name: int(service.time.monotonic() * 1e9) for name in self}
    now = service.time.monotonic()
    health = FakeHealth(managerState=SimpleNamespace(processes=[]), deviceState=SimpleNamespace(started=False),
                        carState=SimpleNamespace(canValid=False))
    health.alive = health.valid = {'managerState': False, 'deviceState': False, 'carState': False}
    health.logMonoTime = {name: int(now * 1e9) for name in health}
    self.svc.health_sm = health
    await self.svc.tick()  # Initial SubMaster has not received deviceState.
    self.assertNotIn('onroad_verify_started_at', self.svc.state)
    health.alive = health.valid = {'managerState': True, 'deviceState': True, 'carState': False}
    await self.svc.tick()  # Parked for arbitrarily long after application.
    self.assertNotIn('onroad_verify_started_at', self.svc.state)
    health['deviceState'].started = True
    await self.svc.tick()  # First onroad tick is unhealthy, but gets a fresh 180s window.
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.assertIn('onroad_verify_started_at', self.svc.state)
    self.assertLess(service.time.monotonic() - self.svc.onroad_verify_started_mono, 1)
    self.svc.state['onroad_verify_started_at'] -= 10000  # RTC correction cannot consume the monotonic window.
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    health['deviceState'].started = False
    await self.svc.tick()
    self.assertNotIn('onroad_verify_started_at', self.svc.state)
    self.assertIsNone(self.svc.onroad_verify_started_mono)

  async def test_onroad_health_must_remain_good_across_process_restarts(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'validated source\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]}}
    class FakeHealth(dict):
      def update(self, _):
        self.logMonoTime = {name: int(service.time.monotonic() * 1e9) for name in self}
    now = service.time.monotonic()
    health = FakeHealth(managerState=SimpleNamespace(processes=[SimpleNamespace(name=n, running=True, pid=i + 100)
                                                       for i, n in enumerate(service.REQUIRED)]),
                        deviceState=SimpleNamespace(started=True), carState=SimpleNamespace(canValid=True))
    health.alive = health.valid = {name: True for name in health}
    health.logMonoTime = {name: int(now * 1e9) for name in health}
    self.svc.health_sm = health
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.svc.onroad_healthy_since = service.time.monotonic() - service.ONROAD_HEALTH_SECONDS - 1
    health['managerState'].processes[0].pid += 1
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.assertLess(service.time.monotonic() - self.svc.onroad_healthy_since, 1)
    health['carState'].canValid = False
    await self.svc.tick()
    self.assertIsNone(self.svc.onroad_healthy_since)
    health['carState'].canValid = True
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'verifying')
    self.svc.onroad_healthy_since = service.time.monotonic() - service.ONROAD_HEALTH_SECONDS - 1
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'complete')

  async def test_applied_file_hash_mismatch_warns_without_claiming_health(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'unexpected bytes\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(b'expected bytes\n')}]}}
    self.svc.healthy = AsyncMock(return_value=True)
    await self.svc.tick()
    self.svc.healthy.assert_not_awaited()
    self.assertEqual(self.svc.state['phase'], 'health_warning')

  async def test_static_web_release_stale_health_uses_original_offroad_deadline(self):
    name = 'selfdrive/carrot/web/js/example.js'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'const value = 2;\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]},
                      'applied_at': service.time.time() - service.ONROAD_HEALTH_DEADLINE - 1}
    self.svc.healthy = AsyncMock(return_value=None)
    await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'health_warning')

  async def test_late_first_healthy_sample_can_finish_bounded_stable_window(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'validated source\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]}}
    clock = [1000.0]
    fake_time = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: 2000.0 + clock[0])
    self.svc.health_identity = (('card', 100),)
    self.svc.healthy = AsyncMock(side_effect=[False, True, True, True])
    with patch.object(service, 'time', fake_time):
      await self.svc.tick()
      clock[0] = 1155.0
      await self.svc.tick()
      clock[0] = 1181.0
      await self.svc.tick()
      self.assertEqual(self.svc.state['phase'], 'verifying')
      clock[0] = 1186.0
      await self.svc.tick()
    self.assertEqual(self.svc.state['phase'], 'complete')

  async def test_restart_in_same_boot_keeps_onroad_failure_deadline(self):
    name = 'selfdrive/controls/controlsd.py'
    target = self.svc.root / name
    target.parent.mkdir(parents=True, exist_ok=True); target.write_bytes(b'validated source\n')
    self.svc.state = {'phase': 'verifying', 'release': {'files': [{'path': name, 'sha256': core.sha(target.read_bytes())}]}}
    clock = [1000.0]
    fake_time = SimpleNamespace(monotonic=lambda: clock[0], time=lambda: 2000.0 + clock[0])
    self.svc.healthy = AsyncMock(return_value=False)
    with patch.object(service, 'time', fake_time):
      await self.svc.tick()
      self.assertEqual(core.load(self.svc.folder / 'state.json')['onroad_verify_started_mono'], 1000.0)
      with patch.object(Path, 'read_text', autospec=True) as reader:
        real_read = Path.open
        def read(path, *args, **kwargs):
          if path.as_posix() == '/proc/sys/kernel/random/boot_id': return 'test-boot'
          with real_read(path, 'r', encoding='utf-8') as stream: return stream.read()
        reader.side_effect = read
        restarted = service.UpdateService({}, root=self.svc.root, state_root=self.svc.folder)
      self.assertEqual(restarted.onroad_verify_started_mono, 1000.0)
      class FakeHealth(dict):
        def update(self, _): pass
      health = FakeHealth()
      health.alive = health.valid = {'managerState': False, 'deviceState': False, 'carState': False}
      health.logMonoTime = {'managerState': 0, 'deviceState': 0, 'carState': 0}
      restarted.health_sm = health  # Fresh SubMaster still awaiting first publication.
      clock[0] = 1170.0
      await restarted.tick()
      self.assertEqual(restarted.onroad_verify_started_mono, 1000.0)
      self.assertEqual(restarted.health_mode, 'unknown')
      clock[0] = 1181.0
      await restarted.tick()
      self.assertEqual(restarted.state['phase'], 'health_warning')


class BootstrapIntegrationTests(unittest.TestCase):
  def test_boot_transaction_precedes_manager_and_overlay_has_config_guard(self):
    root = Path(__file__).resolve().parents[4]
    launch = (root / 'launch_chffrplus.sh').read_text(encoding='utf-8')
    self.assertLess(launch.index('hud_update/boot_apply.py'), launch.index('# start manager'))
    self.assertIn('[ ! -f /data/community/hud_updates/config.json ] && [ -f "${DIR}/.overlay_init" ]', launch)

  def test_update_api_registered_before_static_and_competing_updates_disabled(self):
    root = Path(__file__).resolve().parents[4]
    features = (root / 'openpilot/selfdrive/carrot/server/features/__init__.py').read_text(encoding='utf-8')
    self.assertLess(features.index('hud_update_service.register(app)'), features.index('static.register(app)'))
    from openpilot.selfdrive.carrot.server.services import auto_update
    with patch.object(auto_update.os.path, 'isfile', return_value=True):
      self.assertFalse(auto_update._auto_update_enabled())

  def test_native_and_schema_cannot_be_sent_as_a_source_only_update(self):
    for name in ('panda/board/obj/panda_h7.bin.signed', 'openpilot/cereal/log.capnp',
                 'openpilot/selfdrive/pandad/pandad', 'openpilot/selfdrive/carrot/hud_update/core.py'):
      with self.assertRaises(ValueError, msg=name):
        core.checked_path(Path('/unused'), name)


if __name__ == '__main__': unittest.main()
