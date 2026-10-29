"""Real signed cache and updater state transitions; no vehicle/network commands."""
import asyncio
import copy
import json
import threading
from pathlib import Path
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock,patch

from aiohttp import web
from aiohttp.test_utils import TestClient,TestServer
from selfdrive.carrot.hud_update import core,prefetch,service
from selfdrive.carrot.tests.test_hud_updater import ReleaseTests


class PrefetchTests(unittest.IsolatedAsyncioTestCase):
  signed=ReleaseTests.signed

  async def asyncSetUp(self):
    ReleaseTests.setUp(self)
    self.raw,self.index=self.signed()
    core.save(self.folder/'config.json',{'phone_ip':'127.0.0.1','token':'x'*43,'public_key':self.public})
    read=Path.read_text
    def read_boot(path,*args,**kwargs):
      return 'test-boot' if path.as_posix()=='/proc/sys/kernel/random/boot_id' else read(path,*args,**kwargs)
    with patch.object(Path,'read_text',read_boot):
      self.svc=service.UpdateService({},self.root,self.folder)
    self.svc.latest=self.index
    self.svc.vehicle_matches=lambda:True
    self.svc.wifi=AsyncMock(return_value=True)
    self.svc.parked=AsyncMock(return_value=False)
    app=web.Application();app['hud_update_service']=self.svc
    app.router.add_post('/api/hud_update/action',service.action)
    self.client=TestClient(TestServer(app));await self.client.start_server()
    self.headers={'Authorization':'Bearer '+'x'*43}

  async def asyncTearDown(self):await self.client.close()

  async def post(self,command,**extra):
    return await self.client.post('/api/hud_update/action',json={'action':command,**extra},headers=self.headers)

  async def queue(self):
    response=await self.post('queue',release_id=self.index['release_id'],bundle_sha256=self.index['bundle_sha256'])
    self.assertEqual(response.status,200)

  def fetch(self,url,limit):
    return core.canonical(self.index) if url==core.CHANNEL else self.raw

  def unchanged(self):
    for row in self.release['files']:
      self.assertEqual(core.sha((self.root/row['path']).read_bytes()),row['before'])

  async def prepare(self,automatic=False):
    if automatic:await self.post('automatic',enabled=True)
    else:await self.queue()
    with patch.object(service,'fetch',side_effect=self.fetch):await self.svc.prefetch_tick()

  async def test_default_off_never_accesses_network_or_queues(self):
    with patch.object(service,'fetch') as fetch:
      await self.svc.prefetch_tick();fetch.assert_not_called()
    self.assertEqual(self.svc.state['phase'],'idle')
    self.assertFalse(self.svc.public()['automatic']['enabled'])

  async def test_manual_moving_request_prefetches_but_never_stages_or_reboots(self):
    with patch.object(core,'stage') as stage,patch.object(service.asyncio,'create_subprocess_exec') as reboot:
      await self.prepare();await self.svc.tick()
      stage.assert_not_called();reboot.assert_not_called()
    self.assertTrue(self.svc.prefetch_state['ready']);self.unchanged()
    self.assertFalse((self.folder/'releases').exists())

  async def test_device_auto_prepares_without_any_queue_request_and_keeps_pinned_identity(self):
    await self.prepare(automatic=True)
    self.assertTrue(self.svc.state['automatic'])
    self.assertEqual(self.svc.state['index'],self.index)
    self.assertTrue(self.svc.public()['automatic']['ready']);self.unchanged()

  async def test_cached_bundle_installs_preparation_offline_only_after_fresh_park(self):
    await self.prepare();self.svc.parked=AsyncMock(return_value=True)
    self.svc.parked_since=service.time.monotonic()-11
    with patch.object(service,'fetch') as fetch:
      await self.svc.tick();fetch.assert_not_called()
    self.assertEqual(self.svc.state['phase'],'countdown');self.unchanged()
    self.assertTrue((self.folder/'releases/test-1/original').is_dir())

  async def test_auto_cold_start_never_reboots_until_this_boot_has_driven(self):
    await self.prepare(automatic=True);self.svc.parked=AsyncMock(return_value=True)
    self.svc.parked_since=service.time.monotonic()-50
    with patch.object(core,'stage') as stage:
      await self.svc.tick();stage.assert_not_called()
    self.svc.auto_driven=True;self.svc.parked_since=service.time.monotonic()-11
    await self.svc.tick();self.assertEqual(self.svc.state['phase'],'countdown')

  async def test_explicit_manual_request_can_take_over_a_cold_auto_reservation(self):
    await self.prepare(automatic=True);await self.queue()
    self.assertFalse(self.svc.state['automatic'])
    self.svc.parked=AsyncMock(return_value=True);self.svc.parked_since=service.time.monotonic()-11
    await self.svc.tick();self.assertEqual(self.svc.state['phase'],'countdown')

  async def test_cancel_during_network_fetch_does_not_hold_lock_or_resurrect_request(self):
    await self.queue();entered=threading.Event();finish=threading.Event()
    def blocked(*args):entered.set();finish.wait(5);return self.raw
    with patch.object(service,'fetch',side_effect=blocked):
      work=asyncio.create_task(self.svc.prefetch_tick())
      self.assertTrue(await asyncio.to_thread(entered.wait,2))
      self.assertFalse(self.svc.lock.locked())
      response=await self.post('cancel');self.assertEqual(response.status,200)
      finish.set();await work
    self.assertEqual(self.svc.state['phase'],'cancelled')
    self.assertFalse(self.svc.cache.folder.exists());self.unchanged()

  async def test_cancel_while_cache_commit_holds_lock_cannot_queue_after_await(self):
    await self.post('automatic',enabled=True);entered=threading.Event();finish=threading.Event()
    # Start as manual pending: cancellation is available throughout download.
    await self.queue()
    def blocked(*args):entered.set();finish.wait(5)
    with patch.object(service,'fetch',side_effect=self.fetch),patch.object(self.svc.cache,'save',side_effect=blocked):
      work=asyncio.create_task(self.svc.prefetch_tick())
      self.assertTrue(await asyncio.to_thread(entered.wait,2));self.assertTrue(self.svc.lock.locked())
      self.assertEqual((await self.post('cancel')).status,200)
      finish.set();await work
    self.assertEqual(self.svc.state['phase'],'cancelled');self.assertIsNone(self.svc.prefetched_digest)

  async def test_cancel_during_final_validation_never_arms_or_reboots(self):
    await self.prepare();self.svc.parked=AsyncMock(return_value=True)
    self.svc.parked_since=service.time.monotonic()-11;await self.svc.tick()
    self.svc.countdown_since=service.time.monotonic()-11
    entered=threading.Event();finish=threading.Event()
    def blocked(*args):entered.set();finish.wait(5)
    with patch.object(core,'validate_staged',side_effect=blocked),patch.object(service.asyncio,'create_subprocess_exec') as reboot:
      work=asyncio.create_task(self.svc.tick());self.assertTrue(await asyncio.to_thread(entered.wait,2))
      self.assertEqual((await self.post('cancel')).status,200)
      finish.set();await work;reboot.assert_not_called()
    self.assertEqual(self.svc.state['phase'],'cancelled');self.unchanged()

  async def test_cancel_marker_prevents_restart_rearming_if_phase_write_was_interrupted(self):
    await self.prepare();identity=self.svc.request_identity()
    self.svc.save_auto_policy(cancelled_request=identity,suppressed_bundle=self.index['bundle_sha256'])
    self.svc.parked=AsyncMock(return_value=True)
    with patch.object(core,'stage') as stage:await self.svc.tick();stage.assert_not_called()
    self.assertEqual(self.svc.state['phase'],'cancelled')

  async def test_latest_changes_do_not_replace_a_manual_selected_bundle(self):
    await self.queue();other=copy.deepcopy(self.index);other['release_id']='test-2';other['bundle_sha256']='d'*64
    self.svc.latest=other
    with patch.object(service,'fetch',side_effect=self.fetch) as fetch:
      await self.svc.prefetch_tick()
    self.assertEqual(fetch.call_args.args[0],core.bundle_url(self.index))
    self.assertEqual(self.svc.state['index'],self.index)

  async def test_source_change_after_download_prevents_staging(self):
    await self.prepare();(self.root/self.release['files'][0]['path']).write_bytes(b'local change\n')
    self.svc.parked=AsyncMock(return_value=True);self.svc.parked_since=service.time.monotonic()-11
    with patch.object(core,'stage') as stage:
      with self.assertRaises(ValueError):await self.svc.tick()
      stage.assert_not_called()

  async def test_corrupt_cache_waits_and_never_stages(self):
    await self.prepare();(self.svc.cache.folder/(self.index['bundle_sha256']+'.json')).write_bytes(b'bad')
    self.svc.parked=AsyncMock(return_value=True);self.svc.parked_since=service.time.monotonic()-11
    with patch.object(core,'stage') as stage:await self.svc.tick();stage.assert_not_called()
    self.assertEqual(self.svc.state['phase'],'waiting_parked');self.assertIsNone(self.svc.prefetched_digest)

  async def test_cached_signed_bytes_cannot_be_reinterpreted_under_changed_pinned_metadata(self):
    await self.prepare();other=copy.deepcopy(self.index);other['notes']=['different displayed release']
    self.assertIsNone(self.svc.cache.load(other,self.public,0))
    other=copy.deepcopy(self.index);other['release_id']='different-id'
    self.assertIsNone(self.svc.cache.load(other,self.public,0))
    self.assertIsNone(self.svc.cache.load(self.index,self.public,1))

  async def test_prefetch_retry_backoff_survives_wifi_flapping(self):
    await self.post('automatic',enabled=True)
    self.svc.prefetch_due=service.time.monotonic()+900
    self.svc.prefetch_not_before=service.time.monotonic()+60
    self.svc.wifi_was_connected=False
    with patch.object(service,'fetch') as fetch:
      await self.svc.prefetch_tick();fetch.assert_not_called()

  async def test_cancelled_sequence_stays_skipped_after_later_cancel_and_restart_policy_load(self):
    await self.prepare(automatic=True);await self.post('cancel')
    self.assertEqual(core.load(self.folder/'automatic.json')['skipped_sequence'],1)
    self.svc.state={'phase':'cancelled'};self.svc.prefetch_due=0.;self.svc.prefetch_not_before=0.
    with patch.object(service,'fetch',side_effect=self.fetch) as fetch:
      await self.svc.prefetch_tick()
    self.assertEqual(fetch.call_count,1);self.assertEqual(self.svc.state['phase'],'cancelled')

  async def test_completed_parked_countdown_reboots_only_after_validation_and_final_gate(self):
    await self.prepare();self.svc.parked=AsyncMock(return_value=True)
    self.svc.parked_since=service.time.monotonic()-11;await self.svc.tick()
    self.svc.countdown_since=service.time.monotonic()-11
    process=NS(wait=AsyncMock(return_value=0))
    with patch.object(service.asyncio,'create_subprocess_exec',AsyncMock(return_value=process)) as reboot:
      await self.svc.tick();reboot.assert_awaited_once()
    self.assertEqual(self.svc.state['phase'],'armed');self.unchanged()

  async def test_wifi_required_for_auto_but_explicit_manual_request_is_independent(self):
    await self.post('automatic',enabled=True);self.svc.wifi=AsyncMock(return_value=False)
    with patch.object(service,'fetch') as fetch:await self.svc.prefetch_tick();fetch.assert_not_called()
    await self.prepare();self.assertTrue(self.svc.prefetch_state['ready'])

  async def test_disabling_auto_cancels_automatic_but_not_manual_pending_request(self):
    await self.prepare(automatic=True);await self.post('automatic',enabled=False)
    self.assertEqual(self.svc.state['phase'],'cancelled')
    await self.queue();await self.post('automatic',enabled=False)
    self.assertEqual(self.svc.state['phase'],'waiting_parked')

  async def test_invalid_toggle_type_is_rejected_without_enabling(self):
    self.assertEqual((await self.post('automatic',enabled=1)).status,400)
    self.assertFalse(self.svc.public()['automatic']['enabled'])

  async def test_park_poll_exception_is_transient_and_leaves_request_pending(self):
    await self.prepare();self.svc.parked=AsyncMock(side_effect=RuntimeError('temporary'))
    await self.svc.tick();self.assertEqual(self.svc.state['phase'],'waiting_parked')

  async def test_health_failure_or_active_rollback_is_never_automatically_overwritten(self):
    self.svc.save_auto_policy(enabled=True)
    for phase in ('failed','health_warning','verifying','rolling_back','rolled_back','armed','applying'):
      self.svc.state={'phase':phase}
      with patch.object(service,'fetch') as fetch:await self.svc.prefetch_tick();fetch.assert_not_called()


class WiFiTests(unittest.TestCase):
  def test_requires_fresh_valid_wifi_and_never_accepts_cellular(self):
    sm=type('SM',(dict,),{})(deviceState=NS(networkType='wifi'))
    sm.alive=sm.valid={'deviceState':True};sm.logMonoTime={'deviceState':100_000_000_000}
    self.assertTrue(prefetch.wifi_connected(sm,101.))
    self.assertFalse(prefetch.wifi_connected(sm,104.))
    sm['deviceState'].networkType='cell4G';self.assertFalse(prefetch.wifi_connected(sm,101.))

  def test_drip_download_has_a_total_deadline_and_size_limit(self):
    clock=[0.]
    class Response:
      def __enter__(self):return self
      def __exit__(self,*_):pass
      def geturl(self):return core.RAW+'pinned/file.json'
      def read1(self,n):clock[0]+=8.;return b'x'
    fake=NS(monotonic=lambda:clock[0])
    with patch.object(service,'time',fake),patch.object(service,'urlopen',return_value=Response()):
      with self.assertRaisesRegex(ValueError,'deadline'):service.fetch(core.RAW+'pinned/file.json',1024)
    self.assertLessEqual(clock[0],30.)

  def test_cache_cleanup_only_removes_other_content_addressed_blobs(self):
    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
      cache=prefetch.BundleCache(Path(tmp));cache.folder.mkdir()
      (cache.folder/('a'*64+'.json')).write_bytes(b'old')
      (cache.folder/'user-profile.json').write_bytes(b'preserve')
      raw=b'new';digest=core.sha(raw);cache.save({'bundle_sha256':digest},raw)
      self.assertFalse((cache.folder/('a'*64+'.json')).exists())
      self.assertEqual((cache.folder/'user-profile.json').read_bytes(),b'preserve')
