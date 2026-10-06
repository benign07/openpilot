import copy
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, patch

from selfdrive.carrot.hud_update import core, service
from selfdrive.carrot.tests import test_hud_updater as updater_fixture


class RollbackTests(unittest.TestCase):
  setUp = updater_fixture.ReleaseTests.setUp
  signed = updater_fixture.ReleaseTests.signed

  def install(self):
    raw, index = self.signed()
    release = core.verify_bundle(raw, index, self.public)
    directory = core.stage(self.root, self.folder, release)
    previous = {'release_id': 'baseline-66ef65fe', 'sequence': 0,
                'source_commit': 'b' * 40, 'notes': ['previous source']}
    core.atomic(directory / 'signed_bundle.json', raw)
    core.save(directory / 'index.json', index)
    core.save(directory / 'previous_installed.json', previous)
    core.save(self.folder / 'state.json', {'phase': 'armed', 'armed_boot': 'old', 'armed_at': 1000,
                                         'release': release, 'previous_installed': previous})
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'new', 1020), 'applied')
    return core.load(self.folder / 'installed.json'), directory

  def test_explicit_restore_is_read_only_until_new_boot_and_keeps_sequence_floor(self):
    installed, _ = self.install()
    reverse = core.prepare_rollback(self.root, self.folder, installed, self.public)
    self.assertEqual(reverse['sequence'], installed['sequence'])
    self.assertEqual(reverse['restored_release_id'], 'baseline-66ef65fe')
    self.assertTrue(all((self.root/r['path']).read_text() == 'value = 2\n' for r in reverse['files']))
    core.stage(self.root, self.folder, reverse)
    core.save(self.folder / 'state.json', {'phase': 'armed', 'armed_boot': 'new', 'armed_at': 2000,
                                         'release': reverse, 'previous_installed': installed})
    self.assertEqual(core.apply_at_boot(self.root, self.folder, 'restored', 2020), 'applied')
    actual = core.load(self.folder / 'installed.json')
    self.assertEqual(actual['rollback_of'], installed['release_id'])
    self.assertEqual(actual['sequence'], installed['sequence'])
    self.assertTrue(all((self.root/r['path']).read_text() == 'value = 1\n' for r in reverse['files']))
    with self.assertRaises(ValueError): core.prepare_rollback(self.root, self.folder, actual, self.public)
    raw, index = self.signed()
    with self.assertRaises(ValueError): core.verify_bundle(raw, index, self.public, actual['sequence'])

  def test_damaged_backup_or_changed_current_source_never_prepares_restore(self):
    installed, directory = self.install()
    target = self.root / self.release['files'][0]['path']
    target.write_bytes(b'unrelated source\n')
    with self.assertRaises(ValueError): core.prepare_rollback(self.root, self.folder, installed, self.public)
    self.assertEqual(target.read_bytes(), b'unrelated source\n')
    target.write_bytes(b'value = 2\n')
    backup = directory / 'original' / self.release['files'][1]['path']
    backup.write_bytes(b'damaged backup\n')
    with self.assertRaises(ValueError): core.prepare_rollback(self.root, self.folder, installed, self.public)
    self.assertEqual(target.read_bytes(), b'value = 2\n')

  def test_signed_release_and_installed_identity_are_required(self):
    installed, directory = self.install()
    mismatch = copy.deepcopy(installed); mismatch['source_commit'] = 'c' * 40
    with self.assertRaises(ValueError): core.prepare_rollback(self.root, self.folder, mismatch, self.public)
    original = (directory / 'signed_bundle.json').read_bytes()
    core.atomic(directory / 'signed_bundle.json', original + b' ')
    with self.assertRaises(ValueError): core.prepare_rollback(self.root, self.folder, installed, self.public)
    self.assertTrue(all((self.root/r['path']).read_text() == 'value = 2\n' for r in self.release['files']))

  def test_interrupted_restore_recovers_current_version_and_metadata(self):
    installed, _ = self.install()
    reverse = core.prepare_rollback(self.root, self.folder, installed, self.public)
    core.stage(self.root, self.folder, reverse)
    core.save(self.folder / 'state.json', {'phase': 'armed', 'armed_boot': 'new', 'armed_at': 2000,
                                         'release': reverse, 'previous_installed': installed})
    def fail_after_write(path, data, mode):
      core.atomic(path, data, mode)
      raise OSError('simulated power loss')
    with self.assertRaises(OSError):
      core.apply_at_boot(self.root, self.folder, 'restore', 2020, write=fail_after_write)
    self.assertEqual(core.load(self.folder / 'installed.json'), installed)
    self.assertTrue(all((self.root/r['path']).read_text() == 'value = 2\n' for r in reverse['files']))


class RollbackApiTests(unittest.IsolatedAsyncioTestCase):
  asyncSetUp = updater_fixture.ApiTests.asyncSetUp
  asyncTearDown = updater_fixture.ApiTests.asyncTearDown
  post = updater_fixture.ApiTests.post

  async def test_restore_while_moving_is_only_a_queue_and_can_be_cancelled(self):
    core.save(self.svc.folder / 'installed.json', {'release_id': 'current-1', 'sequence': 1})
    reverse = {'release_id': 'rollback-current-1', 'sequence': 1, 'notes': ['restore'],
               'files': [], 'restored_release_id': 'previous-1'}
    with patch.object(core, 'prepare_rollback', return_value=reverse):
      response = await self.post({'action': 'rollback', 'release_id': 'current-1'})
      self.assertEqual(response.status, 200)
    self.svc.parked = AsyncMock(return_value=False)
    with patch.object(core, 'stage') as stage, patch.object(service, 'fetch') as fetch, \
         patch.object(service.asyncio, 'create_subprocess_exec') as reboot:
      await self.svc.tick()
      stage.assert_not_called(); fetch.assert_not_called(); reboot.assert_not_called()
    self.assertEqual(self.svc.state['phase'], 'waiting_parked')
    response = await self.post({'action': 'cancel'})
    self.assertEqual(response.status, 200)
    self.assertEqual(self.svc.state['phase'], 'cancelled')

  async def test_changed_installed_identity_and_corrupt_backup_reject_before_queue(self):
    core.save(self.svc.folder / 'installed.json', {'release_id': 'current-2', 'sequence': 2})
    with patch.object(core, 'prepare_rollback') as prepare:
      response = await self.post({'action': 'rollback', 'release_id': 'current-1'})
      self.assertEqual(response.status, 409); prepare.assert_not_called()
    with patch.object(core, 'prepare_rollback', side_effect=ValueError('bad backup')):
      response = await self.post({'action': 'rollback', 'release_id': 'current-2'})
      self.assertEqual(response.status, 409)
    self.assertEqual(self.svc.state['phase'], 'idle')


if __name__ == '__main__': unittest.main()
