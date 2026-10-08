from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from openpilot.selfdrive.carrot.hud_update import core
from tools.lx3_baseline_hud_state import initialize_baseline, restore_baseline


class TestBaselineState(unittest.TestCase):
  def setUp(self):
    self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
    self.root = Path(self.tmp.name)
    core.save(self.root / 'config.json', {'pairing': 'test-placeholder'})
    core.save(self.root / 'installed.json', {'release_id': 'legacy-1', 'sequence': 41})

  def test_archive_retains_pairing_floor_and_cannot_advertise_old_native_rollback(self):
    core.save(self.root / 'state.json', {'phase': 'complete'})
    core.save(self.root / 'latest.json', {'release_id': 'legacy-2'})
    before = (self.root / 'config.json').read_bytes()
    result = initialize_baseline(self.root, 'a' * 40, 'b' * 64)
    self.assertEqual((self.root / 'config.json').read_bytes(), before)
    self.assertEqual(result['installed']['sequence'], 41)
    self.assertFalse((self.root / 'latest.json').exists())
    self.assertTrue((Path(result['archive']) / 'installed.json').is_file())
    with self.assertRaises(ValueError):
      core.prepare_rollback(Path('/unused'), self.root, result['installed'], 'unused')
    self.assertFalse(initialize_baseline(self.root, 'a' * 40, 'b' * 64)['changed'])

  def test_active_and_unknown_transactions_are_not_erased(self):
    for phase in ('armed', 'applying', 'rolling_back', 'verifying', 'waiting_parked', 'unknown'):
      core.save(self.root / 'state.json', {'phase': phase})
      before = (self.root / 'installed.json').read_bytes()
      with self.assertRaises(ValueError): initialize_baseline(self.root, 'a' * 40, 'b' * 64)
      self.assertEqual((self.root / 'installed.json').read_bytes(), before)
      self.assertEqual(core.load(self.root / 'state.json')['phase'], phase)

  def test_interrupted_move_cannot_lose_floor_and_can_restore_before_root_rollback(self):
    core.save(self.root / 'state.json', {'phase': 'cancelled'})
    original_replace = Path.replace
    class PowerLoss(BaseException): pass
    def cut_after_installed_move(path, target):
      result = original_replace(path, target)
      if path == self.root / 'installed.json': raise PowerLoss()
      return result
    with patch.object(Path, 'replace', cut_after_installed_move):
      with self.assertRaises(PowerLoss): initialize_baseline(self.root, 'a' * 40, 'b' * 64)
    self.assertFalse((self.root / 'installed.json').exists())
    self.assertEqual(core.load(self.root / 'baseline-migration.json')['preserved_sequence'], 41)
    with self.assertRaises(ValueError): initialize_baseline(self.root, 'a' * 40, 'b' * 64)
    self.assertTrue(restore_baseline(self.root))
    self.assertEqual(core.load(self.root / 'installed.json')['release_id'], 'legacy-1')
    self.assertEqual(core.load(self.root / 'installed.json')['sequence'], 41)
    self.assertEqual(core.load(self.root / 'state.json')['phase'], 'cancelled')
    self.assertFalse(restore_baseline(self.root))
    self.assertEqual(initialize_baseline(self.root, 'a' * 40, 'b' * 64)['installed']['sequence'], 41)

  def test_completed_migration_can_be_restored_with_old_updater_state(self):
    core.save(self.root / 'state.json', {'phase': 'complete', 'old': True})
    core.save(self.root / 'latest.json', {'release_id': 'legacy-2'})
    initialize_baseline(self.root, 'a' * 40, 'b' * 64)
    installed = core.load(self.root / 'installed.json')
    installed['sequence'] = 50
    core.save(self.root / 'installed.json', installed)
    self.assertTrue(restore_baseline(self.root))
    self.assertEqual(core.load(self.root / 'installed.json')['release_id'], 'legacy-1')
    self.assertEqual(core.load(self.root / 'installed.json')['sequence'], 50)
    self.assertEqual(core.load(self.root / 'latest.json')['release_id'], 'legacy-2')
    self.assertTrue(core.load(self.root / 'state.json')['old'])


if __name__ == '__main__': unittest.main()
