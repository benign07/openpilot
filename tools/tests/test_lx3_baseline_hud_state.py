from pathlib import Path
import tempfile
import unittest
from openpilot.selfdrive.carrot.hud_update import core
from tools.lx3_baseline_hud_state import initialize_baseline


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


if __name__ == '__main__': unittest.main()
