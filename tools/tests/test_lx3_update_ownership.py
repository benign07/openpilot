"""Exercise the real manager scheduler with stock and paired-update predicates."""
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch

from openpilot.system.manager.process import ensure_running
from openpilot.system.manager.process_config import enable_updated


class UpdateOwnershipTests(unittest.TestCase):
  def test_hud_pairing_prevents_repeated_stock_updater_starts(self):
    params = Mock(); params.get_bool.return_value = True
    process = SimpleNamespace(name='updated', enabled=True, proc=None, restart_if_crash=False,
                              should_run=enable_updated, start=Mock(), stop=Mock())
    with patch('openpilot.system.manager.process_config.os.path.isfile', return_value=True):
      for _ in range(3):
        self.assertEqual(ensure_running([process], False, params, SimpleNamespace()), [])
    process.start.assert_not_called()

  def test_unpaired_stock_offroad_menu_and_onroad_policy_survives(self):
    params = Mock()
    with patch('openpilot.system.manager.process_config.os.path.isfile', return_value=False):
      for started, menu, expected in ((False, True, True), (False, False, False), (True, True, False)):
        params.get_bool.return_value = menu
        self.assertEqual(enable_updated(started, params, SimpleNamespace()), expected)


if __name__ == '__main__': unittest.main()
