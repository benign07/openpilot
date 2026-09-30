"""Physical stream integrity tests; these never enable vehicle control."""
from pathlib import Path
import ast
import runpy
import unittest

ROOT = Path(__file__).resolve().parents[3]
Input = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_inputs.py'))['Lx3ButtonInput']
ENV = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/crc.py'))
tree = ast.parse((ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py').read_text(encoding='utf-8'))
node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == 'hkg_can_fd_checksum')
exec(compile(ast.Module(body=[node], type_ignores=[]), 'checksum', 'exec'), ENV)
checksum = ENV['hkg_can_fd_checksum']


class TestPhysicalInputs(unittest.TestCase):
  def setUp(self):
    self.input = Input()
    self.now = 1_000_000_000

  def feed(self, counter, raw=0, corrupt=False, bus=0, step=40_000_000):
    self.now += step
    data = bytearray(16); data[2] = counter; data[10] = raw
    data[:2] = checksum(0x10B, None, data).to_bytes(2, 'little')
    if corrupt: data[5] ^= 1
    return self.input.update(0x10B, bus, data, self.now, self.now, checksum)

  def warmup(self):
    for c in (250, 252, 254): self.feed(c)
    self.assertTrue(self.input.ready)

  def test_observed_step_two_and_wrap(self):
    self.warmup()
    self.assertTrue(self.feed(0))
    self.assertTrue(self.feed(2))

  def test_step_one_is_not_accepted(self):
    self.warmup()
    self.assertFalse(self.feed(255))
    self.assertEqual(self.input.reason, 'sequence')

  def test_duplicate_does_not_refresh_or_release(self):
    self.warmup()
    stamp = self.input.received_ns
    self.assertFalse(self.feed(254, raw=8))
    self.assertEqual(self.input.received_ns, stamp)
    self.assertFalse(self.input.ready)

  def test_bad_crc_never_refreshes(self):
    self.warmup()
    stamp = self.input.received_ns
    self.assertFalse(self.feed(0, raw=128, corrupt=True))
    self.assertEqual(self.input.received_ns, stamp)

  def test_gap_requires_neutral_requalification(self):
    self.warmup()
    self.assertFalse(self.feed(0, step=240_000_000))
    self.assertFalse(self.feed(2, raw=8))
    for c in (4, 6, 8): self.feed(c)
    self.assertTrue(self.input.ready)

  def test_echo_cannot_create_a_button(self):
    self.warmup()
    self.assertFalse(self.feed(0, raw=128, bus=130))
    self.assertFalse(self.input.lfa)

  def test_held_startup_requires_neutral(self):
    for c in (0, 2, 4, 6): self.assertFalse(self.feed(c, raw=128))
    for c in (8, 10, 12): self.feed(c)
    self.assertTrue(self.input.ready)

  def test_no_data_expiry_cannot_synthesize_release(self):
    self.warmup()
    self.assertTrue(self.feed(0, raw=128))
    self.assertTrue(self.input.lfa)
    self.assertFalse(self.input.fresh(self.now + 200_000_001))
    self.assertFalse(self.input.ready)


if __name__ == '__main__':
  unittest.main()
