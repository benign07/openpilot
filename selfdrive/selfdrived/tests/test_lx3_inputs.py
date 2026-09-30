"""Physical stream integrity tests; these never enable vehicle control."""
from pathlib import Path
import ast
import runpy
import unittest

ROOT = Path(__file__).resolve().parents[3]
Input = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_inputs.py'))['Lx3ButtonInput']
Intent = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_inputs.py'))['Lx3ButtonIntent']
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


class TestPhysicalGestures(unittest.TestCase):
  def setUp(self):
    self.intent = Intent()
    self.now = 1_000_000_000
    self.counter = 248
    for _ in range(3): self.feed()

  def feed(self, raw=0, corrupt=False, step=40_000_000, bus=0):
    self.now += step
    self.counter = (self.counter + 2) % 256
    data = bytearray(16); data[2] = self.counter; data[10] = raw
    data[:2] = checksum(0x10B, None, data).to_bytes(2, 'little')
    if corrupt: data[0] ^= 1
    return self.intent.update(0x10B, bus, data, self.now, self.now, checksum)

  def test_lfa_press_release_and_main_time_debounce(self):
    self.assertEqual(self.feed(128), [('lfaButton', True)])
    self.assertEqual(self.feed(), [('lfaButton', False)])
    self.assertEqual(self.feed(8), [('mainCruise', True)])
    for _ in range(7): self.assertEqual(self.feed(), [])
    self.assertEqual(self.feed(), [('mainCruise', False)])

  def test_main_flicker_is_one_gesture(self):
    self.feed(8)
    for _ in range(5):
      self.assertEqual(self.feed(), [])
      self.assertEqual(self.feed(8), [])
    for _ in range(7): self.assertEqual(self.feed(), [])
    self.assertEqual(self.feed(), [('mainCruise', False)])

  def test_cancel_over_simultaneous_lfa_and_pending_main(self):
    self.feed(8)
    self.assertEqual(self.feed(132), [('cancel', True)])
    self.assertEqual(self.feed(), [('cancel', False)])
    for _ in range(10): self.assertEqual(self.feed(), [])

  def test_corrupt_release_cannot_enable_after_recovery(self):
    self.feed(128)
    self.assertEqual(self.feed(corrupt=True), [])
    for _ in range(5): self.assertEqual(self.feed(), [])
    self.assertTrue(self.intent.fresh(self.now))

  def test_stale_and_held_recovery_does_not_enable(self):
    self.feed(8)
    self.assertEqual(self.feed(step=240_000_000), [])
    for _ in range(4): self.assertEqual(self.feed(8), [])
    for _ in range(10): self.assertEqual(self.feed(), [])

  def test_echo_ambiguous_input_and_res_set(self):
    self.assertEqual(self.feed(128, bus=130), [])
    # Ignored echo does not advance the physical counter; recover from the gap.
    for _ in range(4): self.feed()
    self.assertEqual(self.feed(129), [])
    for _ in range(3): self.feed()
    self.assertEqual(self.feed(1), [('accelCruise', True)])
    self.assertEqual(self.feed(), [('accelCruise', False)])
    self.assertEqual(self.feed(2), [('decelCruise', True)])
    self.assertEqual(self.feed(), [('decelCruise', False)])


class TestPhysicalParser(unittest.TestCase):
  def setUp(self):
    clock = runpy.run_path(str(ROOT / 'selfdrive/carrot/tests/test_lx3_can_time.py'))
    env = clock['ENV']
    self.parser = env['CANParser'](str(ROOT / 'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc'), [], 0)
    self.parser.raw_capture = {0x10B}
    self.intent = Intent()
    self.ns = 1_000_000_000
    self.counter = 248

  def frame(self, raw=0, corrupt=False, bus=0):
    self.ns += 40_000_000
    self.counter = (self.counter + 2) % 256
    data = bytearray(16); data[2] = self.counter; data[10] = raw
    data[:2] = checksum(0x10B, None, data).to_bytes(2, 'little')
    if corrupt: data[0] ^= 1
    return [self.ns, [(0x10B, bytes(data), bus)]]

  def feed(self, *frames):
    self.parser.update(list(frames))
    return self.intent.from_parser(self.parser, checksum)

  def warmup(self):
    for _ in range(3): self.feed(self.frame())

  def test_actual_parser_preserves_physical_press_release_once(self):
    self.warmup()
    events, ready = self.feed(self.frame(128), self.frame())
    self.assertTrue(ready)
    self.assertEqual(events, [('lfaButton', True), ('lfaButton', False)])
    self.assertEqual(self.intent.from_parser(self.parser, checksum), ([], True))

  def test_bad_frame_after_release_clears_batch_enable(self):
    self.warmup()
    events, ready = self.feed(self.frame(128), self.frame(), self.frame(corrupt=True))
    self.assertFalse(ready)
    self.assertEqual(events, [])

  def test_bounded_overflow_never_replays_last_held_press(self):
    self.warmup()
    self.parser.update([self.frame(128) for _ in range(65)])
    self.assertEqual(len(self.parser.raw_frames), 64)
    self.assertTrue(self.parser.raw_overflow)
    self.assertEqual(self.intent.from_parser(self.parser, checksum), ([], False))

  def test_default_parser_does_not_capture_and_echo_has_no_authority(self):
    self.parser.raw_capture.clear()
    self.feed(self.frame(128))
    self.assertFalse(self.parser.raw_frames)
    self.parser.raw_capture = {0x10B}
    self.feed(self.frame(128, bus=130))
    self.assertFalse(self.parser.raw_frames)


if __name__ == '__main__':
  unittest.main()
