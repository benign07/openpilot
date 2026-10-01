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

  def test_integrity_cause_survives_recovery_until_three_neutral_frames(self):
    self.assertEqual(self.input.state, 'warmingUp')
    self.warmup()
    self.assertEqual(self.input.state, 'ready')
    self.feed(0, corrupt=True)
    self.assertEqual(self.input.state, 'integrityFault')
    self.assertEqual(self.input.diagnostic_reason, 'checksum')
    self.feed(2)  # Sequence jump after rejected CRC does not overwrite its cause.
    self.assertEqual(self.input.diagnostic_reason, 'checksum')
    self.feed(4, raw=128)
    self.assertEqual(self.input.reason, 'neutral_required')
    for c in (6, 8):
      self.feed(c)
      self.assertEqual(self.input.state, 'integrityFault')
      self.assertEqual(self.input.diagnostic_reason, 'checksum')
    self.feed(10)
    self.assertEqual(self.input.state, 'ready')
    self.assertEqual(self.input.diagnostic_reason, 'valid')

  def test_reset_serial_is_nonzero_and_changes_on_every_rejection(self):
    self.warmup()
    serial = self.input.reset_count
    self.feed(0, corrupt=True)
    self.assertEqual(self.input.reset_count, serial + 1)
    self.input.reset_count = 2**32 - 1
    self.input.reject('cancel')
    self.assertEqual(self.input.reset_count, 1)

  def test_dead_stream_is_a_fault_even_before_warmup_finishes(self):
    self.assertFalse(self.input.fresh(self.now))
    self.assertEqual(self.input.state, 'warmingUp')
    self.assertFalse(self.input.fresh(self.now + 200_000_001))
    self.assertEqual((self.input.state, self.input.diagnostic_reason), ('integrityFault', 'stale'))
    self.input = Input()
    self.feed(0)
    self.assertFalse(self.input.fresh(self.now + 100_000_000))
    self.assertEqual(self.input.samples, 1)
    self.assertFalse(self.input.fresh(self.now + 200_000_001))
    self.assertEqual(self.input.samples, 0)
    self.assertEqual((self.input.state, self.input.diagnostic_reason), ('integrityFault', 'stale'))


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

  def test_main_neutral_then_new_button_confirms_release_in_physical_order(self):
    for raw, name in ((1, 'accelCruise'), (2, 'decelCruise'), (3, 'gapAdjustCruise'), (128, 'lfaButton')):
      with self.subTest(raw=raw):
        self.setUp()
        self.assertEqual(self.feed(8), [('mainCruise', True)])
        self.assertEqual(self.feed(), [])
        anchor = self.counter
        self.assertEqual(self.feed(raw), [('mainCruise', False), (name, True)])
        self.assertEqual(self.intent.main_release_counter, anchor)
        self.assertTrue(self.intent.input.ready)
        self.assertEqual(self.feed(), [(name, False)])

  def test_direct_button_changes_do_not_manufacture_release(self):
    for first, second, name in ((1, 2, 'decelCruise'), (128, 1, 'accelCruise'), (1, 8, 'mainCruise')):
      with self.subTest(first=first, second=second):
        self.setUp()
        self.feed(first)
        self.assertEqual(self.feed(second), [(name, True)])
        if second != 8:
          self.assertEqual(self.feed(), [(name, False)])

  def test_main_without_neutral_or_with_damaged_witness_cannot_complete(self):
    self.feed(8)
    self.assertEqual(self.feed(1), [])
    self.assertEqual(self.intent.input.reason, 'ambiguous_gesture')
    self.setUp()
    self.feed(8)
    self.feed()
    self.assertEqual(self.feed(1, corrupt=True), [])
    self.feed()  # Counter recovery after the CRC-rejected frame is not warmup.
    for _ in range(3): self.feed()
    self.assertTrue(self.intent.input.ready)
    self.assertEqual(self.feed(), [])

  def test_new_main_clears_earlier_neutral_witness(self):
    self.feed(8)
    self.feed()
    self.feed(8)
    self.assertEqual(self.feed(1), [])
    self.assertEqual(self.intent.input.reason, 'ambiguous_gesture')

  def test_cancel_over_simultaneous_lfa_and_pending_main(self):
    self.feed(8)
    self.assertEqual(self.feed(132), [('cancel', True)])
    self.assertEqual(self.intent.input.state, 'requalifying')
    self.assertEqual(self.feed(), [])
    for _ in range(10): self.assertEqual(self.feed(), [])

  def test_cancel_requalification_blocks_short_res_then_allows_new_release(self):
    self.assertEqual(self.feed(4), [('cancel', True)])
    self.assertFalse(self.intent.input.ready)
    self.assertEqual(self.feed(), [])
    self.assertEqual(self.feed(1), [])
    self.assertEqual(self.intent.input.state, 'requalifying')
    for _ in range(2):
      self.assertEqual(self.feed(), [])
      self.assertFalse(self.intent.input.ready)
    self.assertEqual(self.feed(), [])
    self.assertTrue(self.intent.input.ready)
    self.assertEqual(self.feed(1), [('accelCruise', True)])
    self.assertEqual(self.feed(), [('accelCruise', False)])

  def test_dead_stream_after_cancel_is_stale_instead_of_waiting_forever(self):
    self.feed(4)
    self.assertEqual(self.intent.input.state, 'requalifying')
    self.assertFalse(self.intent.fresh(self.now + 200_000_001))
    self.assertEqual((self.intent.input.state, self.intent.input.diagnostic_reason), ('integrityFault', 'stale'))

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

  def test_shared_batch_timestamp_preserves_counter_order(self):
    self.warmup()
    press, release = self.frame(128), self.frame()
    batch = [release[0], press[1] + release[1]]
    self.parser.update([batch])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual(events, [('lfaButton', True, 0), ('lfaButton', False, 2)])

  def test_fast_delivery_uses_counter_not_transport_min_period(self):
    self.warmup()
    press = self.frame(128)
    release = self.frame()
    release[0] = press[0] + 5_000_000
    self.parser.update([press, release])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual([e[2] for e in events], [0, 2])

  def test_counter_metadata_belongs_to_each_release_in_batch(self):
    self.warmup()
    frames = [self.frame(128), self.frame(), self.frame(128), self.frame()]
    batch = [frames[-1][0], [f for frame in frames for f in frame[1]]]
    self.parser.update([batch])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual([e[2] for e in events if not e[1]], [2, 6])

  def test_main_counter_is_first_neutral_before_debounce(self):
    self.warmup()
    self.feed(self.frame(8))
    first = self.frame()
    anchor = self.counter
    self.feed(first)
    for _ in range(6): self.feed(self.frame())
    self.parser.update([self.frame()])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual(events, [('mainCruise', False, anchor)])
    self.assertNotEqual(anchor, self.intent.input.counter)

  def test_shared_timestamp_does_not_allow_duplicate_or_reverse_counter(self):
    self.warmup()
    press = self.frame(128)
    self.parser.update([[press[0], press[1] + press[1]]])
    self.assertEqual(self.intent.from_parser(self.parser, checksum, with_counter=True), ([], False))

  def test_forced_main_release_keeps_each_counter_in_shared_batch(self):
    self.warmup()
    frames = [self.frame(8), self.frame(), self.frame(1), self.frame()]
    self.parser.update([[frames[-1][0], [f for frame in frames for f in frame[1]]]])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual(events, [('mainCruise', True, 0), ('mainCruise', False, 2),
                              ('accelCruise', True, 4), ('accelCruise', False, 6)])

  def test_bad_frame_after_release_clears_batch_enable(self):
    self.warmup()
    events, ready = self.feed(self.frame(128), self.frame(), self.frame(corrupt=True))
    self.assertFalse(ready)
    self.assertEqual(events, [])

  def test_hidden_fault_and_requalification_in_one_batch_retains_reset_serial(self):
    self.warmup()
    serial = self.intent.input.reset_count
    frames = [self.frame(corrupt=True)] + [self.frame() for _ in range(4)] + [self.frame(1), self.frame()]
    self.parser.update([[frames[-1][0], [f for frame in frames for f in frame[1]]]])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertTrue(ready)
    self.assertEqual(self.intent.input.state, 'ready')
    self.assertGreater(self.intent.input.reset_count, serial)
    self.assertEqual([e[:2] for e in events], [('accelCruise', True), ('accelCruise', False)])

  def test_bad_frame_after_valid_cancel_preserves_cancel_and_faults(self):
    self.warmup()
    self.parser.update([self.frame(4), self.frame(corrupt=True)])
    events, ready = self.intent.from_parser(self.parser, checksum, with_counter=True)
    self.assertFalse(ready)
    self.assertEqual(events, [('cancel', True, 0)])

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


class TestCameraHealthStartup(unittest.TestCase):
  def setUp(self):
    from types import SimpleNamespace as NS
    from unittest.mock import patch
    clock = runpy.run_path(str(ROOT / 'selfdrive/carrot/tests/test_lx3_can_time.py'))
    self.env = clock['ENV']
    # Production parser construction must use recorded time for startup grace.
    with patch.object(self.env['time'], 'monotonic_ns', return_value=1_000_000_000):
      self.parser = self.env['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    state = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_state.py'))
    self.fault, self.fresh_values = state['lateral_fault'], state['fresh_camera_values']
    self.now = 1_000_000_000

  def feed(self, fault_bit=None, corrupt=False, bus=2):
    self.now += 50_000_000
    data = bytearray(32)
    if fault_bit is not None:
      data[fault_bit // 8] |= 1 << (fault_bit % 8)
    data[:2] = checksum(0x162, None, data).to_bytes(2, 'little')
    if corrupt:
      data[0] ^= 1
    self.parser.update([[self.now, [(0x162, bytes(data), bus)]]])

  def current_fault(self, now=None):
    return self.fault(self.parser.vl['CCNC_0x162'],
                      self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS'], self.now if now is None else now)

  def test_fresh_health_is_available_before_controls_ready_and_display_cache(self):
    self.assertFalse(self.parser.controls_ready)
    self.assertIn(0x162, self.parser.addresses)
    self.assertTrue(self.current_fault())  # Missing data never implies health.
    self.feed()
    self.assertFalse(self.current_fault())
    self.assertFalse(self.parser.controls_ready)
    self.assertTrue(self.current_fault(self.now + 250_000_001))

  def test_fault_and_crc_rejection_preserve_entry_barrier(self):
    bit = self.parser.dbc.name_to_msg['CCNC_0x162'].sigs['FAULT_LSS'].lsb
    self.feed(fault_bit=bit)
    self.assertTrue(self.current_fault())
    stamp = self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS']
    self.feed(corrupt=True)
    self.assertEqual(self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS'], stamp)
    self.assertTrue(self.current_fault())
    self.feed()
    self.assertFalse(self.current_fault())

  def test_forwarded_echo_does_not_establish_camera_health(self):
    self.feed(bus=130)
    self.assertTrue(self.current_fault())
    self.feed()
    stamp = self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS']
    for _ in range(6):
      self.feed(corrupt=True)
    self.assertEqual(self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS'], stamp)
    self.assertTrue(self.current_fault())

  def test_missing_bad_crc_and_timeout_never_publish_unreceived_object_cache(self):
    def cached():
      return self.fresh_values(self.parser.vl['CCNC_0x162'],
                               self.parser.ts_nanos['CCNC_0x162']['FAULT_LSS'], self.now)
    self.assertIsNone(cached())
    self.feed(corrupt=True)
    self.assertIsNone(cached())
    self.feed()
    self.assertIs(cached(), self.parser.vl['CCNC_0x162'])
    self.now += 501_000_000
    self.parser.update([[self.now, []]])
    self.assertIsNone(cached())
    self.assertTrue(self.current_fault())
    self.assertFalse(self.parser.can_valid)

  def test_missing_required_health_after_startup_grace_invalidates_can(self):
    self.now = 3_100_000_000
    self.parser.update([[self.now, [(0x161, bytes(32), 2)]]])
    self.assertTrue(self.current_fault())
    self.assertFalse(self.parser.can_valid)


if __name__ == '__main__':
  unittest.main()
