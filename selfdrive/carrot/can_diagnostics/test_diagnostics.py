import gzip
import json
from pathlib import Path
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from .analysis import DbcIndex, Window, rank_candidates, signal_bits
from .core import Capture, safety_reason, json_safe
from .validation import DrivingValidator, decode_signal
from .routes import register
from .compare_rlog import compare_records


DBC_TEXT = '''BO_ 291 BRAKE: 8 XXX
 SG_ PEDAL : 2|1@1+ (1,0) [0|1] "" XXX
 SG_ PRESSURE : 8|8@1+ (0.5,-2) [0|120] "bar" XXX
 SG_ COUNTER : 16|4@1+ (1,0) [0|15] "" XXX
 SG_ CHECKSUM : 24|8@1+ (1,0) [0|255] "" XXX
'''


class Clock:
  def __init__(self):
    self.now = 100.0

  def __call__(self):
    return self.now


def telemetry(now, speed=0., gear='park', enabled=False):
  return {'carState': {'vEgo': speed, 'standstill': speed == 0, 'gearShifter': gear,
                       'canValid': True, 'brakePressed': False, 'aEgo': 0.},
          'selfdriveState': {'enabled': enabled, 'active': enabled},
          'carState_received': now, 'selfdriveState_received': now,
          'carState_valid': True, 'selfdriveState_valid': True}


class SignalTests(unittest.TestCase):
  def test_missing_numeric_values_produce_strict_json(self):
    data = {'vEgo': float('nan'), 'lead': [1.0, float('inf')]}
    self.assertEqual(json.loads(json.dumps(json_safe(data), allow_nan=False)), {'vEgo': None, 'lead': [1.0, None]})

  def test_rlog_comparison_separates_byte_mismatch_and_missing_coverage(self):
    def row(t, data='00'):
      return {'mono_ns':t,'bus':1,'address':291,'dlc':1,'data':data}
    result = compare_records([row(1), row(2), row(3, '01'), row(4)], [row(2), row(3)])
    self.assertEqual(result['matched_frames'], 1)
    self.assertEqual(result['diagnostic_outside_supplied_rlog'], 2)
    self.assertEqual(result['diagnostic_only_frames'], 1)
    self.assertEqual(result['rlog_only_frames'], 1)

  def test_signed_motorola_crosses_byte_boundary(self):
    signal = {'byte_order': 'big', 'start_bit': 3, 'size': 12, 'bits': signal_bits(3, 12, False),
              'signed': True, 'factor': .5, 'offset': 2}
    self.assertEqual(decode_signal(bytes([0x0F, 0xFE]), signal), (4094, 1.0))
    self.assertEqual(signal_bits(0, 3, False), [0, 15, 14])

  def test_intel_crosses_byte_boundary(self):
    signal = {'byte_order': 'little', 'start_bit': 4, 'size': 12, 'signed': False, 'factor': .1, 'offset': 0}
    self.assertEqual(decode_signal(bytes([0x10, 0x32]), signal)[0], 0x321)

  def test_repeated_edges_find_bit_and_preserve_bus(self):
    windows = []
    for cycle in range(3):
      for phase in ('baseline', 'active', 'release'):
        window = Window(cycle, phase)
        for i in range(40):
          window.add({(1, 0x123, 2): bytes([4 if phase == 'active' else 0, i % 4]),
                      (2, 0x123, 2): bytes([0, i % 4])})
        windows.append(window)
    result = rank_candidates(windows)
    self.assertEqual([(r['bus'], r['bit_lsb0']) for r in result], [(1, 2)])
    self.assertEqual(result[0]['mapping_status'], 'candidate_unverified')

  def test_one_off_change_is_not_a_mapping(self):
    windows = []
    for cycle in range(3):
      for phase in ('baseline', 'active', 'release'):
        window = Window(cycle, phase)
        for _ in range(40):
          window.add({(0, 1, 1): bytes([1 if cycle == 0 and phase == 'active' else 0])})
        windows.append(window)
    self.assertEqual(rank_candidates(windows), [])


class ValidatorTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp.cleanup)
    path = Path(self.temp.name) / 'test.dbc'
    path.write_text(DBC_TEXT)
    self.dbc = DbcIndex([path])

  def test_unknown_address_and_wrong_dlc_are_different(self):
    validator = DrivingValidator(self.dbc)
    validator.consume(0, 0x123, bytes(4), 1000000000)
    validator.consume(0, 0x555, bytes(8), 1000000000)
    self.assertEqual(validator.summary()['length_mismatches'], 1)
    self.assertEqual(validator.summary()['unknown_messages'], 1)

  def test_checksum_absence_is_not_a_successful_check(self):
    validator = DrivingValidator(self.dbc)
    validator.consume(0, 0x123, bytes(8), 1000000000)
    self.assertEqual(validator.summary()['checksum_checks'], 0)
    signals = validator.catalog()['messages'][0]['definitions'][0]['signals']
    self.assertIn('연결되어 있지 않음', signals[-1]['validation_note'])

  def test_configured_checks_and_counter_wrap(self):
    signals = self.dbc.messages[0x123][0]['signals']
    signals[2]['counter_configured'] = True
    signals[3]['checksum_configured'] = True
    self.dbc.checksum_functions['test.dbc', 0x123, 'CHECKSUM'] = lambda payload: 7
    validator = DrivingValidator(self.dbc)
    validator.consume(1, 0x123, bytes([4, 20, 15, 7, 0, 0, 0, 0]), 1000000000)
    validator.consume(1, 0x123, bytes([4, 20, 0, 7, 0, 0, 0, 0]), 1010000000)
    validator.consume(1, 0x123, bytes([4, 20, 3, 9, 0, 0, 0, 0]), 1020000000)
    result = validator.summary()
    self.assertEqual(result['checksum_checks'], 3)
    self.assertEqual(result['checksum_failures'], 1)
    self.assertEqual(result['counter_discontinuities'], 1)

  def test_same_id_on_different_buses_does_not_merge(self):
    validator = DrivingValidator(self.dbc)
    for bus in (0, 1):
      validator.consume(bus, 0x123, bytes(8), 1000000000)
    self.assertEqual(validator.summary()['observed_messages'], 2)

  def test_catalog_snapshot_keeps_decode_bits_and_freezes_observations(self):
    validator = DrivingValidator(self.dbc)
    validator.consume(0, 0x123, bytes(8), 1000000000)
    snapshot = validator.catalog()
    self.assertNotIn('bits', snapshot['messages'][0]['definitions'][0]['signals'][0])
    self.assertIn('bits', self.dbc.messages[0x123][0]['signals'][0])
    validator.consume(0, 0x123, bytes([4, 0, 0, 0, 0, 0, 0, 0]), 1200000000)
    self.assertEqual(snapshot['messages'][0]['signal_observations']['PEDAL']['last_value'], 0)
    self.assertEqual(validator.catalog()['messages'][0]['signal_observations']['PEDAL']['last_value'], 1)


class CaptureTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp.cleanup)
    self.clock = Clock()
    self.capture = Capture(self.temp.name, clock=self.clock,
                           disk_usage=lambda p: SimpleNamespace(free=2 * 1024 ** 3))
    self.addCleanup(self.capture.finish)
    self.refresh()

  def refresh(self, value=0):
    self.capture.telemetry = telemetry(self.clock.now)
    self.capture.last_client = self.clock.now
    self.capture.receive_frame(0, 0x123, bytes([value]), int(self.clock.now * 1e9))

  def test_driving_capture_allows_moving_and_engaged(self):
    self.capture.telemetry = telemetry(self.clock.now, 25., 'drive', True)
    self.capture.start('drive')
    self.clock.now += .1
    self.capture.receive_frame(0, 0x123, b'\x04', int(self.clock.now * 1e9))
    self.capture.marker('braking_late')
    self.capture.tick()
    self.assertTrue(self.capture.active())
    self.capture.finish('done', completed=True)
    folder = Path(self.temp.name) / self.capture.session['id']
    records = [json.loads(line) for line in gzip.open(folder/'raw_can.jsonl.gz', 'rt')]
    self.assertEqual(records[0]['data'], '04')
    self.assertEqual(records[0]['phase'], 'drive')
    self.assertTrue((folder/'catalog.json').is_file())
    self.assertIn('user_marker', (folder/'events.jsonl').read_text(encoding='utf-8'))

  def test_missing_decoded_state_does_not_block_raw_driving_evidence(self):
    self.capture.telemetry = {}
    self.capture.start('drive')
    self.assertTrue(self.capture.active())
    self.assertFalse(self.capture.status()['car_state_fresh'])

  def test_guided_intervention_rejects_moving_and_stale(self):
    self.capture.telemetry = telemetry(self.clock.now, 1., 'drive', True)
    with self.assertRaises(ValueError):
      self.capture.start('brake')
    self.capture.telemetry = telemetry(self.clock.now - 3)
    with self.assertRaises(ValueError):
      self.capture.start('brake')

  def test_driving_survives_ui_background_but_stops_when_can_lost(self):
    self.capture.start('drive')
    self.clock.now += 20
    self.capture.receive_frame(0, 0x123, b'\x00', int(self.clock.now * 1e9))
    self.capture.tick()
    self.assertTrue(self.capture.active())
    self.clock.now += 6
    self.capture.tick()
    self.assertFalse(self.capture.active())

  def test_guided_stops_when_vehicle_moves(self):
    self.capture.start('brake')
    self.capture.telemetry = telemetry(self.clock.now, 1., 'drive')
    self.capture.tick()
    self.assertFalse(self.capture.active())

  def test_stale_can_does_not_make_preflight_ready(self):
    self.capture.last_can = -1e9
    self.capture.receive_frame(0, 0x123, b'\x00', int((self.clock.now - 5) * 1e9))
    self.assertFalse(self.capture.status()['ready'])

  def test_send_echoes_are_not_mixed_with_rx(self):
    before = self.capture.counters['can_frames']
    self.capture.receive_frame(128, 0x123, b'\x00', int(self.clock.now * 1e9))
    self.assertEqual(self.capture.counters['can_frames'], before)
    self.assertEqual(self.capture.counters['echo_frames_ignored'], 1)

  def test_disk_low_blocks_new_session(self):
    self.capture.disk_usage = lambda p: SimpleNamespace(free=10)
    with self.assertRaises(ValueError):
      self.capture.start('drive')

  def test_capture_bound_preserves_partial_report(self):
    self.capture.start('drive')
    with patch('selfdrive.carrot.can_diagnostics.core.MAX_BYTES', 1):
      self.capture.receive_frame(0, 1, b'\x01', int(self.clock.now * 1e9))
    self.assertFalse(self.capture.active())
    self.assertFalse(self.capture.report['completed'])

  def test_real_vehicle_message_cardinality_fits_bounded_capture(self):
    self.capture.start('drive')
    for address in range(650):
      self.capture.receive_frame(0, address, bytes(8), int(self.clock.now * 1e9))
    self.assertTrue(self.capture.active())
    self.assertEqual(self.capture.session['frame_count'], 650)

  def test_finished_duration_does_not_keep_growing(self):
    self.capture.start('drive')
    self.clock.now += 10
    self.capture.finish()
    before = self.capture.status()['session']['elapsed_seconds']
    self.clock.now += 50
    self.assertEqual(self.capture.status()['session']['elapsed_seconds'], before)

  def test_complete_guided_roundtrip(self):
    self.capture.start('brake')
    for step in range(9):
      self.capture.mark()
      for _ in range(50):
        self.clock.now += .101
        self.refresh(4 if step % 3 == 1 else 0)
        self.capture.tick()
    self.assertEqual(self.capture.session['state'], 'completed')
    self.assertEqual(self.capture.report['candidates'][0]['bit_lsb0'], 2)


class ApiTests(unittest.IsolatedAsyncioTestCase):
  async def asyncSetUp(self):
    from .collector import Controller
    self.temp = tempfile.TemporaryDirectory()
    self.controller = Controller(self.temp.name, demo=True)
    app = web.Application(client_max_size=1024)
    register(app, controller=self.controller)
    self.client = TestClient(TestServer(app))
    await self.client.start_server()

  async def asyncTearDown(self):
    await self.client.close()
    self.temp.cleanup()

  async def test_cross_origin_and_form_posts_rejected(self):
    response = await self.client.post('/api/can_diagnostics/start', json={'test_id':'drive'})
    self.assertEqual(response.status, 403)
    response = await self.client.post('/api/can_diagnostics/start', json={'test_id':'drive'},
                                     headers={'X-Carrot-Diagnostics':'1','Origin':'https://evil.example'})
    self.assertEqual(response.status, 403)

  async def test_path_traversal_and_unknown_files_rejected(self):
    for path in ('../../etc/passwd', '20260927T000000Z-abcdef1234/secret.txt'):
      response = await self.client.get('/api/can_diagnostics/download/' + path)
      self.assertEqual(response.status, 404)

  async def test_status_explicitly_labels_demo(self):
    response = await self.client.get('/api/can_diagnostics/status')
    self.assertTrue((await response.json())['demo'])

  async def test_non_string_test_id_is_handled(self):
    response = await self.client.post('/api/can_diagnostics/start', json={'test_id':[]}, headers={'X-Carrot-Diagnostics':'1'})
    self.assertEqual(response.status, 409)


if __name__ == '__main__':
  unittest.main()
