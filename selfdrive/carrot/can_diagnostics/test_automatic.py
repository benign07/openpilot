import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from .automatic import AutoRecorder, ChunkStore, control_mode
from .button_trace_report import summarize as summarize_button_traces
from .automatic_routes import register
from .automatic_runtime import AutomaticController, panda_summary, read_param, selected_fields
from tools.can_auto_sync import analyze, download


def services(now=100, started=True, lat=False, long=False):
  data = {'deviceState': {'started': started},
          'carState': {'vEgo': 20., 'aEgo': -1., 'canValid': True, 'brakePressed': False,
                       'steeringPressed': False, 'cruiseState': {'enabled': False}},
          'carControl': {'enabled': lat or long, 'latActive': lat, 'longActive': long},
          'selfdriveState': {'enabled': lat or long, 'active': lat or long},
          'radarState': {'leadOne': {'status': True, 'dRel': 30.}}}
  return {key: {'mono_ns': int(now * 1e9), 'valid': True, 'data': value} for key, value in data.items()}


class RuntimeTests(unittest.TestCase):
  def test_transient_worker_error_retries_without_losing_service(self):
    class Shutdown:
      def __init__(self, controller): self.controller = controller
      def is_set(self): return self.controller.calls >= 2
      def wait(self, delay): return self.is_set()
    class Controller(AutomaticController):
      def __init__(self):
        super().__init__()
        self.calls = 0
        self.shutdown = Shutdown(self)
      def live_loop(self):
        self.calls += 1
        if self.calls == 1:
          raise RuntimeError('temporary subscriber failure')
    controller = Controller()
    controller.run()
    self.assertEqual(controller.calls, 2)
    self.assertEqual(controller.status()['recorder_restart_count'], 1)

  def test_only_selected_fields_are_converted(self):
    class Nested:
      def to_dict(self): return {'enabled': False}
    class Reader:
      vEgo = 12.5
      gearShifter = 'drive'
      cruiseState = Nested()
      speeds = (1., 2.)
      def to_dict(self): raise AssertionError('Full message conversion is unnecessary')
    self.assertEqual(selected_fields(Reader(), ('vEgo', 'gearShifter', 'cruiseState', 'speeds', 'absent')),
                     {'vEgo':12.5,'gearShifter':'drive','cruiseState':{'enabled':False},'speeds':[1.,2.],'absent':None})
    Reader.buttonEvents = [SimpleNamespace(type='lfaButton', pressed=True)]
    self.assertEqual(selected_fields(Reader(), ('buttonEvents',)),
                     {'buttonEvents': [{'type': 'lfaButton', 'pressed': True}]})

  def test_typed_params_and_legacy_bytes_need_no_encoding_keyword(self):
    class Params:
      def get(self, key):
        return {'route': b'route1', 'mode': 4, 'flag': False, 'text': 'current', 'missing': None}[key]
    params = Params()
    self.assertEqual(read_param(params, 'route'), 'route1')
    self.assertEqual(read_param(params, 'mode'), 4)
    self.assertIs(read_param(params, 'flag'), False)
    self.assertEqual(read_param(params, 'text'), 'current')
    self.assertIsNone(read_param(params, 'missing'))

  def test_panda_summary_keeps_only_authority_and_transport_fields(self):
    bus = SimpleNamespace(totalRxCnt=20, totalRxLostCnt=0, totalErrorCnt=0, busOff=False)
    panda = SimpleNamespace(safetyModel='hyundaiCanfd', safetyParam=190, controlsAllowed=True,
                            safetyTxBlocked=3,
                            rxBufferOverflow=0, txBufferOverflow=0, spiChecksumErrorCount=12,
                            safetyRxChecksInvalid=False, faults=[], canState0=bus, canState1=bus,
                            canState2=bus)
    summary = panda_summary(panda)
    self.assertEqual((summary['safety_param'], summary['controls_allowed'], summary['spi_checksum_errors']),
                     (190, True, 12))
    self.assertEqual(summary['tx_blocked'], 3)
    self.assertEqual(len(summary['buses']), 3)


class RecorderTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp.cleanup)
    self.root = Path(self.temp.name)
    self.store = ChunkStore(self.root, reserve=0, seconds=1, chunk_bytes=4096)
    self.recorder = AutoRecorder(self.store, {'boot_id': 'boot1', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV'})
    self.addCleanup(self.recorder.close)

  def rows(self):
    self.recorder.close()
    rows = []
    for path in self.root.glob('*.jsonl.gz'):
      with gzip.open(path, 'rt', encoding='utf-8') as stream:
        rows.extend(json.loads(line) for line in stream)
    return rows

  def test_starts_without_openpilot_and_rotates_without_stopping_trip(self):
    self.recorder.update(services(), 100)
    trip = self.recorder.trip
    self.recorder.update(services(101.1), 101.1)
    self.assertEqual(self.recorder.trip, trip)
    self.assertEqual(len(self.store.list_chunks()), 1)
    self.assertEqual(self.recorder.state, 'recording')
    self.recorder.update(services(101.4, started=False), 101.4)
    self.assertIsNone(self.store.stream)
    self.assertEqual(len(self.store.list_chunks()), 2)

  def test_stale_ignition_closes_capture_and_resume_is_distinct(self):
    self.recorder.update(services(), 100)
    trip = self.recorder.trip
    self.recorder.update(services(), 104)
    self.assertEqual(self.recorder.state, 'ignition_unknown')
    self.assertIsNone(self.store.stream)
    self.recorder.update(services(105), 105)
    self.assertNotEqual(self.recorder.trip, trip)

  def test_always_lateral_is_not_driver_training(self):
    self.assertEqual(control_mode(services(lat=True), 100), 'openpilot_lateral')
    sample = services()
    sample['carControl']['data']['latActive'] = None
    self.assertEqual(control_mode(sample, 100), 'unknown')
    self.assertEqual(control_mode(services(), 102), 'unknown')

  def test_can_provenance_and_fault_edges_bypass_sampling(self):
    self.recorder.update(services(), 100)
    normal, fault = bytes(32), (1 << 219).to_bytes(32, 'little')
    for bus, direction in ((2, 'rx'), (2, 'tx_requested'), (130, 'rx')):
      self.recorder.can_frame(bus, 0x162, normal, 100_000_000_000, 100, direction)
      self.recorder.can_frame(bus, 0x162, fault, 100_010_000_000, 100.01, direction)
    rows = self.rows()
    edges = [r for r in rows if r.get('name') == 'oem_fault_observation' and r['after'][0] == 1]
    self.assertEqual({r['direction'] for r in edges}, {'rx', 'tx_echo', 'tx_requested'})
    self.assertEqual(len([r for r in rows if r['kind'] == 'can_sample']), 6)

  def test_sampling_and_stale_can_are_explicit(self):
    self.recorder.update(services(), 100)
    for now in (100, 100.01):
      self.recorder.can_frame(0, 0x161, bytes(16), int(now * 1e9), now)
    self.recorder.can_frame(0, 0x161, bytes(16), 98_000_000_000, 100)
    self.assertEqual(self.recorder.sampled_out, 1)
    self.assertEqual(self.recorder.stale_packets, 1)

  def test_steering_path_and_panda_are_sampled_without_unbounded_stream(self):
    self.recorder.update(services(), 100)
    for address, length in ((0x10B, 16), (0x12A, 16), (0xCB, 24), (0xEA, 24)):
      self.recorder.can_frame(2, address, bytes(length), 100_000_000_000, 100)
    self.recorder.can_frame(2, 0xCB, bytes(24), 100_010_000_000, 100.01)
    self.recorder.can_frame(2, 0x123, bytes(24), 100_000_000_000, 100)
    state = {'safety_param': 190, 'controls_allowed': True, 'rx_overflow': 0}
    for now in (100, 100.2, 100.6):
      self.recorder.panda_snapshot([state], int(now * 1e9), now)
    rows = self.rows()
    self.assertEqual({row['address'] for row in rows if row['kind'] == 'can_sample'},
                     {0x10B, 0x12A, 0xCB, 0xEA})
    self.assertEqual(len([row for row in rows if row['kind'] == 'panda_snapshot']), 2)
    self.assertLess(self.store.usage, self.store.quota)

  def test_short_received_panda_authority_pulse_is_retained_outside_button_trace(self):
    self.recorder.update(services(), 100)
    for now, allowed in ((100, False), (100.1, True), (100.2, False), (100.25, False)):
      state = {'safety_param': 190, 'controls_allowed': allowed, 'rx_overflow': 0}
      self.recorder.panda_snapshot([state], int(now * 1e9), now)
    rows = self.rows()
    self.assertEqual([row['states'][0]['controls_allowed'] for row in rows if row['kind'] == 'panda_snapshot'],
                     [False, True, False])
    edges = [row for row in rows if row.get('name') == 'panda_state_edge']
    self.assertEqual([(row['before'], row['after']) for row in edges], [(False, True), (True, False)])
    self.assertEqual([row['mono_ns'] for row in edges], [100_100_000_000, 100_200_000_000])

  def test_trace_reports_frames_lost_at_chunk_limit(self):
    self.recorder.update(services(), 100)
    self.recorder.can_frame(0, 0x10B, bytes(16), 100_010_000_000, 100.01)
    press = bytearray(16); press[10] = 0x80
    self.recorder.can_frame(0, 0x10B, press, 100_030_000_000, 100.03)
    self.assertEqual(self.recorder.trace_count, 1)
    self.store.chunk_bytes = self.store.size
    self.recorder.can_frame(2, 0xCB, bytes(24), 100_050_000_000, 100.05)
    rows = self.rows()
    end = next(row for row in rows if row.get('name') == 'button_trace_end')
    self.assertEqual(end['dropped_full'], 1)
    self.assertEqual(len([row for row in rows if row['kind'] == 'button_trace_can' and row['address'] == 0xCB]), 0)

  def test_panda_pulse_capture_rejects_stale_states_and_preserves_other_car_sampling(self):
    self.recorder.update(services(), 100)
    self.recorder.panda_snapshot([{'controls_allowed': False}], 100_000_000_000, 100)
    self.recorder.panda_snapshot([{'controls_allowed': True}], 99_000_000_000, 100.1)
    self.assertEqual(self.recorder.stale_packets, 1)
    self.recorder.metadata['car_fingerprint'] = 'OTHER_CAR'
    self.recorder.panda_snapshot([{'controls_allowed': True}], 100_100_000_000, 100.1)
    self.recorder.panda_snapshot([{'controls_allowed': False}], 100_200_000_000, 100.2)
    self.recorder.panda_snapshot([{'controls_allowed': False}], 100_600_000_000, 100.6)
    rows = self.rows()
    self.assertEqual(len([row for row in rows if row['kind'] == 'panda_snapshot']), 2)
    self.assertFalse(any(row.get('name') == 'panda_state_edge' for row in rows))

  def test_cumulative_panda_counters_keep_periodic_sample_limit(self):
    self.store.seconds = 120
    self.store.chunk_bytes = 2 * 1024**2
    self.recorder.update(services(), 100)
    for index in range(1201):
      mono_ns = 100_000_000_000 + index * 50_000_000
      state = {'controls_allowed': False, 'safety_param': 190, 'tx_blocked': index}
      self.recorder.panda_snapshot([state], mono_ns, mono_ns / 1e9)
    rows = self.rows()
    snapshots = [row for row in rows if row['kind'] == 'panda_snapshot']
    self.assertLessEqual(len(snapshots), 121)
    self.assertGreaterEqual(len(snapshots), 110)
    self.assertGreaterEqual(snapshots[-1]['states'][0]['tx_blocked'], 1190)

  def test_physical_lfa_and_cruise_buttons_open_bounded_pre_post_trace(self):
    self.recorder.update(services(), 100)
    self.recorder.can_frame(2, 0xCB, bytes(24), 100_000_000_000, 100)
    neutral = bytes(16)
    lfa = bytearray(16); lfa[10] = 0x80
    main = bytearray(16); main[10] = 8
    self.recorder.can_frame(0, 0x10B, neutral, 100_010_000_000, 100.01)
    self.recorder.can_frame(130, 0x10B, lfa, 100_020_000_000, 100.02)
    self.assertEqual(self.recorder.trace_count, 0)
    self.recorder.can_frame(0, 0x10B, lfa, 100_030_000_000, 100.03)
    original = bytearray(24); original[2] = 7; original[3] = 0x10
    sent = bytearray(original); sent[3] = 0x20
    self.recorder.can_frame(2, 0xCB, original, 100_040_000_000, 100.04)
    self.recorder.can_frame(128, 0xCB, sent, 100_040_000_000, 100.04)
    self.recorder.can_frame(192, 0xCB, sent, 100_050_000_000, 100.05)
    self.recorder.can_frame(0, 0x10B, neutral, 100_100_000_000, 100.1)
    self.recorder.can_frame(0, 0x10B, main, 100_500_000_000, 100.5)
    self.assertEqual(self.recorder.trace_count, 1)
    self.recorder.update(services(109), 109)
    rows = self.rows()
    edges = [r for r in rows if r.get('name') == 'physical_button_edge']
    self.assertEqual([r['button'] for r in edges], ['lfa', 'cruise_main'])
    self.assertEqual({r['source'] for r in edges}, {'rx_bus0_0x10B'})
    traces = [r for r in rows if r['kind'] == 'button_trace_can']
    self.assertIn((2, 0xCB), {(r['bus'], r['address']) for r in traces})
    self.assertIn((0, 0x10B), {(r['bus'], r['address']) for r in traces})
    self.assertIn('tx_rejected', {r['direction'] for r in traces})
    self.assertEqual(len([r for r in rows if r.get('name') == 'button_trace_end']), 1)
    report = summarize_button_traces(self.root)
    self.assertEqual(len(report['traces']), 1)
    self.assertEqual(report['traces'][0]['button'], 'lfa')
    self.assertTrue(report['traces'][0]['complete'])
    self.assertEqual(report['traces'][0]['frame_counts']['rx/bus0/0x10B'], 4)
    self.assertEqual(report['traces'][0]['forward_pairs']['0x0CB']['paired'], 1)
    self.assertEqual(report['traces'][0]['forward_pairs']['0x0CB']['payload_changed'], 1)

  def test_trace_captures_full_rate_can_and_panda_authority_edges(self):
    self.recorder.update(services(), 100)
    self.recorder.can_frame(0, 0x10B, bytes(16), 100_000_000_000, 100)
    pressed = bytearray(16); pressed[10] = 1
    self.recorder.can_frame(0, 0x10B, pressed, 100_100_000_000, 100.1)
    for index in range(5):
      now = 100.11 + index * .01
      self.recorder.can_frame(2, 0xCB, bytes([index]) + bytes(23), int(now * 1e9), now)
    allowed = {'controls_allowed': True, 'tx_blocked': 0, 'safety_param': 190}
    blocked = {'controls_allowed': False, 'tx_blocked': 1, 'safety_param': 190}
    self.recorder.panda_snapshot([allowed], 100_200_000_000, 100.2)
    self.recorder.panda_snapshot([blocked], 100_310_000_000, 100.31)
    self.recorder.update(services(100.4, lat=True), 100.4)
    rows = self.rows()
    self.assertEqual(len([r for r in rows if r['kind'] == 'button_trace_can' and r['address'] == 0xCB]), 5)
    self.assertIn('controls_allowed', [r['field'] for r in rows if r.get('name') == 'panda_state_edge'])
    self.assertIn('tx_blocked', [r['field'] for r in rows if r.get('name') == 'panda_state_edge'])
    self.assertIn('lat_active', [r['field'] for r in rows if r.get('name') == 'host_state_edge'])

  def test_lx3_extra_addresses_and_button_trigger_do_not_change_other_cars(self):
    self.recorder.metadata['car_fingerprint'] = 'OTHER_CAR'
    self.recorder.update(services(), 100)
    self.recorder.can_frame(0, 0x2AF, bytes(8), 100_000_000_000, 100)
    self.recorder.can_frame(0, 0x10B, bytes(16), 100_010_000_000, 100.01)
    button = bytearray(16); button[10] = 0x80
    self.recorder.can_frame(0, 0x10B, button, 100_100_000_000, 100.1)
    rows = self.rows()
    self.assertNotIn(0x2AF, {r['address'] for r in rows if r['kind'] == 'can_sample'})
    self.assertEqual(self.recorder.trace_count, 0)

  def test_fault_burst_cannot_grow_chunk_without_bound(self):
    self.recorder.update(services(), 100)
    for index in range(1000):
      now = 100 + index / 10000
      data = ((index % 2) << 219).to_bytes(32, 'little')
      self.recorder.can_frame(2, 0x162, data, int(now * 1e9), now)
    self.assertLess(self.store.size, self.store.chunk_bytes + 1000)
    self.assertGreater(self.recorder.sampled_out, 900)

  def test_quota_preserves_existing_records(self):
    self.recorder.update(services(), 100)
    self.recorder.close()
    originals = {p.name: p.read_bytes() for p in self.root.iterdir()}
    self.store.quota = self.store.usage + 1
    self.recorder.update(services(102), 102)
    self.assertEqual(self.recorder.state, 'storage_full_preserving_records')
    self.assertEqual(originals, {p.name: p.read_bytes() for p in self.root.iterdir()})

  def test_power_loss_recovers_only_complete_lines(self):
    path = self.root / ('b' * 32 + '.partial')
    path.write_bytes(b'{"kind":"header","schema":1}\n{"incomplete":')
    recovered = ChunkStore(self.root, reserve=0)
    self.assertEqual(recovered.list_chunks()[0]['reason'], 'power_loss_recovered')
    with gzip.open(next(self.root.glob('*.gz')), 'rt') as stream:
      self.assertEqual(len(stream.readlines()), 1)

  def test_resume_after_seal_crash_recovers_manifest(self):
    self.recorder.update(services(), 100)
    self.recorder.close()
    next(self.root.glob('*.manifest.json')).unlink()
    recovered = ChunkStore(self.root, reserve=0)
    self.assertEqual(recovered.list_chunks()[0]['reason'], 'interrupted_seal_recovered')

  def test_analysis_separates_control_modes_and_never_applies_settings(self):
    self.recorder.update(services(), 100)
    self.recorder.update(services(100.3, lat=True), 100.3)
    self.recorder.close()
    result = analyze(self.root)
    self.assertEqual(result['moving_samples_by_control_mode'], {'driver': 1, 'openpilot_lateral': 1})
    self.assertFalse(result['control_changes_applied'])
    self.assertTrue(all(row['parameter_change'] is None for row in result['review_candidates']))
    self.assertIn('driver:60-80kmh:gap_seconds', result['histograms'])

  def test_duplicate_phone_and_pc_copy_count_once(self):
    self.recorder.update(services(), 100)
    self.recorder.close()
    duplicate = self.root / 'phone'
    duplicate.mkdir()
    for path in list(self.root.glob('*.json*')):
      (duplicate / path.name).write_bytes(path.read_bytes())
    self.assertEqual(analyze(self.root)['coverage']['verified_chunks'], 1)


class Response(io.BytesIO):
  def __init__(self, data, status=200, headers=None):
    super().__init__(data)
    self.status, self.headers = status, headers or {}


class DownloadTests(unittest.TestCase):
  def setUp(self):
    self.temp = tempfile.TemporaryDirectory()
    self.addCleanup(self.temp.cleanup)
    self.root = Path(self.temp.name)
    self.data = b'chunk-data'
    self.row = {'id': 'a' * 32, 'bytes': len(self.data), 'sha256': hashlib.sha256(self.data).hexdigest()}

  def test_resume_and_deduplicate(self):
    (self.root / (self.row['id'] + '.download')).write_bytes(self.data[:3])
    def opener(request, **kwargs):
      self.assertEqual(request.headers['Range'], 'bytes=3-')
      return Response(self.data[3:], 206, {'Content-Range': 'bytes 3-9/10'})
    self.assertTrue(download('http://localhost', self.root, self.row, opener))
    self.assertFalse(download('http://localhost', self.root, self.row, lambda *a, **k: self.fail('duplicate request')))

  def test_server_ignoring_range_restarts_instead_of_appending(self):
    (self.root / (self.row['id'] + '.download')).write_bytes(self.data[:3])
    self.assertTrue(download('http://localhost', self.root, self.row, lambda *a, **k: Response(self.data)))

  def test_bad_hash_never_becomes_verified_file(self):
    with self.assertRaises(ValueError):
      download('http://localhost', self.root, self.row, lambda *a, **k: Response(b'x' * 10))
    self.assertFalse(list(self.root.glob('*.jsonl.gz')))

  def test_rejects_bad_range_and_path_traversal(self):
    with self.assertRaises(ValueError):
      download('http://localhost', self.root, self.row, lambda *a, **k: Response(self.data, 206, {'Content-Range': 'bytes 2-9/10'}))
    with self.assertRaises(ValueError):
      download('http://localhost', self.root, {**self.row, 'id': '../evil'})


class RouteTests(unittest.IsolatedAsyncioTestCase):
  async def test_completed_chunk_range_matches_manifest_bytes(self):
    with tempfile.TemporaryDirectory() as folder:
      store = ChunkStore(folder, reserve=0)
      recorder = AutoRecorder(store, {'boot_id': 'test'})
      recorder.update(services(), 100)
      recorder.close()
      class Controller:
        root = Path(folder)
        def start(self): pass
        def close(self): pass
        def status(self): return {'state': 'stopped'}
        def chunks(self): return store.list_chunks()
      app = web.Application()
      register(app, Controller())
      async with TestClient(TestServer(app), auto_decompress=False) as client:
        response = await client.get('/api/automatic_drive/chunks')
        row = (await response.json())['chunks'][0]
        response = await client.get('/api/automatic_drive/chunks/' + row['id'])
        full = await response.read()
        self.assertEqual(hashlib.sha256(full).hexdigest(), row['sha256'])
        response = await client.get('/api/automatic_drive/chunks/' + row['id'], headers={'Range': 'bytes=7-'})
        self.assertEqual(response.status, 206)
        self.assertEqual(await response.read(), full[7:])
        self.assertEqual(response.headers['Content-Range'], f'bytes 7-{len(full)-1}/{len(full)}')

  async def test_startup_is_automatic_and_partial_files_are_not_downloadable(self):
    with tempfile.TemporaryDirectory() as folder:
      class Controller:
        root = Path(folder)
        started = False
        closed = False
        def start(self): self.started = True
        def close(self): self.closed = True
        def status(self): return {'state': 'recording'}
        def chunks(self): return []
      controller = Controller()
      app = web.Application()
      register(app, controller)
      async with TestClient(TestServer(app)) as client:
        self.assertTrue(controller.started)
        response = await client.get('/api/automatic_drive/status')
        self.assertEqual((await response.json())['state'], 'recording')
        response = await client.get('/api/automatic_drive/chunks/' + 'a' * 32)
        self.assertEqual(response.status, 404)
      self.assertTrue(controller.closed)


if __name__ == '__main__':
  unittest.main()
