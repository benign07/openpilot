import gzip
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from .automatic import AutoRecorder, ChunkStore, control_mode, host_disabled_panda_allowed
from .automatic_routes import register
from .automatic_runtime import read_param, selected_fields
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
  def test_permission_observation_is_not_engagement_authority(self):
    sample = services()
    sample['pandaStates'] = {'mono_ns': 100_000_000_000, 'valid': True, 'data': [{'controlsAllowed': True}]}
    self.assertTrue(host_disabled_panda_allowed(sample, 100))
    sample['selfdriveState']['data']['enabled'] = True
    self.assertFalse(host_disabled_panda_allowed(sample, 100))
    sample['selfdriveState']['data']['enabled'] = False
    sample['pandaStates']['data'][0]['controlsAllowed'] = False
    self.assertFalse(host_disabled_panda_allowed(sample, 100))

  def test_stale_or_missing_permission_observation_is_unknown(self):
    sample = services()
    self.assertIsNone(host_disabled_panda_allowed(sample, 100))
    sample['pandaStates'] = {'mono_ns': 100_000_000_000, 'valid': True, 'data': [{'controlsAllowed': True}]}
    self.assertIsNone(host_disabled_panda_allowed(sample, 100.251))
    sample['pandaStates']['data'][0]['controlsAllowed'] = None
    self.assertIsNone(host_disabled_panda_allowed(sample, 100))

  def test_passive_panda_is_not_a_permission_observation(self):
    sample = services()
    passive = [{'safetyModel': 'silent', 'controlsAllowed': True}, {'safetyModel': 'noOutput', 'controlsAllowed': True}]
    sample['pandaStates'] = {'mono_ns': 100_000_000_000, 'valid': True, 'data': passive}
    self.assertIsNone(host_disabled_panda_allowed(sample, 100))
    sample['pandaStates']['data'] = passive + [{'safetyModel': 'hyundaiCanfd', 'controlsAllowed': False}]
    self.assertFalse(host_disabled_panda_allowed(sample, 100))

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

  def test_inactive_host_permission_window_is_saved_in_existing_sample(self):
    sample = services()
    sample['pandaStates'] = {'mono_ns': 100_000_000_000, 'valid': True, 'data': [{'controlsAllowed': True}]}
    self.recorder.update(sample, 100)
    row = next(r for r in self.rows() if r['kind'] == 'sample')
    self.assertIs(row['host_disabled_panda_allowed'], True)
    self.assertIs(row['services']['selfdriveState']['data']['enabled'], False)

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

  def test_physical_button_frames_and_zero_force_active_edges_are_retained(self):
    self.recorder.update(services(), 100)
    for index in range(3):
      now = 100 + index * .04
      data = bytearray(16); data[2] = index * 2
      self.recorder.can_frame(0, 0x10B, bytes(data), int(now * 1e9), now)
    inactive = bytearray(24); inactive[3] = 0x10
    active = bytearray(inactive); active[3] = 0x20
    for now, data in ((100.1, inactive), (100.11, active)):
      self.recorder.can_frame(0, 0xCB, bytes(data), int(now * 1e9), now, 'tx_requested')
    rows = self.rows()
    self.assertEqual(len([r for r in rows if r.get('address') == 0x10B and r['kind'] == 'can_sample']), 3)
    edges = [r for r in rows if r.get('name') == 'actuation_state_observation' and r['address'] == 0xCB]
    self.assertEqual([r['after'] for r in edges], [[1, 0], [2, 0]])

  def test_lfa_camera_fault_is_recorded_separately(self):
    self.recorder.update(services(), 100)
    self.recorder.can_frame(2, 0x162, bytes(32), 100_000_000_000, 100)
    self.recorder.can_frame(2, 0x162, (1 << 234).to_bytes(32, 'little'), 100_010_000_000, 100.01)
    edges = [r for r in self.rows() if r.get('name') == 'oem_fault_observation']
    self.assertEqual(edges[-1]['after'], [0, 0, 1])

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
