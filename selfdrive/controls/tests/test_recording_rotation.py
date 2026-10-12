"""Reproduce selected-CAN loss at a real on-disk chunk boundary."""
import gzip
import json
from types import SimpleNamespace
import ast
from pathlib import Path
from openpilot.selfdrive.carrot.can_diagnostics.automatic import AutoRecorder, ChunkStore
from openpilot.selfdrive.carrot.can_diagnostics.automatic_runtime import recording_storage_status, selected_tx_identity


def make_recorder(tmp_path, **kwargs):
  store = ChunkStore(tmp_path, reserve=0, chunk_bytes=2048, **kwargs)
  recorder = AutoRecorder(store, {'boot_id': 'rotation-test', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV'})
  services = {'deviceState': {'valid': True, 'mono_ns': 100_000_000_000, 'data': {'started': True}}}
  recorder.update(services, 100.)
  recorder.begin_trace(100_000_000_000, 100., 'test')
  return recorder, store


def read_rows(root):
  rows = []
  for path in root.glob('*.jsonl.gz'):
    with gzip.open(path, 'rt', encoding='utf-8') as stream:
      rows += [json.loads(line) for line in stream]
  return rows


def test_burst_crosses_chunks_without_waiting_for_context_update(tmp_path):
  recorder, store = make_recorder(tmp_path)
  for index in range(80):
    now = 100. + index * .001
    recorder.can_frame(2, 0xCB, index.to_bytes(24, 'little'), int(now * 1e9), now)
  recorder.close()
  frames = [row for row in read_rows(tmp_path) if row['kind'] == 'button_trace_can']
  assert len(frames) == 80
  assert {int.from_bytes(bytes.fromhex(row['data']), 'little') for row in frames} == set(range(80))
  assert all(row['schema'] == 1 for row in read_rows(tmp_path) if row['kind'] == 'header')
  assert len(store.list_chunks()) > 1


def test_quota_failure_is_explicit_and_does_not_grow_unbounded(tmp_path):
  recorder, store = make_recorder(tmp_path, quota=70000)
  for index in range(800):
    now = 100. + index * .001
    recorder.can_frame(2, 0xCB, index.to_bytes(24, 'little'), int(now * 1e9), now)
  assert recorder.status()['state'] == 'storage_full_preserving_records'
  assert store.error == 'storage_full_preserving_records'
  assert recorder.trace['dropped_full'] > 0
  recorder.close()
  assert store.usage < store.quota


def test_stale_and_future_observations_are_still_rejected(tmp_path):
  recorder, store = make_recorder(tmp_path)
  recorder.can_frame(2, 0xCB, bytes(24), 99_000_000_000, 100.)
  recorder.can_frame(2, 0xCB, bytes(24), 101_000_000_000, 100.)
  recorder.close()
  assert recorder.stale_packets == 2
  assert not [row for row in read_rows(tmp_path) if row['kind'] == 'button_trace_can']


def test_panda_edge_at_full_boundary_is_retained(tmp_path):
  recorder, store = make_recorder(tmp_path)
  while not store.full(100.):
    store.append({'kind': 'padding', 'data': 'x' * 100}, 100.)
  recorder.panda_snapshot([{'controls_allowed': False, 'safety_param': 2238}], 100_000_000_000, 100.)
  recorder.panda_snapshot([{'controls_allowed': True, 'safety_param': 2238}], 100_010_000_000, 100.01)
  recorder.close()
  edges = [row for row in read_rows(tmp_path) if row.get('name') == 'panda_state_edge']
  assert any(row['field'] == 'controls_allowed' and row['after'] is True for row in edges)


def test_stale_sources_are_counted_separately_and_legacy_total_remains(tmp_path):
  recorder, store = make_recorder(tmp_path)
  recorder.can_frame(2, 0xCB, bytes(24), 99_000_000_000, 100.)
  recorder.panda_snapshot([{}], 99_000_000_000, 100.)
  assert recorder.status()['stale_can_packets'] == 2
  assert recorder.status()['stale_can_frames'] == 1
  assert recorder.status()['stale_panda_snapshots'] == 1
  recorder.close()


def test_retention_warning_matches_stock_thresholds_and_does_not_change_them(tmp_path):
  tree = ast.parse((Path(__file__).resolve().parents[3] / 'system/loggerd/deleter.py').read_text())
  constants = {}
  for row in tree.body:
    if isinstance(row, ast.Assign) and isinstance(row.targets[0], ast.Name) and row.targets[0].id in ('MIN_BYTES', 'MIN_PERCENT'):
      constants[row.targets[0].id] = eval(compile(ast.Expression(row.value), '<stock-threshold>', 'eval'), {'__builtins__': {}})
  assert constants == {'MIN_BYTES': 5 * 1024**3, 'MIN_PERCENT': 30}
  small = recording_storage_status(tmp_path, SimpleNamespace(total=10 * 1024**3, free=4 * 1024**3))
  assert small['status'] == 'low_space' and small['needed_bytes'] == 1024**3
  large = recording_storage_status(tmp_path, SimpleNamespace(total=40 * 1024**3, free=11 * 1024**3))
  assert large['status'] == 'low_space' and large['required_free_bytes'] == 12 * 1024**3
  boundary = recording_storage_status(tmp_path, SimpleNamespace(total=40 * 1024**3, free=12 * 1024**3))
  assert boundary['status'] == 'ok' and boundary['needed_bytes'] == 0


def test_sendcan_identity_survives_prebuffer_and_rotation_without_rx_inference(tmp_path):
  recorder, store = make_recorder(tmp_path)
  identity = {'epoch': 123, 'generation': 4, 'axis': 1}
  frame = SimpleNamespace(lx3Identity=SimpleNamespace(valid=True, **identity))
  assert selected_tx_identity(frame) == identity
  assert selected_tx_identity(SimpleNamespace()) is None
  assert selected_tx_identity(SimpleNamespace(lx3Identity=SimpleNamespace(valid=False))) is None
  recorder.can_frame(0, 0xCB, bytes(24), 100_001_000_000, 100.001, 'tx_requested', identity=identity)
  recorder.begin_trace(100_002_000_000, 100.002, 'next')
  recorder.close()
  frames = [row for row in read_rows(tmp_path) if row['kind'] == 'button_trace_can']
  assert frames and all(row['host_tx_identity'] == identity for row in frames)


def test_observer_trigger_at_full_chunk_rotates_and_keeps_cause(tmp_path):
  recorder, store = make_recorder(tmp_path)
  recorder.close_trace(100., 'test')
  while not store.full(100.):
    store.append({'kind': 'padding', 'data': 'x' * 100}, 100.)
  recorder.observer_tick(100.)
  recorder.observer_tick(100.4)
  assert recorder.trace is not None
  recorder.close()
  assert any(row.get('name') == 'diagnostic_trace_trigger' and row['reason'] == 'observer_gap' for row in read_rows(tmp_path))


def test_trace_keeps_legacy_stale_total_and_separate_sources(tmp_path):
  recorder, store = make_recorder(tmp_path)
  recorder.can_frame(2, 0xCB, bytes(24), 99_000_000_000, 100.)
  recorder.panda_snapshot([{}], 99_000_000_000, 100.)
  recorder.close()
  end = next(row for row in read_rows(tmp_path) if row.get('name') == 'button_trace_end')
  assert (end['stale_packets_delta'], end['stale_can_frames_delta'], end['stale_panda_snapshots_delta']) == (2, 1, 1)


def test_append_without_stream_is_explicit_loss(tmp_path):
  store = ChunkStore(tmp_path, reserve=0)
  assert store.append({'kind': 'event'}, 100.) is False
  assert store.dropped_no_stream == 1


def test_quota_state_counts_continuing_fresh_selected_observations(tmp_path):
  recorder, store = make_recorder(tmp_path, quota=70000)
  for index in range(800):
    now = 100. + index * .001
    recorder.can_frame(2, 0xCB, bytes(24), int(now * 1e9), now)
  assert recorder.state == 'storage_full_preserving_records'
  before = recorder.storage_dropped
  recorder.can_frame(2, 0xCB, bytes(24), 101_000_000_000, 101.)
  recorder.panda_snapshot([{}], 101_000_000_000, 101.)
  assert recorder.storage_dropped == before + 2
  recorder.can_frame(2, 0xCB, bytes(24), 99_000_000_000, 101.)
  assert recorder.storage_dropped == before + 2
  recorder.close()


def test_prebuffer_rotation_quota_failure_is_counted(tmp_path):
  recorder, store = make_recorder(tmp_path, quota=70000)
  while not store.full(100.):
    store.append({'kind': 'padding', 'data': 'x' * 100}, 100.)
  store.usage = store.quota
  recorder.append_trace_frame(recorder.can_row(100_000_000_000, 'rx', 2, 0xCB, bytes(24)), 100.)
  assert recorder.storage_dropped == 1 and recorder.trace['dropped_full'] == 1
  recorder.close()


def test_panda_rotation_quota_failure_is_counted(tmp_path):
  recorder, store = make_recorder(tmp_path, quota=70000)
  while not store.full(100.):
    store.append({'kind': 'padding', 'data': 'x' * 100}, 100.)
  store.usage = store.quota
  recorder.panda_snapshot([{}], 100_000_000_000, 100.)
  assert recorder.storage_dropped == 1
  recorder.close()


def test_diagnostic_trigger_rotation_failure_is_counted(tmp_path):
  recorder, store = make_recorder(tmp_path, quota=70000)
  recorder.close_trace(100., 'test')
  while not store.full(100.):
    store.append({'kind': 'padding', 'data': 'x' * 100}, 100.)
  store.usage = store.quota
  recorder.diagnostic_trigger('test_storage', 100_000_000_000, 100.)
  assert recorder.storage_dropped == 1 and recorder.trace is None
  recorder.close()
