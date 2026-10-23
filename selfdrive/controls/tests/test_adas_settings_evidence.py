"""Passive state evidence must preserve short pulses without asserting an ACK."""
import ast
from pathlib import Path
import re
import builtins
from types import SimpleNamespace
import pytest

from selfdrive.carrot.can_diagnostics.automatic import AutoRecorder


def test_old_native_registry_is_observed_without_reading_unregistered_keys():
  from selfdrive.carrot.can_diagnostics.automatic_runtime import settings_snapshot
  class OldParams:
    def all_keys(self): return [b'OpenpilotEnabledToggle', b'RecordRoadCam', b'DisableLogging']
    def get(self, key):
      assert key in ('OpenpilotEnabledToggle', 'RecordRoadCam', 'DisableLogging')
      return {'OpenpilotEnabledToggle': False, 'RecordRoadCam': 1, 'DisableLogging': False}[key]
  row = settings_snapshot(OldParams())
  assert row['OpenpilotEnabledToggle'] is False and row['RecordRoadCam'] == 1
  assert row['driving_preferences'] == {'status': 'module_not_installed'}
  assert 'ApproachComfortEnabled' in row['unsupported_on_this_build']


def test_modern_registered_settings_keep_their_values():
  from selfdrive.carrot.can_diagnostics.automatic_runtime import settings_snapshot, PARAMS
  p = SimpleNamespace(all_keys=lambda: list(PARAMS), get=lambda key: b'7')
  row = settings_snapshot(p, SimpleNamespace(read=lambda: {'LaunchFollowStrength': 10}))
  assert all(row[key] == '7' for key in PARAMS)
  assert row['unsupported_on_this_build'] == []
  assert row['driving_preferences']['LaunchFollowStrength'] == 10


@pytest.mark.parametrize('missing_name,optional', [
  ('openpilot.selfdrive.controls.lib.approach_comfort', True), ('nested_dependency', False)])
def test_only_exact_absent_optional_module_is_tolerated(monkeypatch, missing_name, optional):
  from selfdrive.carrot.can_diagnostics.automatic_runtime import optional_driving_preferences
  original = builtins.__import__
  def fake_import(name, *args, **kwargs):
    if name == 'openpilot.selfdrive.controls.lib.approach_comfort':
      raise ModuleNotFoundError('missing dependency', name=missing_name)
    return original(name, *args, **kwargs)
  monkeypatch.setattr(builtins, '__import__', fake_import)
  if optional:
    assert optional_driving_preferences() is None
  else:
    with pytest.raises(ModuleNotFoundError): optional_driving_preferences()


def test_host_passive_context_does_not_claim_actual_panda_or_relay_state():
  from selfdrive.carrot.can_diagnostics.automatic_runtime import vehicle_capture_context
  cp = SimpleNamespace(passive=True, dashcamOnly=False,
                       safetyConfigs=[SimpleNamespace(safetyModel='noOutput', safetyParam=0)])
  row = vehicle_capture_context(cp)
  assert row['passive'] is True and row['dashcam_only'] is False
  assert row['safety_configs'] == [{'model': 'noOutput', 'param': 0}]
  assert row['scope'] == 'host_CarParams_not_live_Panda_mode_or_physical_relay_verification'


def test_missing_car_params_clears_prior_trip_capture_plan():
  from contextlib import nullcontext
  from selfdrive.carrot.can_diagnostics.automatic_runtime import update_vehicle_metadata
  cp = SimpleNamespace(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', passive=True,
                       dashcamOnly=False, safetyConfigs=[])
  schema = SimpleNamespace(from_bytes=lambda raw: nullcontext(cp))
  metadata = {}
  update_vehicle_metadata(metadata, b'present', schema, 100)
  assert metadata['vehicle_capture_context']['passive'] is True
  assert metadata['car_fingerprint'] == cp.carFingerprint
  update_vehicle_metadata(metadata, None, schema, 200)
  assert metadata['car_fingerprint'] is None
  assert metadata['vehicle_capture_context'] == {'status': 'car_params_absent', 'observed_mono_ns': 200}


class Store:
  def __init__(self): self.rows = []
  def append(self, row, now): self.rows.append(row); return True
  def full(self, now): return False


def recorder(car='HYUNDAI_PALISADE_LX3_HEV'):
  store = Store()
  result = AutoRecorder(store, {'car_fingerprint': car})
  result.state = 'recording'
  return result, store


def observations(store):
  return [row for row in store.rows if row.get('name') == 'adas_state_observation']


def send(r, t, address, value, length=16, bus=2, direction='rx'):
  r.can_frame(bus, address, value.to_bytes(length, 'little'), int(t*1e9), t, direction,
              source_mono_ns=int(t*1e9)+42)


def test_setting_candidates_missing_from_old_recorder_are_retained():
  r, store = recorder()
  send(r, 100., 0x160, 0)
  send(r, 100.02, 0x160, 2 << 24)
  send(r, 100.04, 0x1E0, (3 << 24) | (2 << 27) | (1 << 30) | (2 << 47))
  rows = observations(store)
  assert len(rows) == 3
  assert rows[0]['phase'] == 'baseline'
  assert rows[1]['fields'] == {'AEB_SETTING': 2}
  assert rows[1]['before'] == {'AEB_SETTING': 0}
  assert rows[2]['fields']['HDA_OptUsmSta'] == 3
  assert rows[2]['fields']['LFA_OptUsmSta'] == 2
  assert rows[2]['fields']['HDA_LFA_SymSta'] == 2
  assert rows[1]['source_log_mono_ns'] == 100_020_000_042
  assert rows[1]['classification'] == 'dbc_candidate_not_setting_ack_or_cause'
  assert rows[1]['integrity'] == 'not_validated_by_recorder'
  assert r.settings_trace_count == 1


def test_short_host_pulse_and_release_survive_ten_hz_sampling_without_trace():
  r, store = recorder()
  for t, value in ((100., 0), (100.02, 1 << 39), (100.06, 0)):
    send(r, t, 0x1AA, value, direction='tx_requested')
  rows = observations(store)
  assert [row['fields']['LFA_BTN'] for row in rows] == [0, 1, 0]
  assert len([row for row in store.rows if row['kind'] == 'can_sample']) == 1
  assert r.trace is None
  assert not any(row.get('name') == 'physical_button_edge' for row in store.rows)
  assert all(row['direction'] == 'tx_requested' for row in rows)


def test_source_echo_rejection_and_request_have_independent_baselines():
  r, store = recorder()
  for bus, direction, value in ((2, 'rx', 0), (2, 'tx_requested', 1 << 39),
                                 (130, 'rx', 1 << 39), (194, 'rx', 1 << 39)):
    send(r, 100., 0x1AA, value, bus=bus, direction=direction)
  rows = observations(store)
  assert {row['direction'] for row in rows} == {'rx', 'tx_requested', 'tx_echo', 'tx_rejected'}
  assert all(row['phase'] == 'baseline' for row in rows)
  assert r.trace is None


def test_all_declared_bits_match_installed_dbc_including_motorola_fields():
  text = (Path(__file__).resolve().parents[3] /
          'opendbc_repo/opendbc/dbc/generator/hyundai/hyundai_canfd_lx3_hev.dbc').read_text(encoding='utf8')
  messages = {}
  for block in text.split('BO_ ')[1:]:
    head = re.match(r'(\d+) (\w+): (\d+)', block)
    if head:
      messages[int(head[1])] = (int(head[3]), {m[1]: (int(m[2]), int(m[3]), int(m[4]))
        for m in re.finditer(r'SG_ (\w+) : (\d+)\|(\d+)@(\d)', block)})
  for address, (length, fields) in AutoRecorder.ADAS_STATE_FIELDS.items():
    assert length == messages[address][0]
    for name, start, size in fields:
      dbc_start, dbc_size, little = messages[address][1][name]
      assert size == dbc_size
      if little:
        positions = list(range(dbc_start, dbc_start+size))
      else:
        positions, bit = [], dbc_start
        for _ in range(size):
          positions.append(bit)
          bit = bit+15 if bit % 8 == 0 else bit-1
      assert sorted(positions) == list(range(start, start+size)), (address, name)


def test_stale_wrong_length_other_car_and_old_timestamp_do_not_create_edges():
  r, store = recorder()
  send(r, 100., 0x160, 0)
  r.can_frame(2, 0x160, bytes(16), 98_000_000_000, 100.1)
  send(r, 100.2, 0x160, 1 << 24, length=32)
  r.can_frame(2, 0x160, (1 << 24).to_bytes(16, 'little'), 99_990_000_000, 100.3)
  assert len(observations(store)) == 1
  assert r.adas_out_of_order == 1
  other, other_store = recorder('KIA_EV9')
  send(other, 100., 0x160, 0)
  send(other, 100.1, 0x1E0, 0)
  send(other, 100.2, 0x1AA, 0)
  assert not other_store.rows


def test_periodic_buttons_and_noisy_fields_are_bounded_and_cannot_exhaust_other_quota():
  r, store = recorder()
  for i in range(3000):
    send(r, 100.+i*.001, 0x1AA, (i % 2) << 39, direction='tx_requested')
  assert len(observations(store)) <= 3 * r.ADAS_OBSERVATIONS_PER_SECOND
  assert r.adas_observation_dropped > 0
  assert r.trace_count == r.diagnostic_trace_count == 0
  send(r, 104., 0x1AA, 1 << 39, direction='tx_requested')
  assert observations(store)[-1]['dropped_since_previous'] > 0
  for i in range(15):
    now = 120.+40*i
    r.diagnostic_trigger('adas_setting_candidate_edge', int(now*1e9), now)
    r.close_trace(now+9., 'test')
  assert r.settings_trace_count == r.SETTINGS_TRACE_MAX_PER_TRIP
  r.diagnostic_trigger('transport_counter_change', 800_000_000_000, 800.)
  r.close_trace(809., 'test')
  r.diagnostic_trigger('oem_fault_edge', 820_000_000_000, 820.)
  assert r.steering_trace_count == 1
  assert r.trace_count-r.diagnostic_trace_count == 0


def test_first_baseline_and_counter_only_changes_do_not_create_setting_toggle():
  r, store = recorder()
  for i in range(30): send(r, 100.+i*.02, 0x1E0, i << 16)
  assert len(observations(store)) == 1
  assert r.trace is None


def test_multiple_frames_in_one_packet_keep_equal_timestamps_in_receive_order():
  r, store = recorder()
  for value in (0, 1 << 39, 0): send(r, 100., 0x1AA, value, direction='tx_requested')
  rows = observations(store)
  assert [row['fields']['LFA_BTN'] for row in rows] == [0, 1, 0]
  assert [row['observation_index'] for row in rows] == [1, 2, 3]
  assert rows[1]['same_timestamp_as_previous'] is True
  assert r.adas_out_of_order == 0


def test_missing_interval_rebaselines_without_claiming_a_toggle():
  r, store = recorder()
  send(r, 100., 0x160, 0)
  send(r, 102., 0x160, 1 << 24)
  row = observations(store)[-1]
  assert row['phase'] == 'baseline_after_gap' and row['before'] is None
  assert r.trace is None


def test_false_store_append_exposes_loss_and_does_not_claim_saved_observation():
  r, store = recorder()
  original = store.append
  store.append = lambda row, now: False
  send(r, 100., 0x1AA, 0)
  assert r.adas_observation_count == 0 and r.adas_observation_dropped == 1
  store.append = original
  send(r, 100.02, 0x1AA, 1 << 39)
  assert observations(store)[0]['dropped_since_previous'] == 1


def test_real_chunks_rotate_and_pc_report_keeps_untraced_state_and_trip_reset(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import ChunkStore
  from selfdrive.carrot.can_diagnostics.button_trace_report import summarize
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  store = ChunkStore(tmp_path, reserve=0, seconds=1, chunk_bytes=4096)
  r = AutoRecorder(store, {'boot_id': 'A', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV', 'route': 'one'})
  r.update(services(), 100.)
  for t, value in ((100.01, 0), (100.03, 1 << 39), (100.07, 0)):
    send(r, t, 0x1AA, value, direction='tx_requested')
  r.metadata['route'] = 'two'
  r.update(services(101.1), 101.1)
  send(r, 101.11, 0x1AA, 0, direction='tx_requested')
  assert r.adas_state[('tx_requested', 2, 0x1AA)][0] == 101_110_000_000
  r.update(services(102., started=False), 102.)
  r.update(services(103.), 103.)
  assert r.adas_observation_count == 0 and r.adas_state == {}
  send(r, 103.01, 0x1AA, 0, direction='tx_requested')
  r.close()
  report = summarize(tmp_path)
  rows = report['adas_state_evidence']['observations']
  assert len(rows) == 5
  assert [row['fields']['LFA_BTN'] for row in rows[:3]] == [0, 1, 0]
  assert rows[3]['phase'] == rows[4]['phase'] == 'baseline'
  assert rows[3]['_route'] == 'two'
  assert report['traces'] == []
  assert report['adas_state_evidence']['report_omitted'] == 0


def test_existing_recorder_remains_passive():
  for filename in ('automatic.py', 'automatic_runtime.py'):
    source = Path(__file__).resolve().parents[2] / ('carrot/can_diagnostics/'+filename)
    tree = ast.parse(source.read_text())
    calls = {n.func.attr if isinstance(n.func, ast.Attribute) else n.func.id
             for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, (ast.Attribute, ast.Name))}
    assert not calls & {'pub_sock', 'PubMaster', 'make_can_msg', 'put', 'put_nonblocking',
                        'put_bool', 'put_int', 'sendcan', 'can_send', 'can_send_many', 'Panda', 'SpiDevice'}


def test_host_storm_cannot_starve_received_setting_edge():
  r, store = recorder()
  send(r, 100., 0x160, 0)
  for i in range(100): send(r, 100.001+i*.001, 0x1AA, (i % 2) << 39, direction='tx_requested')
  send(r, 100.15, 0x160, 1 << 24)
  rows = [row for row in observations(store) if row['address'] == 0x160]
  assert len(rows) == 2 and rows[-1]['fields']['AEB_SETTING'] == 1
  assert r.settings_trace_count == 1 and r.adas_observation_dropped > 0
  assert r.adas_dropped_by_direction['rx'] == 0 and r.adas_dropped_by_direction['host'] > 0


def test_unknown_fingerprint_wrong_length_and_stale_coverage_are_explicit():
  r, store = recorder(None)
  send(r, 100., 0x160, 0)
  assert r.adas_unknown_fingerprint_frames == 1 and not store.rows
  r.metadata['car_fingerprint'] = 'HYUNDAI_PALISADE_LX3_HEV'
  send(r, 100.1, 0x160, 0, length=32)
  r.can_frame(2, 0x160, bytes(16), 98_000_000_000, 100.2)
  send(r, 100.3, 0x160, 0)
  coverage = observations(store)[0]['coverage_counters']
  assert coverage['unknown_fingerprint_frames'] == coverage['length_mismatch'] == coverage['stale_can_frames'] == 1


def test_next_chunk_keeps_unchanged_state_as_baseline(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import ChunkStore
  from selfdrive.carrot.can_diagnostics.button_trace_report import summarize
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  store = ChunkStore(tmp_path, reserve=0, seconds=1, chunk_bytes=100000)
  r = AutoRecorder(store, {'boot_id': 'A', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV'})
  r.update(services(), 100.)
  send(r, 100.1, 0x1E0, 2 << 27)
  send(r, 100.8, 0x1E0, 2 << 27)
  r.update(services(101.2), 101.2)
  send(r, 101.3, 0x1E0, 2 << 27)
  r.close()
  rows = summarize(tmp_path)['adas_state_evidence']['observations']
  assert len(rows) == 2
  assert rows[-1]['phase'] == 'baseline_chunk_boundary'
  assert rows[-1]['before'] is None and rows[-1]['fields']['LFA_OptUsmSta'] == 2


def test_eleven_host_baselines_fit_without_artificial_loss():
  r, store = recorder()
  for bus, direction, addresses in ((0, 'tx_requested', (0x1E0, 0x1AA, 0x12A, 0x161, 0x162)),
                                     (128, 'rx', (0x160, 0x1E0, 0x1AA, 0x12A, 0x161, 0x162))):
    for address in addresses:
      send(r, 100., address, 0, length=r.ADAS_STATE_FIELDS[address][0], bus=bus, direction=direction)
  assert len(observations(store)) == 11
  assert r.adas_dropped_by_direction == {'rx': 0, 'host': 0}


def test_direct_chunk_reopen_with_short_gap_still_gets_baseline(tmp_path):
  from selfdrive.carrot.can_diagnostics.automatic import ChunkStore
  from selfdrive.carrot.can_diagnostics.button_trace_report import summarize
  from selfdrive.carrot.can_diagnostics.test_automatic import services
  store = ChunkStore(tmp_path, reserve=0, seconds=10, chunk_bytes=100000)
  r = AutoRecorder(store, {'boot_id': 'A', 'car_fingerprint': 'HYUNDAI_PALISADE_LX3_HEV'})
  r.update(services(), 100.)
  send(r, 100.1, 0x1E0, 2 << 27)
  store.seal('test_direct_reopen_without_trip_change')
  r.update(services(100.3), 100.3)
  send(r, 100.4, 0x1E0, 2 << 27)
  r.close()
  rows = summarize(tmp_path)['adas_state_evidence']['observations']
  assert len(rows) == 2 and rows[-1]['phase'] == 'baseline_chunk_boundary'
