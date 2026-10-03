import ast
import json
from pathlib import Path
import tempfile
import unittest

from tools.lx3_can_response import analyze, channel, checksum, decode, frame, load_rows, render_html


def packet(t, address=0xCB, src=0, kind='can', active=2, angle=0, cap=25, driver=0, fault=0):
  data = bytearray({0xCB: 24, 0xEA: 24, 0x162: 32, 0x1A0: 32, 0x10B: 16}[address])
  if address == 0xCB:
    data[3], data[6] = active << 4, cap
    data[4:6] = (angle & 0x3FFF).to_bytes(2, 'little')
  elif address == 0xEA:
    data[18] = active | fault << 5
    data[16:18] = angle.to_bytes(2, 'little', signed=True)
    data[10:12] = (driver + 4095).to_bytes(2, 'little')
  data[:2] = checksum(address, data).to_bytes(2, 'little')
  return {'t': t, 'kind': kind, 'address': address, 'bus': src, 'hex': data.hex()}


class TestCanResponse(unittest.TestCase):
  def test_channel_separates_requests_echoes_rejections_and_rx(self):
    self.assertEqual(channel('sendcan', 0), ('host_request', 0))
    self.assertEqual(channel('can', 130), ('wire_echo', 2))
    self.assertEqual(channel('can', 194), ('tx_rejected', 2))
    self.assertEqual(channel('can', 2), ('ecu_rx', 2))
    self.assertEqual(channel('can', 64), ('unknown', 64))

  def test_recorded_crc_vector_and_production_checksum_all_lengths(self):
    observed = bytes.fromhex('f8199e101400000000000000000000000000000000000000')
    self.assertEqual(checksum(0xCB, observed), 0x19F8)
    root = Path(__file__).resolve().parents[2]
    source = root / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py'
    fn = next(n for n in ast.parse(source.read_text(encoding='utf-8')).body if isinstance(n, ast.FunctionDef) and n.name == 'hkg_can_fd_checksum')
    table = []
    for value in range(256):
      crc = value << 8
      for _ in range(8):
        crc = ((crc << 1) ^ (0x1021 if crc & 0x8000 else 0)) & 65535
      table.append(crc)
    env = {'CRC16_XMODEM': table}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(source), 'exec'), env)
    for size in (8, 16, 24, 32):
      for address in (0xCB, 0xEA, 0x10B, 0x162, 0x1A0):
        data = bytes(range(size))
        self.assertEqual(checksum(address, data), env['hkg_can_fd_checksum'](address, None, data))

  def test_angle_sign_and_driver_units(self):
    for angle in (-1749, -1, 0, 1, 1749):
      for address in (0xCB, 0xEA):
        f = frame(packet(1, address=address, angle=angle, driver=-497))
        self.assertEqual(f['signals']['angle_deg'], angle / 10)
        if address == 0xEA:
          self.assertEqual(f['signals']['driver_torque_raw'], -497)

  def test_bad_ecu_crc_length_not_used_but_host_intent_can_be_decoded(self):
    bad = packet(1, address=0xEA)
    raw = bytearray.fromhex(bad['hex'])
    raw[18] ^= 0x20
    bad['hex'] = raw.hex()
    self.assertEqual(frame(bad)['signals'], {})
    self.assertEqual(frame(dict(bad, kind='sendcan'))['signals']['lfa_fault'], 1)
    short = dict(bad, hex=bad['hex'][:-2])
    result = analyze([bad, short])
    self.assertEqual(result['ecu_transitions'], [])
    self.assertEqual(sum(result['integrity_issues'].values()), 2)

  def test_echo_is_not_response_or_proof_of_op_origin(self):
    rows = [packet(1, kind='sendcan'), packet(1.001, src=2), packet(1.002, src=128)]
    event = analyze(rows)['command_events'][0]
    self.assertEqual(event['wire_origin'], 'ambiguous_stock_and_host')
    self.assertEqual(event['wire_match_delay_s'], .002)
    self.assertEqual(event['mdps_after_count'], 0)
    self.assertFalse(event['mdps_window_dense'])
    self.assertEqual(event['first_changes'], {})

  def test_actual_response_delay_and_same_batch_does_not_claim_causality(self):
    rows = [packet(.99, 0xEA, active=1), packet(1, kind='sendcan'), packet(1, src=128),
            packet(1, 0xEA, active=1), packet(1.03, 0xEA, active=2, angle=15, driver=60)]
    event = analyze(rows)['command_events'][0]
    self.assertEqual(event['wire_match_delay_s'], 0)
    self.assertEqual(event['first_changes']['lfa_state']['delay_from_request_s'], .03)
    self.assertEqual(event['first_changes']['angle_deg']['after'], 1.5)
    self.assertFalse(event['mdps_window_dense'])

  def test_first_fault_observation_is_not_rising_edge(self):
    result = analyze([packet(1, 0xEA, fault=1), packet(2, 0xEA, fault=0), packet(3, 0xEA, fault=1)])
    changes = result['ecu_transitions']
    self.assertTrue(changes[0]['first_observation'])
    self.assertIsNone(changes[0]['changes']['lfa_fault']['before'])
    self.assertFalse(changes[2]['first_observation'])
    self.assertEqual(changes[2]['gap_from_previous_s'], 1)

  def test_rejections_group_addresses_without_decoding_as_responses(self):
    result = analyze([packet(1, src=192), packet(1, 0xEA, src=194)])
    self.assertEqual(result['rejected_by_address'], {'tx_rejected/bus0/0xCB': 1, 'tx_rejected/bus2/0xEA': 1})
    self.assertEqual(result['ecu_transitions'], [])

  def test_host_placeholder_crc_and_accepted_neutral_are_separate(self):
    request = packet(1, kind='sendcan', active=1, cap=0)
    request['hex'] = '0000' + request['hex'][4:]
    rows = [{'t': .99, 'kind': 'carState', 'data': {'latEnabled': True}}, request,
            packet(.995, src=2, active=2), packet(.997, 0xEA, active=1), packet(.998, 0xEA, src=130, active=2)]
    report = analyze(rows)
    self.assertEqual(report['integrity_issues'], {})
    self.assertEqual(sum(report['host_request_crc_differences'].values()), 1)
    phase = report['steering_observation']['phases'][0]
    self.assertEqual(phase['phase'], 'accepted_session_neutral')
    self.assertEqual(phase['nearby_state_counts'], {'camera=2,physical_mdps=1,camera_mdps_echo=2': 1})

  def test_mode_context_expires_and_event_limit_disclosed(self):
    rows = [{'t': .99, 'kind': 'carControl', 'data': {'latActive': True, 'longActive': False}},
            packet(1, kind='sendcan'), packet(1.3, kind='sendcan', angle=30), packet(1.6, kind='sendcan', active=1, cap=0)]
    report = analyze(rows, max_events=2)
    self.assertEqual([e['mode'] for e in report['command_events']], ['lateral_only', 'unknown'])
    self.assertTrue(report['truncated'])
    self.assertEqual(report['selected_command_events'], 3)

  def test_json_jsonl_input_html_escaping_and_invalid_time(self):
    with tempfile.TemporaryDirectory() as directory:
      path = Path(directory) / 'input.json'
      rows = [packet(1)]
      path.write_text(json.dumps({'rows': rows}), encoding='utf-8')
      self.assertEqual(load_rows(path), rows)
      path = Path(directory) / 'input.jsonl'
      path.write_text(json.dumps(rows[0]) + '\n', encoding='utf-8')
      self.assertEqual(load_rows(path), rows)
    report = analyze([])
    report['input_provenance'] = [{'path': '<script>alert(1)</script>'}]
    self.assertNotIn('<script>alert(1)</script>', render_html(report))
    with self.assertRaises(ValueError):
      analyze([packet(float('nan'))])

  def test_selected_frames_match_real_dbc_definitions(self):
    from selfdrive.carrot.tests.test_lx3_can_time import ENV, DBC_FILE, definitions, ROOT
    env = dict(ENV)
    definitions(ROOT / 'opendbc_repo/opendbc/can/packer.py', env)
    packer = env['CANPacker'](str(DBC_FILE))
    fields = {
      'SCC_CONTROL': {'ACCMode': 2, 'SysFailState': 1, 'TakeOverReq': 2, 'DriverAlert': 3,
                      'aReqValue': -1.25, 'aReqRaw': -.5, 'StopReq': 1},
      'MDPS': {'LFA2_ACTIVE': 2, 'STEERING_COL_TORQUE': -497, 'LKA_FAULT': 1, 'LFA2_FAULT': 1,
               'STEERING_ANGLE_2': 12.3},
    }
    for name, values in fields.items():
      addr, data, _ = packer.make_can_msg(name, 0, values)
      decoded = decode(addr, data)
      if name == 'SCC_CONTROL':
        self.assertEqual(decoded, {'mode': 2, 'system_fault_raw': 1, 'takeover_raw': 2, 'driver_alert_raw': 3,
                                   'accel_value': -1.25, 'accel_raw': -.5, 'stop': 1})
      else:
        self.assertEqual(decoded, {'lfa_state': 2, 'driver_torque_raw': -497, 'angle_deg': -12.3,
                                   'lka_fault': 1, 'lfa_fault': 1})


if __name__ == '__main__':
  unittest.main()
