#!/usr/bin/env python3
"""Offline LX3 command/response explorer. Local log files only; no CAN/network I/O.

JSON input: {"rows": [{"t": seconds, "kind": "can"|"sendcan", "bus": src,
"address": integer, "hex": payload}, ...]}. State rows use "data" dictionaries.
Also accepts JSONL or local rlog[.zst/.bz2] with --schema path/to/log.capnp.
All inputs must belong to one boot. Times are logger monotonic batch times.
"""
import argparse
import bisect
import bz2
from collections import Counter, defaultdict
import hashlib
import html
import json
import math
from pathlib import Path

LENGTHS = {0xCB: 24, 0xEA: 24, 0x1A0: 32, 0x162: 32, 0x10B: 16}
WARNINGS = {'FSS': 213, 'FCA': 216, 'LSS': 219, 'SLA': 222, 'DAW': 225,
            'HBA': 228, 'SCC': 231, 'LFA': 234, 'DAS': 246}
LIMITATIONS = [
  '송신 요청(sendcan), 실제 송신 에코, ECU 원본 수신은 별개입니다. 에코도 ECU 수락 응답은 아닙니다.',
  '송신 에코에는 순정 프레임 전달도 포함됩니다. 같은 값의 호스트 요청과 일치해도 발신 주체는 확정할 수 없습니다.',
  '시간은 로그 배치 시각입니다. 같은 배치의 순서와 정밀한 지연, 인과관계를 입증하지 않습니다.',
  '수신 공백·부분 로그·qlog에서는 응답 미관측을 차량 무응답으로 해석할 수 없습니다.',
  '조향각·상태 변화는 시간상 연관입니다. 운전자 입력, 순정 제어와 도로 영향이 함께 작용합니다.',
  'MDPS 열 토크와 보조 한도는 포트의 원시 단위입니다. Nm로 해석하지 않습니다.',
  '경고 값은 DBC 필드의 원시 값이며 DTC 진단 결과가 아닙니다. 첫 관측은 발생 시점이 아닙니다.',
  '호스트 요청의 CRC 차이는 Panda에서 카운터/CRC를 완성하기 전 값일 수 있습니다. 실제 버스 CRC 오류와 따로 집계합니다.',
  '요청 간 공백에는 해제·일시 정지 구간도 포함됩니다. 스케줄 지연이나 프레임 손실로 바로 단정하지 않습니다.',
  'Panda 전달 버퍼 안의 폐기·만료는 송신 거절 프레임으로 기록되지 않을 수 있습니다. 거절 0은 전송 성공을 뜻하지 않습니다.',
]


def checksum(address, data):
  """Hyundai CAN-FD CRC16 XMODEM, same payload/address/salts as production."""
  crc = 0
  for value in data[2:] + bytes((address & 255, (address >> 8) & 255)):
    crc ^= value << 8
    for _ in range(8):
      crc = ((crc << 1) ^ (0x1021 if crc & 0x8000 else 0)) & 0xFFFF
  return crc ^ {8: 0x5F29, 16: 0x041D, 24: 0x819D, 32: 0x9F5B}.get(len(data), 0)


def channel(kind, src):
  if kind == 'sendcan' and 0 <= src < 8:
    return 'host_request', src
  if kind == 'can':
    if 0 <= src < 8:
      return 'ecu_rx', src
    if 128 <= src < 136:
      return 'wire_echo', src - 128
    if 192 <= src < 200:
      return 'tx_rejected', src - 192
  return 'unknown', src


def decode(address, data):
  bits = int.from_bytes(data, 'little')
  if address == 0xCB:
    raw = (bits >> 32) & 0x3FFF
    raw -= 0x4000 if raw & 0x2000 else 0
    # DBC LKAS uses -0.1; the builder passes -apply_angle. Normalize to
    # CarState.steeringAngleDeg / controller convention, not DBC sign.
    return {'active': (data[3] >> 4) & 3, 'angle_deg': raw / 10, 'cap_raw': data[6]}
  if address == 0xEA:
    return {'lfa_state': data[18] & 3, 'angle_deg': int.from_bytes(data[16:18], 'little', signed=True) / 10,
            'driver_torque_raw': ((bits >> 80) & 0x1FFF) - 4095,
            'lka_fault': (bits >> 54) & 1, 'lfa_fault': (bits >> 149) & 1}
  if address == 0x1A0:
    return {'mode': (data[8] >> 4) & 7, 'system_fault_raw': data[8] & 3,
            'takeover_raw': data[9] & 3, 'driver_alert_raw': (data[9] >> 5) & 3,
            'accel_value': round(((bits >> 128) & 0x7FF) * .01 - 10.23, 2),
            'accel_raw': round(((bits >> 140) & 0x7FF) * .01 - 10.23, 2), 'stop': data[23] & 3}
  if address == 0x162:
    return {name: (bits >> bit) & 7 for name, bit in WARNINGS.items()}
  if address == 0x10B:
    return {'button_raw': data[10] & 15, 'lfa_button': (data[10] >> 7) & 1, 'counter': data[2]}
  return {}


def frame(row):
  kind, src, address = row.get('kind'), int(row['bus']), int(row['address'])
  data = bytes.fromhex(row['hex'])
  source, bus = channel(kind, src)
  integrity = ('unmapped' if address not in LENGTHS else 'bad_length' if len(data) != LENGTHS[address]
               else 'valid' if int.from_bytes(data[:2], 'little') == checksum(address, data) else 'bad_crc')
  # Host packer bytes may have a placeholder CRC. Decode their intent but do
  # not use them as qualified ECU evidence. Actual RX/echo requires valid CRC.
  readable = address in LENGTHS and len(data) == LENGTHS[address] and (integrity == 'valid' or source == 'host_request')
  return {'t': float(row['t']), 'source': source, 'bus': bus, 'address': address,
          'hex': data.hex(), 'integrity': integrity, 'signals': decode(address, data) if readable else {}}


def load_rows(path, schema=None):
  path = Path(path)
  if path.suffix.lower() == '.json':
    document = json.loads(path.read_text(encoding='utf-8-sig'))
    return document['rows'] if isinstance(document, dict) else document
  if path.suffix.lower() == '.jsonl':
    return [json.loads(line) for line in path.read_text(encoding='utf-8-sig').splitlines() if line.strip()]
  if schema is None:
    raise ValueError('rlog input needs --schema local/path/log.capnp')
  import capnp  # Optional, only used with local rlog.
  capnp.remove_import_hook()
  event_type = capnp.load(str(Path(schema).resolve())).Event
  payload = path.read_bytes()
  if path.suffix == '.zst':
    import zstandard
    payload = zstandard.ZstdDecompressor().stream_reader(payload).read()
  elif path.suffix == '.bz2':
    payload = bz2.decompress(payload)
  rows = []
  for event in event_type.read_multiple_bytes(payload, traversal_limit_in_words=2**29):
    kind, t = event.which(), event.logMonoTime / 1e9
    if kind in ('can', 'sendcan'):
      rows.extend({'t': t, 'kind': kind, 'bus': int(msg.src), 'address': int(msg.address), 'hex': bytes(msg.dat).hex()}
                  for msg in getattr(event, kind))
    elif kind in ('carState', 'carControl', 'selfdriveState', 'pandaStates', 'onroadEvents'):
      value = getattr(event, kind)
      rows.append({'t': t, 'kind': kind, 'data': value.to_dict() if hasattr(value, 'to_dict') else [m.to_dict() for m in value]})
  return rows


class Index:
  def __init__(self, rows):
    self.rows = sorted(rows, key=lambda r: r['t'])
    self.times = [row['t'] for row in self.rows]

  def between(self, start, end):
    return self.rows[bisect.bisect_left(self.times, start):bisect.bisect_right(self.times, end)]

  def before(self, t, max_age=.2, inclusive=True):
    i = (bisect.bisect_right(self.times, t) if inclusive else bisect.bisect_left(self.times, t)) - 1
    return self.rows[i] if i >= 0 and t - self.rows[i]['t'] <= max_age else None


def brief(row):
  return None if row is None else {k: row[k] for k in ('t', 'source', 'bus', 'address', 'signals') if k in row}


def command_matches(address, actual, requested):
  # Panda preserves CURRENT original SCC warnings even when the host template
  # predates them. Match requested control fields; retain warnings in the echo.
  keys = {0xCB: ('active', 'angle_deg', 'cap_raw'), 0x1A0: ('mode', 'accel_value', 'accel_raw', 'stop'), 0xEA: ('lfa_state',)}[address]
  return all(actual[k] == requested[k] for k in keys)


def command_phase(address, signals):
  return (signals['active'], signals['cap_raw'] == 0) if address == 0xCB else (signals['mode'], signals['stop'])


def analyze(rows, window=1., max_events=2000):
  if not math.isfinite(window) or window <= 0 or max_events <= 0:
    raise ValueError('window and max_events must be positive')
  groups, states, counts = defaultdict(list), defaultdict(list), Counter()
  rejected, integrity_bad, host_crc, excluded = Counter(), Counter(), Counter(), 0
  all_times = []
  for row in rows:
    t = float(row['t'])
    if not math.isfinite(t) or t < 0:
      raise ValueError('timestamps must be finite nonnegative monotonic seconds')
    all_times.append(t)
    if row.get('kind') not in ('can', 'sendcan'):
      states[row.get('kind')].append(row)
      continue
    f = frame(row)
    key = f"{f['source']}/bus{f['bus']}/0x{f['address']:X}"
    counts[key] += 1
    if f['source'] == 'tx_rejected':
      rejected[key] += 1
    if f['integrity'] in ('bad_length', 'bad_crc'):
      target = host_crc if f['source'] == 'host_request' and f['integrity'] == 'bad_crc' else integrity_bad
      target[f"{key}/{f['integrity']}"] += 1
    if not f['signals'] or f['source'] not in ('host_request', 'ecu_rx', 'wire_echo'):
      excluded += 1
      continue
    groups[(f['source'], f['bus'], f['address'])].append(f)
  indices = {key: Index(value) for key, value in groups.items()}
  state_indices = {key: Index(value) for key, value in states.items()}
  empty = Index([])

  def idx(source, bus, address):
    return indices.get((source, bus, address), empty)

  def context(t):
    result = {}
    for kind in ('carControl', 'carState', 'selfdriveState', 'pandaStates'):
      r = state_indices.get(kind, empty).before(t)
      if r is not None:
        result[kind] = {'age_s': round(t - r['t'], 6), 'data': r['data']}
    return result

  transitions = []
  for (source, bus, address), index in indices.items():
    if source != 'ecu_rx' or (bus, address) not in ((0, 0xEA), (2, 0x162), (2, 0x1A0), (0, 0x10B)):
      continue
    fields = {0xEA: ['lfa_state', 'lka_fault', 'lfa_fault'], 0x162: list(WARNINGS),
              0x1A0: ['mode', 'system_fault_raw', 'takeover_raw', 'driver_alert_raw'],
              0x10B: ['button_raw', 'lfa_button']}[address]
    prev = None
    for f in index.rows:
      changed = {name: {'before': None if prev is None else prev['signals'][name], 'after': f['signals'][name]}
                 for name in fields if prev is None or prev['signals'][name] != f['signals'][name]}
      if changed:
        transitions.append(dict(brief(f), changes=changed, first_observation=prev is None,
                                gap_from_previous_s=None if prev is None else round(f['t'] - prev['t'], 6)))
      prev = f
  # Preserve every state transition in JSON; the HTML may limit rendered rows.
  transitions.sort(key=lambda r: r['t'])

  events, selected, modes = [], 0, Counter()
  mdps = idx('ecu_rx', 0, 0xEA)
  for address in (0xCB, 0x1A0):
    requests = idx('host_request', 0, address).rows
    # Cut observation windows at the next active/neutral (or SCC mode/stop)
    # transition, even when that request is not selected for the output table.
    phase_ends = [math.inf] * len(requests)
    for i in range(len(requests) - 2, -1, -1):
      phase_ends[i] = (requests[i + 1]['t'] if command_phase(address, requests[i]['signals']) !=
                       command_phase(address, requests[i + 1]['signals']) else phase_ends[i + 1])
    previous, last_selected = None, -math.inf
    for request_index, command in enumerate(requests):
      s, t = command['signals'], command['t']
      p = previous['signals'] if previous else None
      state_change = p is None or any(s[k] != p[k] for k in (('active',) if address == 0xCB else ('mode', 'stop')))
      amplitude_change = p is None or (abs(s['cap_raw'] - p['cap_raw']) >= 5 or abs(s['angle_deg'] - p['angle_deg']) >= 2
                                      if address == 0xCB else abs(s['accel_value'] - p['accel_value']) >= .3)
      if not state_change and not (amplitude_change and t - last_selected >= .25):
        continue
      # Compare with last selected request so gradual changes accumulate.
      previous, last_selected = command, t
      selected += 1
      if len(events) >= max_events:
        continue
      echoes = idx('wire_echo', 0, address).between(t, t + .05)
      match = next((f for f in echoes if command_matches(address, f['signals'], s)), None)
      stock = idx('ecu_rx', 2, address).before(match['t'] if match else t, .05)
      end = min(t + window, phase_ends[request_index])
      cut_at_transition = phase_ends[request_index] <= t + window
      before = mdps.before(t, .1, inclusive=False)
      before_age = None if before is None else t - before['t']
      same_batch = mdps.between(t, t)
      after = [f for f in mdps.between(t, end) if f['t'] > t and (not cut_at_transition or f['t'] < end)]
      baseline_status = 'missing' if before is None else 'baseline_stale' if before_age > .020000001 else 'fresh'
      if before and any(f['signals'] != before['signals'] for f in same_batch):
        baseline_status = 'same_batch_unordered'
      observed, candidates = {}, {}
      if address == 0xCB and baseline_status == 'fresh':
        for field, delta in (('lfa_state', 1), ('angle_deg', 1.), ('driver_torque_raw', 50), ('lka_fault', 1), ('lfa_fault', 1)):
          changed = next((f for f in after if abs(f['signals'][field] - before['signals'][field]) >= delta), None)
          if changed is not None:
            change = {'t': changed['t'], 'observed_after_request_s': round(changed['t'] - t, 6),
                      'before': before['signals'][field], 'after': changed['signals'][field]}
            if field in ('angle_deg', 'driver_torque_raw'):
              observed[field] = change
            else:
              change['equals_requested_active_raw'] = changed['signals'][field] == s['active'] if field == 'lfa_state' else None
              candidates[field] = change
      times = [t] + [f['t'] for f in after] + [end]
      max_gap = max((b - a for a, b in zip(times, times[1:])), default=end - t)
      ctx = context(t)
      cc = ctx.get('carControl', {}).get('data', {})
      mode = 'combined' if cc.get('latActive') and cc.get('longActive') else 'lateral_only' if cc.get('latActive') else 'lateral_inactive'
      if not cc:
        mode = 'unknown'
      modes[mode] += 1
      events.append({'t': t, 'request': brief(command), 'request_crc': command['integrity'], 'mode': mode,
                     'wire_match': brief(match), 'wire_match_delay_s': None if match is None else round(match['t'] - t, 6),
                     'wire_origin': 'ambiguous_stock_and_host' if match and stock and command_matches(address, stock['signals'], s)
                     else 'host_semantic_match_not_proof_of_origin' if match else 'not_observed',
                     'mdps_before': brief(before), 'mdps_after_count': len(after), 'mdps_max_gap_s': round(max_gap, 6),
                     'mdps_before_age_s': None if before_age is None else round(before_age, 6), 'baseline_status': baseline_status,
                     'same_batch_unordered': [brief(f) for f in same_batch],
                     'mdps_window_dense': bool(after) and max_gap <= .1 and baseline_status == 'fresh',
                     'window_end': end, 'window_end_reason': 'next_command_phase' if cut_at_transition else 'duration',
                     'observed_changes_after_request': observed, 'state_change_candidates': candidates,
                     'nearby_ecu_changes': [r for r in transitions if t < r['t'] <= end and
                                            (not cut_at_transition or r['t'] < end) and not r['first_observation']],
                     'context': ctx})
  events.sort(key=lambda r: r['t'])
  # Every request participates in gap/error metrics, independently of event
  # downsampling. A large angle error is an observation, not a rejection cause.
  steering = idx('host_request', 0, 0xCB).rows
  gaps = [{'t': b['t'], 'gap_s': round(b['t'] - a['t'], 6)} for a, b in zip(steering, steering[1:]) if b['t'] - a['t'] > .03]
  errors, phases = [], []
  for request in steering:
    t, signals = request['t'], request['signals']
    measured = mdps.before(t, .05)
    if signals['active'] == 2 and measured:
      errors.append(abs(signals['angle_deg'] - measured['signals']['angle_deg']))
    cs = state_indices.get('carState', empty).before(t)
    enabled = None if cs is None else cs['data'].get('latEnabled')
    neutral = signals['active'] == 1 and signals['cap_raw'] == 0
    phase = ('active_request' if signals['active'] == 2 else 'invalid_or_other' if not neutral
             else 'accepted_session_neutral' if enabled is True else 'outside_session_neutral' if enabled is False else 'unknown_neutral')
    camera = idx('ecu_rx', 2, 0xCB).before(t, .05)
    mirrored = idx('wire_echo', 2, 0xEA).before(t, .05)
    pair = f"camera={camera['signals']['active'] if camera else None},physical_mdps={measured['signals']['lfa_state'] if measured else None},camera_mdps_echo={mirrored['signals']['lfa_state'] if mirrored else None}"
    if not phases or phases[-1]['phase'] != phase or t - phases[-1]['end'] > .1:
      phases.append({'start': t, 'end': t, 'phase': phase, 'request_count': 0, 'nearby_state_counts': {}})
    p = phases[-1]
    p['end'], p['request_count'] = t, p['request_count'] + 1
    p['nearby_state_counts'][pair] = p['nearby_state_counts'].get(pair, 0) + 1
  errors.sort()
  observation = {'request_count': len(steering), 'gaps_over_30ms_count': len(gaps), 'gaps_first_100': gaps[:100],
                 'max_gap_s': max((g['gap_s'] for g in gaps), default=None),
                 'active_angle_error_samples': len(errors), 'abs_angle_error_p95_deg': errors[int((len(errors) - 1) * .95)] if errors else None,
                 'abs_angle_error_max_deg': max(errors, default=None), 'phases': phases,
                 'interpretation': 'Nearest prior samples <=50ms; sampled states and timing correlation only, not verified ECU acknowledgment.'}
  matches = defaultdict(lambda: {'requests': 0, 'semantic_echo_observed': 0, 'semantic_echo_not_observed': 0})
  for bus, address in ((0, 0xCB), (0, 0x1A0), (2, 0xEA)):
    for request in idx('host_request', bus, address).rows:
      t = request['t']
      cs = state_indices.get('carState', empty).before(t)
      enabled = None if cs is None else cs['data'].get('latEnabled')
      session = 'accepted_session' if enabled is True else 'outside_session' if enabled is False else 'unknown_session'
      bucket = matches[f'bus{bus}/0x{address:X}/{session}']
      found = any(command_matches(address, f['signals'], request['signals']) for f in idx('wire_echo', bus, address).between(t, t + .05))
      bucket['requests'] += 1
      bucket['semantic_echo_observed' if found else 'semantic_echo_not_observed'] += 1
  return {'format_version': 2, 'scope': 'offline observations, no actuation or causal conclusion',
          'limitations': LIMITATIONS, 'time_start': min(all_times) if all_times else None,
          'time_end': max(all_times) if all_times else None, 'window_s': window,
          'counts': dict(sorted(counts.items())), 'rejected_by_address': dict(sorted(rejected.items())),
          'integrity_issues': dict(sorted(integrity_bad.items())), 'host_request_crc_differences': dict(sorted(host_crc.items())),
          'excluded_from_signal_analysis': excluded,
          'selected_command_events': selected, 'emitted_command_events': len(events), 'truncated': selected > len(events),
          'selection': 'All active/mode/stop transitions; accumulated >=2deg/5cap/0.3accel steps at >=250ms intervals.',
          'mode_counts_of_emitted_events': dict(modes), 'steering_observation': observation,
          'all_request_echo_observations': dict(matches),
          'ecu_transitions': transitions, 'command_events': events}


def render_html(report):
  esc = lambda value: html.escape(str(value), quote=True)
  warnings = [r for r in report['ecu_transitions'] if r['address'] in (0x162, 0xEA, 0x1A0)]
  table = ''.join(f"<tr><td>{r['t']:.6f}</td><td>0x{r['address']:X}</td><td>{esc(r['changes'])}</td>"
                  f"<td>{'첫 관측' if r['first_observation'] else '변화'}</td></tr>" for r in warnings[:3000])
  commands = ''.join(f"<tr><td>{e['t']:.6f}</td><td>{esc(e['mode'])}</td><td>{esc(e['request']['signals'])}</td>"
                     f"<td>{esc(e['wire_origin'])}</td><td>{esc(e['wire_match_delay_s'])}</td>"
                     f"<td>{esc(e['observed_changes_after_request'])}<br>{esc(e['state_change_candidates'])}</td>"
                     f"<td>{esc(e['baseline_status'])}<br>{'밀집' if e['mdps_window_dense'] else '공백/부족'}</td></tr>"
                     for e in report['command_events'])
  return '<!doctype html><html lang="ko"><meta charset="utf-8"><title>LX3 CAN 명령–응답 진단</title>' + '''
<style>body{font:15px system-ui;margin:32px;color:#172332;background:#f4f6fa}table{border-collapse:collapse;width:100%;background:white}
td,th{padding:8px;border:1px solid #ccd3df;text-align:left;vertical-align:top}pre{white-space:pre-wrap}li{margin:8px 0}
h1{font-size:26px}.scroll{overflow:auto;max-height:650px}button,input{font:inherit;padding:8px;margin:8px 0}</style>
<h1>LX3 CAN 명령–응답 진단</h1><p>저장된 정상 운행 기록의 시간상 연관을 확인하는 읽기 전용 PC 보고서입니다.</p>''' + \
    '<ul>' + ''.join('<li>' + esc(line) + '</li>' for line in report['limitations']) + '</ul>' + \
    f"<p>분석 범위: 부팅 후 {esc(report['time_start'])}~{esc(report['time_end'])}초 · 선택 요청 {report['emitted_command_events']}개 · " + \
    ('<strong>표 상한에 도달했습니다.</strong>' if report['truncated'] else '선택 요청 표 잘림 없음.') + '</p>' + \
    '<details><summary>입력 해시·검사 범위·주소별 집계·구간 상세</summary><pre>' + esc(json.dumps({k: v for k, v in report.items() if k not in
      ('command_events', 'ecu_transitions', 'counts', 'limitations')}, ensure_ascii=False, indent=2)) + '</pre></details>' + \
    '<h2>ECU 상태·경고 변화</h2><p>원시 값입니다. 전체 변화는 JSON에 보존됩니다.</p><div class="scroll"><table><tr>' + \
    '<th>부팅 후 초</th><th>주소</th><th>변화</th><th>관측</th></tr>' + table + '</table></div>' + \
    '<h2>호스트 요청과 이후 관측</h2><input id="filter" placeholder="예: lateral_only, active, combined" aria-label="명령 표 필터">' + \
    '<div class="scroll"><table id="commands"><thead><tr><th>초</th><th>모드</th><th>요청</th><th>에코 해석</th><th>에코 지연(초)</th>' + \
    '<th>이후 MDPS 변화</th><th>수신 밀도</th></tr></thead><tbody>' + commands + '</tbody></table></div>' + \
    '<script>document.getElementById("filter").addEventListener("input",e=>{for(const r of document.querySelectorAll("#commands tbody tr"))' + \
    '{r.hidden=!r.textContent.toLowerCase().includes(e.target.value.toLowerCase())}})</script></html>'


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('inputs', nargs='+', type=Path)
  parser.add_argument('--schema', type=Path)
  parser.add_argument('--out', type=Path, required=True, help='new output directory (never overwrite a previous report)')
  parser.add_argument('--window', type=float, default=1.)
  parser.add_argument('--max-events', type=int, default=2000)
  args = parser.parse_args()
  if args.out.exists():
    parser.error('output directory already exists; choose a new name')
  rows, provenance = [], []
  for path in args.inputs:
    if not path.is_file():
      parser.error(f'local input file missing: {path}')
    rows.extend(load_rows(path, args.schema))
    provenance.append({'path': str(path.resolve()), 'sha256': hashlib.sha256(path.read_bytes()).hexdigest()})
  result = analyze(rows, args.window, args.max_events)
  result['input_provenance'] = provenance
  result['program_sha256'] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
  args.out.mkdir(parents=True)
  (args.out / 'report.json').write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
  (args.out / 'report.html').write_text(render_html(result), encoding='utf-8')
  print(json.dumps({k: result[k] for k in ('time_start', 'time_end', 'emitted_command_events', 'truncated',
                                         'rejected_by_address', 'integrity_issues')}, ensure_ascii=False))


if __name__ == '__main__':
  main()
