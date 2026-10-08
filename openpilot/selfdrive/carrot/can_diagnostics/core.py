"""Hardware-independent guided capture. All phase labels come from the user."""
from __future__ import annotations

import csv
import gzip
import json
import math
from pathlib import Path
import shutil
import time
import uuid

from .analysis import DbcIndex, Window, rank_candidates, MAX_MESSAGE_KEYS
from .validation import DrivingValidator

TESTS = {
  'brake': {'title': '브레이크 페달', 'field': 'brakePressed',
            'baseline': 'P단과 주차 브레이크를 유지하고, 브레이크 페달에서 발을 떼 주세요.',
            'active': '브레이크 페달을 밟고 그대로 유지해 주세요.',
            'release': '브레이크 페달에서 발을 떼고 유지해 주세요.'},
  'left_blinker': {'title': '왼쪽 방향지시등', 'field': 'leftBlinker',
                   'baseline': '양쪽 방향지시등과 비상등을 꺼 주세요.',
                   'active': '왼쪽 방향지시등을 켜고 유지해 주세요.',
                   'release': '방향지시등을 꺼 주세요.'},
  'right_blinker': {'title': '오른쪽 방향지시등', 'field': 'rightBlinker',
                    'baseline': '양쪽 방향지시등과 비상등을 꺼 주세요.',
                    'active': '오른쪽 방향지시등을 켜고 유지해 주세요.',
                    'release': '방향지시등을 꺼 주세요.'},
}
MAX_BYTES = 512 * 1024 * 1024
MIN_FREE_BYTES = 512 * 1024 * 1024
MAX_STORE_BYTES = 1024 * 1024 * 1024
MAX_SECONDS = 180
DRIVE_MAX_SECONDS = 1800
MARKERS = {'braking_late': '감속 시작이 늦음', 'braking_strong': '제동이 강함',
           'braking_weak': '감속이 부족함', 'lead_missed': '앞차 인식 누락',
           'false_lead': '차량 오인식', 'cut_in': '끼어들기', 'good': '동작 양호'}
SETTLE_SECONDS = .75
WINDOW_SECONDS = 4.0
SCHEMA_VERSION = 1


def json_safe(value):
  """Unavailable numeric telemetry must not break a browser's JSON parser."""
  if isinstance(value, float) and not math.isfinite(value):
    return None
  if isinstance(value, dict):
    return {key: json_safe(item) for key, item in value.items()}
  if isinstance(value, (list, tuple)):
    return [json_safe(item) for item in value]
  return value


def safety_reason(telemetry, now):
  cs, sd = telemetry.get('carState', {}), telemetry.get('selfdriveState', {})
  for service in ('carState', 'selfdriveState'):
    age = now - telemetry.get(service + '_received', -1e9)
    if not telemetry.get(service + '_valid') or not 0 <= age <= 1.0:
      return '차량 상태 신호를 기다리고 있습니다. 신호가 없거나 오래되면 시작할 수 없습니다.'
  speed = cs.get('vEgo')
  if isinstance(speed, bool) or not isinstance(speed, (int, float)) or not math.isfinite(speed):
    return '차량 속도를 확인할 수 없습니다.'
  if abs(speed) > .05 or cs.get('standstill') is not True or cs.get('gearShifter') != 'park':
    return '차량을 완전히 멈추고 P단으로 바꿔 주세요.'
  if cs.get('canValid') is not True:
    return '차량 CAN 상태가 유효하지 않습니다.'
  if sd.get('enabled') is not False or sd.get('active') is not False:
    return '오픈파일럿 제어를 해제해 주세요.'
  return None


class Capture:
  def __init__(self, root, dbc=None, clock=time.monotonic, disk_usage=shutil.disk_usage):
    self.root = Path(root)
    self.dbc = dbc or DbcIndex()
    self.clock, self.disk_usage = clock, disk_usage
    self.telemetry = {}
    self.latest = {}
    self.last_can = -1e9
    self.last_client = clock()
    self.session = None
    self.report = None
    self.raw = self.events = None
    self.error = None
    self.validator = DrivingValidator(self.dbc)
    self.previous_auto = {}
    self.metadata = {'demo': False}
    self.counters = {'can_frames': 0, 'echo_frames_ignored': 0, 'invalid_frames': 0, 'stale_frames_ignored': 0}

  def preflight(self, mode='drive'):
    if mode != 'drive':
      reason = safety_reason(self.telemetry, self.clock())
      if reason:
        return reason
    if not 0 <= self.clock() - self.last_can < 1:
      return '수신 CAN 신호를 기다리고 있습니다.'
    return None

  def active(self):
    return self.session is not None and self.session['state'] in ('driving', 'awaiting_action', 'settling', 'recording')

  def start(self, test_id):
    if test_id != 'drive' and test_id not in TESTS:
      raise ValueError('지원하지 않는 진단 항목입니다.')
    if self.active():
      raise ValueError('이미 진단이 진행 중입니다.')
    if reason := self.preflight(test_id):
      raise ValueError(reason)
    self.root.mkdir(parents=True, exist_ok=True)
    if self.disk_usage(self.root).free < MIN_FREE_BYTES:
      raise ValueError('저장 공간이 부족합니다. 512MB 이상 필요합니다.')
    used = sum(p.stat().st_size for p in self.root.glob('*/*') if p.is_file())
    if used >= MAX_STORE_BYTES:
      raise ValueError('진단 보관 용량이 1GB에 도달했습니다. 기존 기록을 먼저 옮겨 주세요.')
    session_id = time.strftime('%Y%m%dT%H%M%SZ', time.gmtime()) + '-' + uuid.uuid4().hex[:10]
    folder = self.root / session_id
    folder.mkdir()
    self.raw = gzip.open(folder / 'raw_can.jsonl.gz', 'wt', encoding='utf-8', compresslevel=1)
    self.events = (folder / 'events.jsonl').open('w', encoding='utf-8')
    now = self.clock()
    self.session = {'id': session_id, 'test_id': test_id, 'state': 'driving' if test_id == 'drive' else 'awaiting_action', 'step': 0,
                    'started': now, 'bytes': 0, 'frame_count': 0, 'label_source': 'natural_driving' if test_id == 'drive' else 'user_confirmed',
                    'windows': [], 'last_sample': now, 'last_disk_check': now, 'initial_counters': dict(self.counters)}
    self.report = None
    self.error = None
    self.validator = DrivingValidator(self.dbc)
    self.previous_auto = {}
    self.event('start', schema_version=SCHEMA_VERSION, test_id=test_id,
               label_source=self.session['label_source'], dbc_sources=self.dbc.sources, metadata=self.metadata,
               bit_numbering='byte_index * 8 + LSB0; bus is raw frame src',
               limits={'max_bytes': MAX_BYTES, 'max_seconds': DRIVE_MAX_SECONDS if test_id == 'drive' else MAX_SECONDS})
    return self.status()

  def event(self, kind, **fields):
    if self.events:
      self.events.write(json.dumps(json_safe({'kind': kind, 'monotonic_ns': int(self.clock() * 1e9),
                                   'wall_time_ns': time.time_ns(), **fields}), ensure_ascii=False, allow_nan=False) + '\n')
      self.events.flush()

  def phase(self):
    if self.session and self.session['test_id'] == 'drive':
      return 0, 'drive'
    step = min(self.session['step'], 8) if self.session else 0
    return step // 3, ('baseline', 'active', 'release')[step % 3]

  def mark(self):
    if not self.active() or self.session['state'] != 'awaiting_action':
      raise ValueError('현재 조작을 확인할 단계가 아닙니다.')
    if reason := self.preflight(self.session['test_id']):
      self.finish(reason, completed=False)
      raise ValueError(reason)
    self.session['state'] = 'settling'
    self.session['deadline'] = self.clock() + SETTLE_SECONDS
    self.event('user_confirmed_action', cycle=self.phase()[0], phase=self.phase()[1],
               telemetry=self.telemetry.get('carState', {}))
    return self.status()

  def receive_frame(self, bus, address, data, log_mono_time):
    now = self.clock()
    if bus >= 128:
      self.counters['echo_frames_ignored'] += 1
      return
    if not (0 <= bus <= 7 and 0 <= address <= 0x1FFFFFFF and 0 < len(data) <= 64):
      self.counters['invalid_frames'] += 1
      return
    # A queued old CAN event is not evidence of current vehicle connectivity.
    age = now - log_mono_time / 1e9
    if not 0 <= age <= .5:
      self.counters['stale_frames_ignored'] += 1
      return
    key = (bus, address, len(data))
    if key not in self.latest and len(self.latest) >= MAX_MESSAGE_KEYS:
      if self.active():
        self.finish('CAN 주소 종류가 수집 한도를 초과했습니다.', completed=False)
      return
    self.latest[key] = (bytes(data), now)
    self.last_can = now
    self.counters['can_frames'] += 1
    if not self.active():
      return
    if reason := self.preflight(self.session['test_id']):
      self.finish(reason, completed=False)
      return
    cycle, phase = self.phase()
    # Only validated integers, hex bytes and internal enum strings are formatted.
    # Avoid constructing a dict and JSON encoder for every received CAN frame.
    line = (f'{{"mono_ns":{log_mono_time},"received_ns":{int(now * 1e9)},"bus":{bus},'
            f'"address":{address},"dlc":{len(data)},"data":"{data.hex()}","cycle":{cycle},'
            f'"phase":"{phase}","stage":"{self.session["state"]}"}}\n')
    if self.session['bytes'] + len(line) > MAX_BYTES:
      self.finish('수집 용량 한도에 도달했습니다.', completed=False)
      return
    self.raw.write(line)
    self.session['bytes'] += len(line)
    self.session['frame_count'] += 1
    self.validator.consume(bus, address, data, log_mono_time)

  def marker(self, code):
    if not self.active() or self.session['test_id'] != 'drive':
      raise ValueError('주행 기록 중에만 표식을 남길 수 있습니다.')
    if code not in MARKERS:
      raise ValueError('알 수 없는 표식입니다.')
    self.event('user_marker', code=code, title=MARKERS[code])
    return self.status()

  def record_context(self):
    now = self.clock()
    services = ('carState', 'selfdriveState', 'radarState', 'longitudinalPlan', 'carControl', 'modelV2')
    context = {}
    freshness = {}
    for name in services:
      received = self.telemetry.get(name + '_received', -1e9)
      age = now - received
      freshness[name] = {'available': name in self.telemetry, 'valid': self.telemetry.get(name + '_valid', False),
                         'age_ms': round(age * 1000) if name in self.telemetry else None,
                         'logMonoTime': self.telemetry.get(name + '_received_ns')}
      context[name] = self.telemetry.get(name) if 0 <= age <= 1 else None
    self.event('context', cycle=self.phase()[0], phase=self.phase()[1], services=context, service_status=freshness)
    cs, radar = context.get('carState') or {}, context.get('radarState') or {}
    if not freshness['carState']['valid']:
      cs = {}
    if not freshness['radarState']['valid']:
      radar = {}
    automatic = {k: cs.get(k) for k in ('brakePressed', 'leftBlinker', 'rightBlinker', 'leftBlindspot', 'rightBlindspot')}
    automatic['decelerating'] = cs['aEgo'] < -.8 if isinstance(cs.get('aEgo'), (int, float)) else None
    automatic['lead_present'] = (radar.get('leadOne') or {}).get('status')
    for key, value in automatic.items():
      if value is not None and key in self.previous_auto and value != self.previous_auto[key]:
        self.event('automatic_transition', signal=key, value=value,
                   source='decoded_service', source_logMonoTime=freshness['radarState' if key == 'lead_present' else 'carState']['logMonoTime'])
      if value is not None:
        self.previous_auto[key] = value

  def tick(self):
    if not self.active():
      return
    now = self.clock()
    driving = self.session['test_id'] == 'drive'
    reason = self.preflight('drive' if driving else self.session['test_id'])
    if driving and now - self.last_can < 5:
      reason = None
    if not driving and now - self.last_client > 15:
      reason = '앱 연결이 끊겨 진단을 중단했습니다.'
    if now - self.session['started'] > (DRIVE_MAX_SECONDS if driving else MAX_SECONDS):
      reason = '진단 시간 한도에 도달했습니다.'
    if now - self.session['last_disk_check'] > 2:
      self.session['last_disk_check'] = now
      if self.disk_usage(self.root).free < MIN_FREE_BYTES:
        reason = '저장 공간이 부족해 진단을 중단했습니다.'
    if reason:
      self.finish(reason, completed=False)
      return
    s = self.session
    if driving:
      if now - s['last_sample'] >= .1:
        s['last_sample'] = now
        self.record_context()
      return
    if s['state'] == 'settling' and now >= s['deadline']:
      s['state'] = 'recording'
      s['deadline'] = now + WINDOW_SECONDS
      s['windows'].append(Window(*self.phase()))
      self.event('window_start', cycle=self.phase()[0], phase=self.phase()[1])
    if s['state'] == 'recording' and now - s['last_sample'] >= .1:
      s['last_sample'] = now
      fresh = {k: payload for k, (payload, at) in self.latest.items() if now - at < .25}
      s['windows'][-1].add(fresh)
      self.record_context()
    if s['state'] == 'recording' and now >= s['deadline']:
      self.event('window_end', cycle=self.phase()[0], phase=self.phase()[1])
      s['step'] += 1
      if s['step'] == 9:
        self.finish('3회 반복 측정을 마쳤습니다.', completed=True)
      else:
        s['state'] = 'awaiting_action'
        self.raw.flush()

  def finish(self, reason='사용자가 중단했습니다.', completed=False):
    if not self.active():
      return
    s = self.session
    s['state'] = 'completed' if completed else 'stopped'
    s['ended'] = self.clock()
    s['reason'] = reason
    self.event('finish', completed=completed, reason=reason)
    for handle in (self.raw, self.events):
      if handle:
        handle.close()
    self.raw = self.events = None
    candidates = rank_candidates(s['windows'], self.dbc) if completed else []
    self.report = {'schema_version': SCHEMA_VERSION, 'session_id': s['id'], 'test_id': s['test_id'],
                   'completed': completed, 'reason': reason, 'label_source': s['label_source'], 'metadata': self.metadata,
                   'frame_count': s['frame_count'], 'raw_bytes': s['bytes'],
                   'capture_counters': {k: v-s['initial_counters'].get(k, 0) for k, v in self.counters.items()},
                   'dbc_sources': self.dbc.sources, 'candidates': candidates,
                   'drive_validation': self.validator.summary(),
                   'start_monotonic_ns': int(s['started'] * 1e9), 'end_monotonic_ns': int(self.clock() * 1e9),
                   'limitations': ['상관관계 후보이며 CAN 매핑이 확정된 것이 아닙니다.',
                                   'DBC 이름과 런타임 버스 범위는 참고 근거이며 개별 신호의 실제 의미는 별도 검증해야 합니다.',
                                   '수신 CAN만 기록합니다. 전송·차량 제어·설정 변경은 수행하지 않습니다.'],
                   'windows': [{'cycle': w.cycle, 'phase': w.phase,
                                'message_samples': [{'bus': k[0], 'address': k[1], 'dlc': k[2], 'count': row[0]}
                                                    for k, row in sorted(w.samples.items())]}
                               for w in s['windows']]}
    folder = self.root / s['id']
    temporary = folder / 'report.tmp'
    temporary.write_text(json.dumps(self.report, ensure_ascii=False, indent=2), encoding='utf-8')
    temporary.replace(folder / 'report.json')
    (folder / 'catalog.json').write_text(json.dumps(self.validator.catalog(), ensure_ascii=False, indent=2), encoding='utf-8')
    with (folder / 'candidates.csv').open('w', newline='', encoding='utf-8-sig') as stream:
      fields = ['bus', 'address_hex', 'dlc', 'bit_lsb0', 'byte_index', 'bit_in_byte', 'score',
                'repeat_count', 'polarity', 'mapping_status']
      writer = csv.DictWriter(stream, fieldnames=fields, extrasaction='ignore')
      writer.writeheader()
      writer.writerows(candidates)

  def status(self):
    now = self.clock()
    result = {'schema_version': SCHEMA_VERSION, 'ready': self.preflight() is None,
              'block_reason': self.preflight(), 'error': self.error, 'counters': dict(self.counters),
              'car': {k: self.telemetry.get('carState', {}).get(k) for k in
                      ('vEgo', 'gearShifter', 'canValid', 'brakePressed', 'leftBlinker', 'rightBlinker')},
              'car_state_fresh': self.telemetry.get('carState_valid', False) and 0 <= now - self.telemetry.get('carState_received', -1e9) <= 1,
              'tests': [{'id': k, 'title': v['title']} for k, v in TESTS.items()],
              'guided_block_reason': self.preflight('guided'),
              'drive_validation': self.validator.summary(), 'markers': MARKERS,
              'session': None, 'report': self.report}
    if self.session:
      s = self.session
      if s['test_id'] == 'drive':
        result['session'] = {k: s[k] for k in ('id', 'test_id', 'state', 'step', 'frame_count', 'bytes')}
        result['session'].update({'elapsed_seconds': round(s.get('ended', now) - s['started'], 1),
                                  'max_seconds': DRIVE_MAX_SECONDS, 'reason': s.get('reason')})
        return result
      cycle, phase = self.phase()
      field = TESTS[s['test_id']]['field']
      observed = self.telemetry.get('carState', {}).get(field)
      result['session'] = {k: s[k] for k in ('id', 'test_id', 'state', 'step', 'frame_count', 'bytes')}
      result['session'].update({'cycle': cycle + 1, 'phase': phase, 'total_steps': 9,
                                'prompt': TESTS[s['test_id']][phase],
                                'remaining_seconds': max(0, round(s.get('deadline', now) - now, 1)),
                                'elapsed_seconds': round(s.get('ended', now) - s['started'], 1),
                                'decoded_feedback': observed, 'expected_feedback': phase == 'active',
                                'reason': s.get('reason')})
    return result
