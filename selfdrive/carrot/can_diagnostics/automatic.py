"""Bounded, passive trip evidence. No CAN publishers or control parameter writes."""
from __future__ import annotations

from collections import deque
import hashlib
import gzip
import json
import math
import os
from pathlib import Path
import shutil
import time
import uuid


def clean(value):
  if isinstance(value, float) and not math.isfinite(value):
    return None
  if isinstance(value, dict):
    return {k: clean(v) for k, v in value.items()}
  if isinstance(value, (list, tuple)):
    return [clean(v) for v in value]
  return value


def atomic_json(path, data):
  tmp = path.with_suffix('.tmp')
  with tmp.open('w', encoding='utf-8') as stream:
    json.dump(clean(data), stream, ensure_ascii=False, allow_nan=False)
    stream.flush()
    os.fsync(stream.fileno())
  os.replace(tmp, path)


class ChunkStore:
  def __init__(self, root, *, quota=512 * 1024**2, reserve=512 * 1024**2, seconds=60, chunk_bytes=2 * 1024**2):
    self.root = Path(root)
    self.quota, self.reserve = quota, reserve
    self.seconds, self.chunk_bytes = seconds, chunk_bytes
    self.stream = None
    self.error = None
    self.root.mkdir(parents=True, exist_ok=True)
    self.recover()
    self.usage = sum(p.stat().st_size for p in self.root.iterdir() if p.is_file())

  def recover(self):
    # Keep complete JSON lines after abrupt power loss; never claim uninterrupted coverage.
    for path in self.root.glob('*.partial'):
      end = 0
      with path.open('rb') as stream:
        for line in stream:
          try:
            if not line.endswith(b'\n'):
              break
            json.loads(line)
            end = stream.tell()
          except (ValueError, UnicodeError):
            break
      with path.open('r+b') as stream:
        stream.truncate(end)
      target = path.with_suffix('.jsonl')
      os.replace(path, target)
      self.compress(target, 'power_loss_recovered')
    for path in self.root.glob('*.jsonl'):
      self.compress(path, 'interrupted_seal_recovered')
    for path in self.root.glob('*.jsonl.gz'):
      if not self.manifest_path(path).exists():
        self.manifest(path, 'interrupted_seal_recovered')

  @staticmethod
  def manifest_path(path):
    return path.parent / (path.name.split('.')[0] + '.manifest.json')

  def compress(self, path, reason):
    target = path.with_suffix('.jsonl.gz')
    temporary = path.with_suffix('.compressing')
    with temporary.open('wb') as stream:
      stream.write(gzip.compress(path.read_bytes(), compresslevel=1, mtime=0))
      stream.flush()
      os.fsync(stream.fileno())
    os.replace(temporary, target)
    self.manifest(target, reason)
    path.unlink()
    return target

  def manifest(self, path, reason):
    data = path.read_bytes()  # Each chunk is bounded to 2 MiB plus one record.
    result = {'id': path.name.split('.')[0], 'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest(),
              'reason': reason, 'schema': 1}
    atomic_json(self.manifest_path(path), result)
    return result

  def list_chunks(self):
    return [json.loads(p.read_text(encoding='utf-8')) for p in sorted(self.root.glob('*.manifest.json'))]

  def begin(self, metadata, now):
    if self.stream:
      return True
    if self.usage + self.chunk_bytes + 65536 > self.quota or shutil.disk_usage(self.root).free < self.reserve + self.chunk_bytes:
      self.error = 'storage_full_preserving_records'
      return False
    self.error = None
    self.path = self.root / (uuid.uuid4().hex + '.partial')
    self.stream = self.path.open('xb')
    self.started, self.size, self.last_flush = now, 0, now
    self.append({'kind': 'header', 'schema': 1, **metadata}, now)
    return True

  def append(self, row, now):
    if not self.stream:
      return
    data = (json.dumps(clean(row), separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()
    self.stream.write(data)
    self.size += len(data)
    self.usage += len(data)
    if now - self.last_flush >= 1:
      self.stream.flush()
      os.fsync(self.stream.fileno())
      self.last_flush = now

  def full(self, now):
    return self.stream is not None and (now - self.started >= self.seconds or self.size >= self.chunk_bytes)

  def seal(self, reason):
    if not self.stream:
      return
    self.stream.flush()
    os.fsync(self.stream.fileno())
    self.stream.close()
    self.stream = None
    target = self.path.with_suffix('.jsonl')
    os.replace(self.path, target)
    target = self.compress(target, reason)
    self.usage += target.stat().st_size + self.manifest_path(target).stat().st_size - self.size


def fresh(services, key, now, age=1):
  service = services.get(key, {})
  return service.get('valid') is True and 0 <= now - service.get('mono_ns', 0) / 1e9 <= age


def control_mode(services, now):
  if not all(fresh(services, k, now) for k in ('carState', 'carControl', 'selfdriveState')):
    return 'unknown'
  cs, cc, sd = (services[k]['data'] for k in ('carState', 'carControl', 'selfdriveState'))
  if not cs.get('canValid') or not all(type(cc.get(k)) is bool for k in ('enabled', 'latActive', 'longActive')) or not all(type(sd.get(k)) is bool for k in ('enabled', 'active')):
    return 'unknown'
  if cc.get('latActive') and cc.get('longActive'):
    return 'openpilot_both'
  if cc.get('latActive'):
    return 'openpilot_lateral'
  if cc.get('longActive'):
    return 'openpilot_longitudinal'
  if cc.get('enabled') or sd.get('enabled') or sd.get('active'):
    return 'openpilot_standby'
  return 'stock_cruise' if cs.get('cruiseState', {}).get('enabled') else 'driver'


class AutoRecorder:
  # Keep 10 Hz samples throughout the drive and an unthrottled, bounded
  # selection around physical LX3 button presses. The ordinary rlog remains
  # the independent full CAN source when its route segments are retained.
  ADDRESSES = {0x0A0, 0x0CB, 0x0EA, 0x105, 0x10B, 0x12A, 0x161, 0x162, 0x175,
               0x1A0, 0x1AA, 0x1CF, 0x1EA, 0x2A4, 0x2AF, 0x362}
  LX3_ONLY_ADDRESSES = {0x0A0, 0x105, 0x175, 0x1AA, 0x1CF, 0x2AF}
  TRACE_ADDRESSES = {0x0CB, 0x0EA, 0x10B, 0x12A, 0x161, 0x162, 0x1A0,
                     0x1EA, 0x2A4, 0x2AF, 0x362}
  # Raw samples are kept at 10 Hz outside a button trace. The full-rate trace
  # is bounded in time, memory, and frames, and never transmits a CAN packet.
  BUTTON_TRACE_PRE_NS = 2_000_000_000
  BUTTON_TRACE_POST_NS = 8_000_000_000
  BUTTON_TRACE_MAX_NS = 30_000_000_000
  BUTTON_TRACE_MAX_FRAMES = 20_000
  BUTTON_TRACE_MAX_PER_TRIP = 24

  def __init__(self, store, metadata):
    self.store, self.metadata = store, metadata
    self.trip = None
    self.last_sample = -math.inf
    self.previous = {}
    self.lead_changes = deque(maxlen=20)
    self.last_event = {}
    self.can_last, self.faults = {}, {}
    self.last_panda_sample = -math.inf
    self.sampled_out = self.stale_packets = 0
    self.state = 'waiting_for_ignition'
    self.trace_pre = deque(maxlen=3000)
    self.trace = None
    self.trace_count = 0
    self.trace_pre_evicted = 0
    self.last_button = None
    self.last_button_edge = {}
    self.last_host_state = None
    self.last_panda_state = None

  def trace_active(self, now):
    return self.trace is not None and int(now * 1e9) <= self.trace['until_ns']

  @staticmethod
  def can_row(mono_ns, direction, bus, address, data):
    return {'mono_ns': mono_ns, 'direction': direction, 'bus': bus,
            'address': address, 'dlc': len(data), 'data': data.hex()}

  def close_trace(self, now, reason):
    if self.trace is not None:
      self.store.append({'kind': 'event', 'name': 'button_trace_end', 'mono_ns': int(now * 1e9),
                         'trace_id': self.trace['id'], 'reason': reason,
                         'frames': self.trace['frames'], 'truncated': self.trace['truncated'],
                         'dropped_full': self.trace['dropped_full'],
                         'stale_packets_delta': self.stale_packets - self.trace['stale_start']}, now)
      self.trace = None

  def button_edges(self, bus, address, data, mono_ns, now, direction):
    if (self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV' or
        direction != 'rx' or bus != 0 or address != 0x10B or len(data) != 16):
      return
    # LX3 DBC CRUISE_BUTTONS_ALT2: CRUISE_BUTTONS 83|4@0, LFA_BTN 87|1@0.
    # This is the physical input, never sendcan or a Panda TX echo.
    raw = data[10] & 0x0F
    lfa = bool(data[10] & 0x80)
    previous = self.last_button
    self.last_button = (raw, lfa)
    if previous is None:
      return
    candidates = []
    if lfa and not previous[1]:
      candidates.append('lfa')
    if raw != previous[0] and raw in (1, 2, 3, 4, 8):
      candidates.append({1: 'res_accel', 2: 'set_decel', 3: 'gap',
                         4: 'cancel', 8: 'cruise_main'}[raw])
    for button in candidates:
      # The main value can flicker 8/0 within one held press. Keep every raw
      # frame in the trace while grouping its repeated edges for 300 ms.
      if mono_ns - self.last_button_edge.get(button, -10**18) < 300_000_000:
        continue
      self.last_button_edge[button] = mono_ns
      if self.trace is not None and mono_ns > self.trace['until_ns']:
        self.close_trace(now, 'window_elapsed')
      if self.trace is not None and mono_ns > self.trace['start_ns'] + self.BUTTON_TRACE_MAX_NS:
        self.close_trace(now, 'maximum_window')
      if self.trace is None and self.trace_count < self.BUTTON_TRACE_MAX_PER_TRIP:
        self.trace_count += 1
        self.trace = {'id': uuid.uuid4().hex, 'start_ns': mono_ns,
                      'until_ns': mono_ns + self.BUTTON_TRACE_POST_NS,
                      'frames': 0, 'truncated': False, 'dropped_full': 0,
                      'stale_start': self.stale_packets}
        self.store.append({'kind': 'event', 'name': 'button_trace_start', 'mono_ns': mono_ns,
                           'trace_id': self.trace['id'], 'button': button,
                           'pre_ns': self.BUTTON_TRACE_PRE_NS,
                           'post_ns': self.BUTTON_TRACE_POST_NS}, now)
        for pre_ns, pre_direction, pre_bus, pre_address, pre_data in self.trace_pre:
          if mono_ns - self.BUTTON_TRACE_PRE_NS <= pre_ns < mono_ns:
            self.append_trace_frame(self.can_row(pre_ns, pre_direction, pre_bus, pre_address, pre_data), now)
      if self.trace is not None:
        self.trace['until_ns'] = min(self.trace['start_ns'] + self.BUTTON_TRACE_MAX_NS,
                                     mono_ns + self.BUTTON_TRACE_POST_NS)
      self.store.append({'kind': 'event', 'name': 'physical_button_edge',
                         'mono_ns': mono_ns, 'button': button, 'raw_cruise': raw,
                         'lfa_btn': lfa, 'trace_id': self.trace['id'] if self.trace else None,
                         'source': 'rx_bus0_0x10B'}, now)

  def append_trace_frame(self, row, now):
    if self.trace is None or self.trace['truncated']:
      return
    if self.store.full(now):
      self.trace['dropped_full'] += 1
      return
    if self.trace['frames'] >= self.BUTTON_TRACE_MAX_FRAMES:
      self.trace['truncated'] = True
      self.store.append({'kind': 'event', 'name': 'button_trace_truncated',
                         'mono_ns': int(now * 1e9), 'trace_id': self.trace['id'],
                         'reason': 'frame_limit'}, now)
      return
    self.store.append({'kind': 'button_trace_can', 'trace_id': self.trace['id'], **row}, now)
    self.trace['frames'] += 1

  def event(self, name, now, **details):
    if now - self.last_event.get(name, -math.inf) < 1:
      return
    self.last_event[name] = now
    self.store.append({'kind': 'event', 'name': name, 'mono_ns': int(now * 1e9), **details}, now)

  def update(self, services, now):
    ignition_fresh = fresh(services, 'deviceState', now, 3)
    started = ignition_fresh and services['deviceState']['data'].get('started') is True
    if not started:
      reason = 'ignition_off' if ignition_fresh else 'ignition_unknown'
      self.close_trace(now, reason)
      self.store.seal(reason)
      self.trip = None
      self.state = reason
      return
    if self.trip is None:
      self.trip = uuid.uuid4().hex
      self.previous, self.can_last, self.faults, self.last_event = {}, {}, {}, {}
      self.last_panda_sample = -math.inf
      self.lead_changes.clear()
      self.last_sample = -math.inf
      self.trace_pre.clear()
      self.trace = None
      self.trace_count = self.trace_pre_evicted = 0
      self.last_button = self.last_host_state = self.last_panda_state = None
      self.last_button_edge.clear()
    if self.trace is not None and not self.trace_active(now):
      self.close_trace(now, 'window_elapsed')
    if self.store.full(now):
      self.store.seal('rotation')
    if not self.store.begin({**self.metadata, 'trip_id': self.trip, 'mono_ns': int(now * 1e9),
                             'utc_ns': time.time_ns(), 'capture': 'sampled_evidence',
                             'full_can_source': 'rlog_not_copied_by_this_recorder'}, now):
      self.state = self.store.error
      return
    self.state = 'recording'
    if now - self.last_sample < (.05 if self.trace_active(now) else .2):
      return
    mode = control_mode(services, now)
    sample = {'kind': 'sample', 'mono_ns': int(now * 1e9), 'mode': mode, 'services': services,
              'trace_id': self.trace['id'] if self.trace_active(now) else None,
              'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
              'route': self.metadata.get('route')}
    self.store.append(sample, now)
    cs = services.get('carState', {}).get('data', {})
    cc = services.get('carControl', {}).get('data', {})
    sd = services.get('selfdriveState', {}).get('data', {})
    if (self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV' and
        all(fresh(services, key, now) for key in ('carState', 'carControl', 'selfdriveState'))):
      host = {'cruise_available': cs.get('cruiseState', {}).get('available'),
              'cruise_enabled': cs.get('cruiseState', {}).get('enabled'),
              'lat_enabled': cs.get('latEnabled'), 'steer_fault_temporary': cs.get('steerFaultTemporary'),
              'steer_fault_permanent': cs.get('steerFaultPermanent'),
              'can_valid': cs.get('canValid'), 'control_enabled': cc.get('enabled'),
              'lat_active': cc.get('latActive'), 'long_active': cc.get('longActive'),
              'alert_type': sd.get('alertType')}
      if self.last_host_state is not None:
        sources = {'cruise_available': 'carState', 'cruise_enabled': 'carState',
                   'lat_enabled': 'carState', 'steer_fault_temporary': 'carState',
                   'steer_fault_permanent': 'carState', 'can_valid': 'carState',
                   'control_enabled': 'carControl', 'lat_active': 'carControl',
                   'long_active': 'carControl', 'alert_type': 'selfdriveState'}
        for field, value in host.items():
          before = self.last_host_state.get(field)
          if value != before:
            source = sources[field]
            self.store.append({'kind': 'event', 'name': 'host_state_edge',
                               'mono_ns': services[source]['mono_ns'], 'observed_mono_ns': int(now * 1e9),
                               'source': source, 'field': field, 'before': before, 'after': value,
                               'trace_id': self.trace['id'] if self.trace_active(now) else None}, now)
      self.last_host_state = host
    radar = services.get('radarState', {}).get('data', {})
    current = {'mode': mode}
    if mode != 'unknown' and isinstance(cs.get('vEgo'), (int, float)) and cs['vEgo'] > 2:
      current.update(brake=bool(cs.get('brakePressed')), override=bool(cs.get('steeringPressed') and cc.get('latActive')),
                     hard_deceleration=cs.get('aEgo', 0) is not None and cs.get('aEgo', 0) < -2.5)
    if fresh(services, 'radarState', now):
      current['lead'] = bool(radar.get('leadOne', {}).get('status'))
    if self.previous.get('mode') != mode:
      self.event('control_mode', now, before=self.previous.get('mode'), after=mode)
    for field in ('brake', 'override', 'hard_deceleration'):
      if current.get(field) and not self.previous.get(field):
        self.event(field, now, mode=mode, classification='review_candidate_not_preference')
    if 'lead' in current and 'lead' in self.previous and current['lead'] != self.previous['lead']:
      self.lead_changes.append(now)
      if sum(now - t <= 10 for t in self.lead_changes) >= 4:
        self.event('lead_flicker_candidate', now, classification='target_change_or_dropout_unconfirmed')
    self.previous, self.last_sample = current, now

  def can_frame(self, bus, address, data, mono_ns, now, direction='rx'):
    if (self.state != 'recording' or address not in self.ADDRESSES or not 0 <= bus < 256 or len(data) > 64 or
        (address in self.LX3_ONLY_ADDRESSES and self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV')):
      return
    if self.store.full(now):
      # Rotate on the next context update; a burst must not bypass the chunk bound.
      self.sampled_out += 1
      return
    if not 0 <= now - mono_ns / 1e9 <= .5:
      self.stale_packets += 1
      return
    if direction == 'rx':
      if bus >= 192:
        direction = 'tx_rejected'
      elif bus >= 128:
        direction = 'tx_echo'
    self.button_edges(bus, address, data, mono_ns, now, direction)
    if address in self.TRACE_ADDRESSES:
      if self.trace_active(now):
        self.append_trace_frame(self.can_row(mono_ns, direction, bus, address, data), now)
      if len(self.trace_pre) == self.trace_pre.maxlen:
        self.trace_pre_evicted += 1
      self.trace_pre.append((mono_ns, direction, bus, address, data))
      while self.trace_pre and mono_ns - self.trace_pre[0][0] > self.BUTTON_TRACE_PRE_NS:
        self.trace_pre.popleft()
    key = (direction, bus, address, len(data))
    if key not in self.can_last and len(self.can_last) >= 128:
      self.sampled_out += 1
      return
    fault_changed = False
    if address == 0x162 and len(data) == 32 and self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      bits = int.from_bytes(data, 'little')
      value = ((bits >> 219) & 7, (bits >> 246) & 7)
      before = self.faults.get(key)
      fault_changed = value != before
      if fault_changed:
        self.store.append({'kind': 'event', 'name': 'oem_fault_observation', 'mono_ns': mono_ns,
                           'direction': direction, 'bus': bus, 'before': before, 'after': value,
                           'trace_id': self.trace['id'] if self.trace_active(now) else None,
                           'semantic_status': 'dbc_definition_not_causal_diagnosis'}, now)
      self.faults[key] = value
    if now - self.can_last.get(key, -math.inf) < .1 and not fault_changed:
      self.sampled_out += 1
      return
    self.can_last[key] = now
    self.store.append({'kind': 'can_sample', **self.can_row(mono_ns, direction, bus, address, data)}, now)

  def panda_snapshot(self, states, mono_ns, now):
    if self.state != 'recording' or not states or now - self.last_panda_sample < (.1 if self.trace_active(now) else .5):
      return
    if self.store.full(now):
      self.sampled_out += 1
      return
    if not 0 <= now - mono_ns / 1e9 <= .5:
      self.stale_packets += 1
      return
    self.last_panda_sample = now
    self.store.append({'kind': 'panda_snapshot', 'mono_ns': mono_ns, 'states': states[:4],
                       'trace_id': self.trace['id'] if self.trace_active(now) else None}, now)
    current = states[0]
    if self.last_panda_state is not None:
      for field in ('controls_allowed', 'rx_checks_invalid', 'rx_overflow', 'tx_overflow',
                    'tx_blocked', 'safety_model', 'safety_param'):
        before, after = self.last_panda_state.get(field), current.get(field)
        if before != after:
          self.store.append({'kind': 'event', 'name': 'panda_state_edge', 'mono_ns': mono_ns,
                             'field': field, 'before': before, 'after': after,
                             'trace_id': self.trace['id'] if self.trace_active(now) else None}, now)
    self.last_panda_state = current.copy()

  def close(self):
    self.close_trace(time.monotonic(), 'server_shutdown')
    self.store.seal('server_shutdown')
    self.state = 'stopped'

  def status(self):
    return {'state': self.state, 'trip_id': self.trip, 'bytes': self.store.usage, 'quota': self.store.quota,
            'error': self.store.error, 'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
            'button_traces': self.trace_count, 'button_trace_active': self.trace is not None,
            'button_trace_pre_evicted': self.trace_pre_evicted,
            'control_changes': False, 'phone_backup': False}
