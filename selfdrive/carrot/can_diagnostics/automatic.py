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


def host_disabled_panda_allowed(services, now):
  """Observation only: pending ACK and orphan grants need later time correlation."""
  if not all(fresh(services, key, now, .25) for key in ('selfdriveState', 'pandaStates')):
    return None
  enabled = services['selfdriveState']['data'].get('enabled')
  # Same ignored models as the control path; a passive Panda is not a grant.
  pandas = [p for p in services['pandaStates']['data'] if p.get('safetyModel') not in ('silent', 'noOutput')]
  permissions = []
  for panda in pandas:
    guarded = panda.get('safetyModel') == 'hyundaiCanfd' and type(panda.get('safetyParam')) is int and bool(panda['safetyParam'] & 1024)
    version = panda.get('lx3PermissionVersion')
    if version == 1:
      permissions.append(panda.get('lx3ControlsAllowed'))
    elif guarded or version not in (None, 0):
      # Protocol0 on this policy includes a failed/short companion read. The
      # separately sampled universal health bit cannot fill that missing data.
      permissions.append(None)
    else:
      permissions.append(panda.get('controlsAllowed'))
  if type(enabled) is not bool or not pandas or not all(type(value) is bool for value in permissions):
    return None
  return not enabled and any(permissions)


class AutoRecorder:
  ADDRESSES = {0x161, 0x162, 0x1EA, 0x2A4, 0x362, 0x1A0, 0x41B, 0x417, 0x367,
               0x10B, 0xCB, 0xEA, 0x12A, 0x1AA}
  PERMISSION_FIELDS = {
    'pandaStates': ('safetyModel', 'safetyParam', 'lx3PermissionVersion', 'lx3RequestedMode',
                    'lx3AcceptedMode', 'lx3PhysicalCounter', 'lx3RequestGeneration',
                    'lx3ControlsAllowed', 'lx3PermissionPhase'),
    'selfdriveState': ('enabled', 'active', 'state', 'lx3EngagementMode', 'lx3AckMode',
                       'lx3AckGeneration', 'lx3AckPhysicalCounter', 'lx3AckValid'),
  }

  def __init__(self, store, metadata):
    self.store, self.metadata = store, metadata
    self.trip = None
    self.last_sample = -math.inf
    self.previous = {}
    self.lead_changes = deque(maxlen=20)
    self.last_event = {}
    self.can_last, self.faults = {}, {}
    self.actuation_states = {}
    self.permission_states, self.permission_times = {}, {}
    self.permission_sequence = 0
    self.sampled_out = self.stale_packets = 0
    self.state = 'waiting_for_ignition'

  def event(self, name, now, **details):
    if now - self.last_event.get(name, -math.inf) < 1:
      return
    self.last_event[name] = now
    self.store.append({'kind': 'event', 'name': name, 'mono_ns': int(now * 1e9), **details}, now)

  def permission_observations(self, services, now):
    """Keep received phase/ACK changes without the generic one-second throttle.

    SubMaster conflates publications. A small publication gap does not prove
    every firmware transition was observed; these rows never assert that.
    """
    if self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV':
      return
    for name, fields in self.PERMISSION_FIELDS.items():
      service = services.get(name, {})
      timestamp = service.get('mono_ns', 0)
      available = fresh(services, name, now, .25)
      data = service.get('data', [] if name == 'pandaStates' else {})
      value = ([{key: panda.get(key) for key in fields} for panda in data] if name == 'pandaStates'
               else {key: data.get(key) for key in fields})
      current = {'fresh_valid': available, 'data': value}
      previous_time = self.permission_times.get(name)
      gap = timestamp - previous_time if previous_time is not None and timestamp > previous_time else None
      changed = current != self.permission_states.get(name)
      if timestamp > self.permission_times.get(name, 0):
        self.permission_times[name] = timestamp
      if not changed and (gap is None or gap <= 250_000_000):
        continue
      if self.store.full(now):
        self.sampled_out += 1
        continue  # Do not advance the baseline when storage omitted the change.
      self.permission_sequence += 1
      self.store.append({'kind': 'event', 'name': 'lx3_permission_observation',
                         'mono_ns': int(now * 1e9), 'service_mono_ns': timestamp,
                         'service': name, 'sequence': self.permission_sequence,
                         'before': self.permission_states.get(name), 'after': current,
                         'publication_gap_ns': gap,
                         'coverage': 'conflated_publications_not_all_firmware_transitions',
                         'classification': 'observation_only_not_engagement_authority'}, now)
      self.permission_states[name] = current

  def update(self, services, now):
    ignition_fresh = fresh(services, 'deviceState', now, 3)
    started = ignition_fresh and services['deviceState']['data'].get('started') is True
    if not started:
      reason = 'ignition_off' if ignition_fresh else 'ignition_unknown'
      self.store.seal(reason)
      self.trip = None
      self.state = reason
      return
    if self.trip is None:
      self.trip = uuid.uuid4().hex
      self.previous, self.can_last, self.faults, self.last_event = {}, {}, {}, {}
      self.actuation_states = {}
      self.permission_states, self.permission_times = {}, {}
      self.permission_sequence = 0
      self.lead_changes.clear()
      self.last_sample = -math.inf
    if self.store.full(now):
      self.store.seal('rotation')
    if not self.store.begin({**self.metadata, 'trip_id': self.trip, 'mono_ns': int(now * 1e9),
                             'utc_ns': time.time_ns(), 'capture': 'sampled_evidence',
                             'full_can_source': 'rlog_not_copied_by_this_recorder'}, now):
      self.state = self.store.error
      return
    self.state = 'recording'
    self.permission_observations(services, now)
    if now - self.last_sample < .2:
      return
    mode = control_mode(services, now)
    sample = {'kind': 'sample', 'mono_ns': int(now * 1e9), 'mode': mode, 'services': services,
              'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
              'route': self.metadata.get('route')}
    if self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      sample['host_disabled_panda_allowed'] = host_disabled_panda_allowed(services, now)
    self.store.append(sample, now)
    cs = services.get('carState', {}).get('data', {})
    cc = services.get('carControl', {}).get('data', {})
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
    if self.state != 'recording' or address not in self.ADDRESSES or not 0 <= bus < 256 or len(data) > 64:
      return
    if self.store.full(now):
      # Rotate on the next context update; a burst must not bypass the chunk bound.
      self.sampled_out += 1
      return
    if not 0 <= now - mono_ns / 1e9 <= .5:
      self.stale_packets += 1
      return
    if direction == 'rx' and bus >= 128:
      direction = 'tx_echo'
    key = (direction, bus, address, len(data))
    if key not in self.can_last and len(self.can_last) >= 128:
      self.sampled_out += 1
      return
    fault_changed = False
    if address == 0x162 and len(data) == 32 and self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      bits = int.from_bytes(data, 'little')
      value = ((bits >> 219) & 7, (bits >> 246) & 7, (bits >> 234) & 7)
      before = self.faults.get(key)
      fault_changed = value != before
      if fault_changed:
        self.store.append({'kind': 'event', 'name': 'oem_fault_observation', 'mono_ns': mono_ns,
                           'direction': direction, 'bus': bus, 'before': before, 'after': value,
                           'semantic_status': 'dbc_definition_not_causal_diagnosis'}, now)
      self.faults[key] = value
    state_changed = False
    if self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      state = None
      if address == 0x10B and len(data) == 16:
        state = (data[10] & 143,)
      elif address == 0xCB and len(data) == 24:
        state = ((data[3] >> 4) & 3, data[6])
      elif address == 0xEA and len(data) == 24:
        state = (data[6] & 1, data[18] & 3, bool(data[6] & 64), bool(data[18] & 32))
      if state is not None:
        before = self.actuation_states.get(key)
        state_changed = state != before
        if state_changed:
          self.store.append({'kind': 'event', 'name': 'actuation_state_observation', 'mono_ns': mono_ns,
                             'direction': direction, 'bus': bus, 'address': address,
                             'before': before, 'after': state,
                             'semantic_status': 'raw_dbc_state_not_verified_EPS_delivery'}, now)
        self.actuation_states[key] = state
    # Do not rate-limit physical frames by worker wall time: multiple valid
    # 25Hz frames can be drained in one50ms worker batch. Keep their counters
    # for replay, still bounded by drain/chunk/quota limits. Other CAN is sampled.
    period = 0 if address == 0x10B and bus == 0 and direction == 'rx' else .1
    if now - self.can_last.get(key, -math.inf) < period and not fault_changed and not state_changed:
      self.sampled_out += 1
      return
    self.can_last[key] = now
    self.store.append({'kind': 'can_sample', 'mono_ns': mono_ns, 'direction': direction,
                       'bus': bus, 'address': address, 'dlc': len(data), 'data': data.hex()}, now)

  def close(self):
    self.store.seal('server_shutdown')
    self.state = 'stopped'

  def status(self):
    return {'state': self.state, 'trip_id': self.trip, 'bytes': self.store.usage, 'quota': self.store.quota,
            'error': self.store.error, 'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
            'control_changes': False, 'phone_backup': False}
