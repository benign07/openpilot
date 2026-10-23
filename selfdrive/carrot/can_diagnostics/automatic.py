"""Bounded, passive trip evidence. No CAN publishers or control parameter writes."""
from __future__ import annotations

from collections import deque
import hashlib
import gzip
import zlib
import json
import math
import os
from pathlib import Path
import re
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
  def __init__(self, root, *, quota=512 * 1024**2, reserve=512 * 1024**2, seconds=60, chunk_bytes=2 * 1024**2,
               rolling=False):
    self.root = Path(root)
    self.quota, self.reserve = quota, reserve
    self.seconds, self.chunk_bytes = seconds, chunk_bytes
    self.stream = None
    self.error = None
    self.dropped_no_stream = 0
    self.recovery_quarantined = 0
    self.rolling = rolling
    self.retired_chunks = self.retired_bytes = 0
    self.retention_skipped = 0
    self.last_retention_scan = -math.inf
    self.retention_cursor = None
    self.root.mkdir(parents=True, exist_ok=True)
    self.recover()
    # Preserved bad records still consume space; quarantine is not free quota.
    self.usage = sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file())

  def quarantine(self, path):
    directory = self.root / 'recovery_quarantine'
    directory.mkdir(exist_ok=True)
    target = directory / path.name
    if target.exists():
      target = directory / (path.name + '.' + uuid.uuid4().hex)
    os.replace(path, target)
    self.recovery_quarantined += 1

  @staticmethod
  def valid_header(stream):
    line = stream.readline(65537)
    if not line.endswith(b'\n') or len(line) > 65536:
      return False
    try:
      row = json.loads(line)
      return isinstance(row, dict) and row.get('kind') == 'header' and row.get('schema') == 1
    except (ValueError, UnicodeError):
      return False

  def recover(self):
    # A crash during retention may leave a hidden manifest. Restore it when
    # its data survived; otherwise complete the already-started retirement.
    for path in self.root.glob('*.manifest.retiring'):
      chunk = path.name.split('.')[0]
      if not re.fullmatch(r'[a-f0-9]{32}', chunk) or path.is_symlink():
        continue
      data = self.root / (chunk + '.jsonl.gz')
      if data.is_file():
        os.replace(path, self.root / (chunk + '.manifest.json'))
      else:
        path.unlink()
    # Keep complete JSON lines after abrupt power loss; never claim uninterrupted coverage.
    for path in self.root.glob('*.partial'):
      end = 0
      with path.open('rb') as stream:
        header_ok = self.valid_header(stream)
        stream.seek(0)
        for line in stream if header_ok else ():
          try:
            if not line.endswith(b'\n'):
              break
            json.loads(line)
            end = stream.tell()
          except (ValueError, UnicodeError):
            break
      if not header_ok:
        self.quarantine(path)
        continue
      with path.open('r+b') as stream:
        stream.truncate(end)
      target = path.with_suffix('.jsonl')
      os.replace(path, target)
      self.compress(target, 'power_loss_recovered')
    for path in self.root.glob('*.jsonl'):
      with path.open('rb') as stream:
        header_ok = self.valid_header(stream)
      if not header_ok:
        self.quarantine(path)
        continue
      self.compress(path, 'interrupted_seal_recovered')
    for path in self.root.glob('*.jsonl.gz'):
      if not self.manifest_path(path).exists():
        try:
          with gzip.open(path, 'rb') as stream:
            header_ok = self.valid_header(stream)
        except (gzip.BadGzipFile, EOFError, zlib.error):
          header_ok = False
        if not header_ok:
          self.quarantine(path)
          continue
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
    rows = []
    for path in sorted(self.root.glob('*.manifest.json')):
      try:
        rows.append(json.loads(path.read_text(encoding='utf-8')))
      except FileNotFoundError:
        continue  # The collector can retire a completed file during indexing.
    return rows

  def make_room(self, now):
    """Owner's bounded oldest-first policy for the passive recorder only.

    Other callers retain the old preserve-on-full policy. Never touch open,
    quarantined, mismatched or symlinked records. Downloads already opened by
    FileResponse can finish on their original descriptor.
    """
    if not self.rolling or now - self.last_retention_scan < 30:
      return
    self.last_retention_scan = now
    needed = self.chunk_bytes + 65536
    try:
      self.usage = sum(p.stat().st_size for p in self.root.rglob('*') if p.is_file())
      candidates = sorted(self.root.glob('*.manifest.json'), key=lambda p: (p.stat().st_mtime_ns, p.name))
      # Keep the newest two completed records even when no older file can help.
      candidates = [(p.stat().st_mtime_ns, p.name, p) for p in candidates[:-2]]
      if self.retention_cursor is not None:
        candidates = ([row for row in candidates if row[:2] > self.retention_cursor] +
                      [row for row in candidates if row[:2] <= self.retention_cursor])
      scanned_bytes = 0
      deadline = time.monotonic() + .1
      for stamp, name, manifest in candidates[:64]:
        if scanned_bytes >= 8 * 1024**2 or time.monotonic() >= deadline:
          break
        if self.usage + needed <= self.quota and shutil.disk_usage(self.root).free >= self.reserve + self.chunk_bytes:
          break
        self.retention_cursor = (stamp, name)
        chunk = manifest.name.split('.')[0]
        data = self.root / (chunk + '.jsonl.gz')
        if not re.fullmatch(r'[a-f0-9]{32}', chunk) or manifest.is_symlink() or data.is_symlink():
          self.retention_skipped += 1
          continue
        try:
          if manifest.stat().st_size > 4096:
            raise ValueError('oversized manifest')
          row = json.loads(manifest.read_text(encoding='utf-8'))
          if not isinstance(row, dict):
            raise ValueError('invalid manifest')
          size = data.stat().st_size
          if (row.get('id') != chunk or type(row.get('bytes')) is not int or row['bytes'] != size
              or not 0 < size <= self.chunk_bytes + 65536
              or not isinstance(row.get('sha256'), str) or not re.fullmatch(r'[a-f0-9]{64}', row['sha256'])):
            raise ValueError('unverified record')
          if hashlib.sha256(data.read_bytes()).hexdigest() != row['sha256']:
            raise ValueError('record checksum mismatch')
          scanned_bytes += size
          with gzip.open(data, 'rb') as stream:
            if not self.valid_header(stream):
              raise ValueError('invalid record header')
            # Reach EOF/CRC without allowing a damaged or oversized compressed
            # record to be retired merely because its first line was valid.
            if len(stream.read(self.chunk_bytes + 65537)) > self.chunk_bytes + 65536:
              raise ValueError('oversized decompressed record')
          manifest_size = manifest.stat().st_size
          retiring = manifest.with_suffix('.retiring')
          os.replace(manifest, retiring)  # Stop advertising before removing data.
          try:
            data.unlink()
          except OSError:
            os.replace(retiring, manifest)
            raise
          retiring.unlink()
          self.usage -= size + manifest_size
          self.retired_chunks += 1
          self.retired_bytes += size + manifest_size
        except (OSError, ValueError, gzip.BadGzipFile, EOFError, zlib.error):
          self.retention_skipped += 1
    except OSError:
      self.retention_skipped += 1

  def begin(self, metadata, now):
    if self.stream:
      return True
    if self.usage + self.chunk_bytes + 65536 > self.quota or shutil.disk_usage(self.root).free < self.reserve + self.chunk_bytes:
      self.make_room(now)
    if self.usage + self.chunk_bytes + 65536 > self.quota or shutil.disk_usage(self.root).free < self.reserve + self.chunk_bytes:
      self.error = 'storage_full_preserving_records'
      return False
    self.error = None
    self.path = self.root / (uuid.uuid4().hex + '.partial')
    self.stream = self.path.open('xb')
    self.started, self.size, self.last_flush = now, 0, now
    self.append({'kind': 'header', 'schema': 1, **metadata}, now)
    # A power cut before the periodic data flush must not leave an empty chunk.
    self.stream.flush()
    os.fsync(self.stream.fileno())
    if os.name == 'posix':
      fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
      try: os.fsync(fd)
      finally: os.close(fd)
    return True

  def append(self, row, now):
    if not self.stream:
      self.dropped_no_stream += 1
      return False
    data = (json.dumps(clean(row), separators=(',', ':'), ensure_ascii=False, allow_nan=False) + '\n').encode()
    self.stream.write(data)
    self.size += len(data)
    self.usage += len(data)
    if now - self.last_flush >= 1:
      self.stream.flush()
      os.fsync(self.stream.fileno())
      self.last_flush = now
    return True

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
               0x160, 0x1A0, 0x1AA, 0x1BA, 0x1CF, 0x1E0, 0x1EA, 0x2A4, 0x2AF, 0x362}
  LX3_ONLY_ADDRESSES = {0x0A0, 0x105, 0x160, 0x175, 0x1AA, 0x1BA, 0x1CF, 0x1E0, 0x2AF}
  TRACE_ADDRESSES = {0x0CB, 0x0EA, 0x10B, 0x12A, 0x161, 0x162, 0x1A0,
                     0x160, 0x1AA, 0x1BA, 0x1E0, 0x1EA, 0x2A4, 0x2AF, 0x362}
  # LSB bit positions of contiguous raw fields in the installed LX3 DBC.
  # These are candidates, not AVN setting acknowledgements or ECU identities.
  # Do not include counters/geometry: neither is a settings transition.
  ADAS_STATE_FIELDS = {
    0x160: (16, (('AEB_SETTING', 24, 2),)),
    0x1E0: (16, (('HDA_OptUsmSta', 24, 3), ('LFA_OptUsmSta', 27, 3),
                 ('HDA_CntrlModSta', 30, 2), ('HDA_LFA_SymSta', 47, 2), ('HDA_LFA_WrnSnd', 49, 2))),
    0x1AA: (16, (('ADAPTIVE_CRUISE_MAIN_BTN', 34, 1), ('CRUISE_BUTTONS', 36, 3),
                 ('LFA_BTN', 39, 1), ('NORMAL_CRUISE_MAIN_BTN', 41, 1))),
    0x12A: (16, (('LKA_MODE', 24, 3), ('LKA_ACTIVE', 27, 2), ('LKA_WARNING', 32, 1),
                 ('LKA_ICON', 38, 2), ('FCA_SYSWARN', 40, 1), ('STEER_REQ', 52, 1),
                 ('LKAS_ANGLE_ACTIVE', 76, 2), ('HAS_LANE_SAFETY', 80, 1))),
    0x161: (32, (('FCA_ICON', 24, 3), ('FCA_ALT_ICON', 27, 3), ('LKA_ICON', 30, 3),
                 ('ALERTS_1', 128, 6), ('ALERTS_2', 134, 5), ('ALERTS_3', 139, 5),
                 ('ALERTS_4', 144, 8), ('ALERTS_5', 152, 5), ('HDA_ICON', 212, 3), ('LFA_ICON', 224, 3))),
    0x162: (32, (('VIBRATE', 52, 1), ('FAULT_FSS', 213, 3), ('FAULT_FCA', 216, 3),
                 ('FAULT_LSS', 219, 3), ('FAULT_LFA', 234, 3), ('FAULT_HDA', 237, 3), ('FAULT_DAS', 246, 3))),
  }
  ADAS_OBSERVATIONS_PER_SECOND = 20
  SETTINGS_TRACE_MAX_PER_TRIP = 6
  SETTINGS_TRACE_INTERVAL = 30.0
  # Raw samples are kept at 10 Hz outside a button trace. The full-rate trace
  # is bounded in time, memory, and frames, and never transmits a CAN packet.
  BUTTON_TRACE_PRE_NS = 2_000_000_000
  BUTTON_TRACE_POST_NS = 8_000_000_000
  BUTTON_TRACE_MAX_NS = 30_000_000_000
  BUTTON_TRACE_MAX_FRAMES = 20_000
  BUTTON_TRACE_MAX_PER_TRIP = 24
  DIAGNOSTIC_TRACE_MAX_PER_TRIP = 8
  STEERING_TRACE_MAX_PER_TRIP = 8
  STEERING_TRACE_INTERVAL = 10.0
  DIAGNOSTIC_TRACE_INTERVAL = 30.0
  BLINDSPOT_TRACE_MAX_PER_TRIP = 4
  BLINDSPOT_TRACE_INTERVAL = 30.0

  def __init__(self, store, metadata):
    self.store, self.metadata = store, metadata
    self.trip = None
    self.recording_route = None
    self.stale_route = None
    self.route_observed_ns = 0
    self.last_sample = -math.inf
    self.previous = {}
    self.lead_changes = deque(maxlen=20)
    self.last_event = {}
    self.can_last, self.faults = {}, {}
    self.last_panda_sample = -math.inf
    self.sampled_out = self.stale_packets = 0
    self.stale_can_frames = self.stale_panda_snapshots = 0
    self.storage_dropped = 0
    self.chunk_rotations = 0
    self.max_chunk_rotation_ms = 0.0
    self.state = 'waiting_for_ignition'
    self.trace_pre = deque(maxlen=3000)
    self.trace = None
    self.trace_count = 0
    self.trace_pre_evicted = 0
    self.last_button = None
    self.last_button_edge = {}
    self.last_host_state = None
    self.last_panda_state = None
    self.diagnostic_trace_count = 0
    self.steering_trace_count = 0
    self.last_steering_trace = -math.inf
    self.last_lane_departure = None
    self.last_haptic_request = None
    self.last_blindspot_candidate = None
    self.blindspot_trace_count = 0
    self.last_blindspot_trace = -math.inf
    self.last_diagnostic_trace = -math.inf
    self.adas_state = {}
    self.adas_observation_times = deque(maxlen=self.ADAS_OBSERVATIONS_PER_SECOND)
    self.adas_observation_count = self.adas_observation_dropped = self.adas_out_of_order = 0
    self.adas_dropped_since_previous = 0
    self.settings_trace_count = 0
    self.last_settings_trace = -math.inf
    self.transport_baseline = None
    self.transport_identity = None
    self.transport_report_time = -math.inf
    self.transport_previous = {}
    self.transport_discontinuous = False
    self.transport_seen = set()
    self.transport_gaps = set()
    self.last_observer_tick = None
    self.longitudinal_frames = self.longitudinal_rejected = 0
    self.video_index_stale = self.video_index_dropped = self.video_index_route_mismatch = 0

  def trace_active(self, now):
    return self.trace is not None and int(now * 1e9) <= self.trace['until_ns']

  @staticmethod
  def can_row(mono_ns, direction, bus, address, data, source_mono_ns=None, identity=None):
    return {'mono_ns': mono_ns, 'direction': direction, 'bus': bus,
            'address': address, 'dlc': len(data), 'data': data.hex(),
            **({'source_log_mono_ns': source_mono_ns} if source_mono_ns is not None else {}),
            **({'host_tx_identity': identity} if identity else {})}

  def close_trace(self, now, reason):
    if self.trace is not None:
      self.store.append({'kind': 'event', 'name': 'button_trace_end', 'mono_ns': int(now * 1e9),
                         'trace_id': self.trace['id'], 'reason': reason,
                         'frames': self.trace['frames'], 'truncated': self.trace['truncated'],
                         'dropped_full': self.trace['dropped_full'],
                         'stale_packets_delta': self.stale_packets - self.trace['stale_start'],
                         'stale_can_frames_delta': self.stale_can_frames - self.trace['stale_can_start'],
                         'stale_panda_snapshots_delta': self.stale_panda_snapshots - self.trace['stale_panda_start']}, now)
      self.trace = None

  def begin_trace(self, mono_ns, now, label, **details):
    self.trace_count += 1
    self.trace = {'id': uuid.uuid4().hex, 'start_ns': mono_ns,
                  'until_ns': mono_ns + self.BUTTON_TRACE_POST_NS,
                  'frames': 0, 'truncated': False, 'dropped_full': 0,
                  'stale_start': self.stale_packets, 'stale_can_start': self.stale_can_frames,
                  'stale_panda_start': self.stale_panda_snapshots}
    # Retain the existing chunk/reader contract for all bounded trace windows.
    self.store.append({'kind': 'event', 'name': 'button_trace_start', 'mono_ns': mono_ns,
                       'pre_window_before_route_observation': any(
                         mono_ns - self.BUTTON_TRACE_PRE_NS <= row[0] < mono_ns and row[0] < self.route_observed_ns
                         for row in self.trace_pre),
                       'trace_id': self.trace['id'], 'button': label,
                       'pre_ns': self.BUTTON_TRACE_PRE_NS, 'post_ns': self.BUTTON_TRACE_POST_NS,
                       **details}, now)
    for pre_ns, direction, bus, address, data, source_mono_ns, identity in self.trace_pre:
      if mono_ns - self.BUTTON_TRACE_PRE_NS <= pre_ns < mono_ns:
        self.append_trace_frame(self.can_row(pre_ns, direction, bus, address, data, source_mono_ns, identity), now)

  def diagnostic_trigger(self, reason, mono_ns, now, **details):
    if self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV' or self.state != 'recording':
      return
    if not self.rotate_full_chunk(now):
      self.storage_dropped += 1
      return
    if self.trace is not None and not self.trace_active(now):
      self.close_trace(now, 'window_elapsed')
    if now - self.last_event.get('diagnostic:' + reason, -math.inf) < 5.0:
      return
    self.last_event['diagnostic:' + reason] = now
    steering = reason in ('lane_departure', 'haptic_request', 'oem_fault_edge')
    blindspot = reason == 'blindspot_warning_candidate'
    settings = reason == 'adas_setting_candidate_edge'
    if settings:
      quota_available = self.settings_trace_count < self.SETTINGS_TRACE_MAX_PER_TRIP
      last_trace, interval = self.last_settings_trace, self.SETTINGS_TRACE_INTERVAL
    elif blindspot:
      quota_available = self.blindspot_trace_count < self.BLINDSPOT_TRACE_MAX_PER_TRIP
      last_trace, interval = self.last_blindspot_trace, self.BLINDSPOT_TRACE_INTERVAL
    elif steering:
      quota_available = self.steering_trace_count < self.STEERING_TRACE_MAX_PER_TRIP
      last_trace, interval = self.last_steering_trace, self.STEERING_TRACE_INTERVAL
    else:
      quota_available = (self.diagnostic_trace_count - self.steering_trace_count - self.blindspot_trace_count - self.settings_trace_count
                         < self.DIAGNOSTIC_TRACE_MAX_PER_TRIP)
      last_trace, interval = self.last_diagnostic_trace, self.DIAGNOSTIC_TRACE_INTERVAL
    if self.trace is None and quota_available and now - last_trace >= interval:
      self.begin_trace(mono_ns, now, 'diagnostic:' + reason, trigger=reason, trigger_details=details)
      self.diagnostic_trace_count += 1
      if settings:
        self.settings_trace_count += 1
        self.last_settings_trace = now
      elif blindspot:
        self.blindspot_trace_count += 1
        self.last_blindspot_trace = now
      elif steering:
        self.steering_trace_count += 1
        self.last_steering_trace = now
      else:
        self.last_diagnostic_trace = now
    # Sustained SPI errors cannot continually extend a window or consume the
    # reserved physical-button window quota. Ordinary bounded samples continue.
    self.store.append({'kind': 'event', 'name': 'diagnostic_trace_trigger', 'mono_ns': mono_ns,
                       'observed_mono_ns': int(now * 1e9), 'reason': reason,
                       'trace_id': self.trace['id'] if self.trace_active(now) else None,
                       'classification': 'observation_not_causal_diagnosis', **details}, now)

  def observer_tick(self, now):
    before = self.last_observer_tick
    self.last_observer_tick = now
    if before is not None and now - before > .3:
      self.diagnostic_trigger('observer_gap', int(now * 1e9), now,
                              elapsed_s=now-before, meaning='collector_gap_not_producer_outage_proof',
                              pre_window_may_be_sparse_due_to_collector_gap=True)

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
      if self.trace is None and self.trace_count - self.diagnostic_trace_count < self.BUTTON_TRACE_MAX_PER_TRIP:
        self.begin_trace(mono_ns, now, button)
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
    if not self.rotate_full_chunk(now):
      self.trace['dropped_full'] += 1
      self.storage_dropped += 1
      return
    if self.trace['frames'] >= self.BUTTON_TRACE_MAX_FRAMES:
      self.trace['truncated'] = True
      self.store.append({'kind': 'event', 'name': 'button_trace_truncated',
                         'mono_ns': int(now * 1e9), 'trace_id': self.trace['id'],
                         'reason': 'frame_limit'}, now)
      return
    if self.store.append({'kind': 'button_trace_can', 'trace_id': self.trace['id'], **row}, now) is False:
      self.trace['dropped_full'] += 1
      self.storage_dropped += 1
    else:
      self.trace['frames'] += 1

  def chunk_metadata(self, now):
    return {**self.metadata, 'route': self.recording_route, 'trip_id': self.trip, 'mono_ns': int(now * 1e9),
            'route_observed_mono_ns': self.route_observed_ns,
            'utc_ns': time.time_ns(), 'capture': 'sampled_evidence',
            'full_can_source': 'rlog_not_copied_by_this_recorder'}

  def rotate_full_chunk(self, now):
    if not self.store.full(now):
      return True
    started = time.monotonic()
    self.store.seal('rotation')
    ready = self.store.begin(self.chunk_metadata(now), now)
    self.chunk_rotations += 1
    self.max_chunk_rotation_ms = max(self.max_chunk_rotation_ms, (time.monotonic() - started) * 1000)
    if not ready:
      self.state = self.store.error
    return ready

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
      if ignition_fresh and self.recording_route is not None:
        # CurrentRoute is not cleared offroad. Only a confirmed off marks the
        # completed route stale; a missing deviceState must still recover it.
        self.stale_route, self.recording_route = self.recording_route, None
      self.close_trace(now, reason)
      self.store.seal(reason)
      self.trip = None
      self.state = reason
      return
    if self.trip is None:
      self.trip = uuid.uuid4().hex
      self.route_observed_ns = int(now * 1e9)
      self.previous, self.can_last, self.faults, self.last_event = {}, {}, {}, {}
      self.last_panda_sample = -math.inf
      self.lead_changes.clear()
      self.last_sample = -math.inf
      self.trace_pre.clear()
      self.trace = None
      self.trace_count = self.trace_pre_evicted = 0
      self.last_button = self.last_host_state = self.last_panda_state = None
      self.last_button_edge.clear()
      self.diagnostic_trace_count = 0
      self.steering_trace_count = 0
      self.last_steering_trace = -math.inf
      self.last_lane_departure = self.last_haptic_request = None
      self.last_blindspot_candidate = None
      self.blindspot_trace_count = 0
      self.last_blindspot_trace = -math.inf
      self.last_diagnostic_trace = -math.inf
      self.adas_state.clear()
      self.adas_observation_times.clear()
      self.adas_observation_count = self.adas_observation_dropped = self.adas_out_of_order = 0
      self.adas_dropped_since_previous = 0
      self.settings_trace_count = 0
      self.last_settings_trace = -math.inf
      self.transport_baseline = self.transport_identity = None
      self.transport_report_time = -math.inf
      self.transport_previous = {}
      self.transport_discontinuous = False
      self.transport_seen = set()
      self.transport_gaps = set()
      self.last_observer_tick = None
      self.chunk_rotations = 0
      self.max_chunk_rotation_ms = 0.0
    route = self.metadata.get('route')
    route = None if route == self.stale_route else route
    route_rotation_started = None
    if route != self.recording_route:
      # loggerd can change route without an ignition-off sample. A chunk has
      # one immutable route header; never append the next route below it.
      if self.store.stream is not None:
        route_rotation_started = time.monotonic()
      self.close_trace(now, 'route_changed')
      self.store.seal('route_changed')
      # Same-boot native pre-event evidence remains useful. Its source clock
      # and the trace-start annotation expose pre-observation coverage.
      self.last_sample = -math.inf
      self.recording_route = route
      self.route_observed_ns = int(now * 1e9)
      self.adas_state.clear()
    if self.trace is not None and not self.trace_active(now):
      self.close_trace(now, 'window_elapsed')
    ready = self.rotate_full_chunk(now) and self.store.begin(self.chunk_metadata(now), now)
    if route_rotation_started is not None:
      self.chunk_rotations += 1
      self.max_chunk_rotation_ms = max(self.max_chunk_rotation_ms, (time.monotonic() - route_rotation_started) * 1000)
    if not ready:
      self.state = self.store.error
      return
    self.state = 'recording'
    if now - self.last_sample < (.05 if self.trace_active(now) else .2):
      return
    mode = control_mode(services, now)
    sample = {'kind': 'sample', 'mono_ns': int(now * 1e9), 'mode': mode, 'services': services,
              'trace_id': self.trace['id'] if self.trace_active(now) else None,
              'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
              'stale_can_frames': self.stale_can_frames, 'stale_panda_snapshots': self.stale_panda_snapshots,
              'storage_dropped': self.storage_dropped, 'dropped_no_stream': self.store.dropped_no_stream,
              'adas_state_evidence': {'observations': self.adas_observation_count,
                                      'dropped': self.adas_observation_dropped,
                                      'out_of_order': self.adas_out_of_order},
              'route': self.recording_route}
    self.store.append(sample, now)
    cs = services.get('carState', {}).get('data', {})
    cc = services.get('carControl', {}).get('data', {})
    sd = services.get('selfdriveState', {}).get('data', {})
    # A side-object flag plus a same-side blinker is only a search candidate.
    # It does not identify rear closing speed, an audible warning, or vibration.
    keys = ('leftBlinker', 'rightBlinker', 'leftBlindspot', 'rightBlindspot')
    if (fresh(services, 'carState', now, .3) and services['carState'].get('alive', True) is True
        and all(type(cs.get(key)) is bool for key in keys)):
      candidate = (cs['leftBlinker'] and cs['leftBlindspot'], cs['rightBlinker'] and cs['rightBlindspot'])
      before = self.last_blindspot_candidate
      if any(candidate) and (before is None or any(value and not old for value, old in zip(candidate, before))):
        self.diagnostic_trigger('blindspot_warning_candidate', services['carState']['mono_ns'], now,
                                left=candidate[0], right=candidate[1], initial_observation=before is None,
                                v_ego=cs.get('vEgo'), lat_active=cc.get('latActive'), long_active=cc.get('longActive'),
                                warning_confirmed=False, haptic_confirmed=False,
                                meaning='same_side_blinker_and_object_not_rear_approach_or_actuation_proof',
                                unknown_address_search='match_full_rlog_on_PC')
      self.last_blindspot_candidate = candidate
    else:
      self.last_blindspot_candidate = None
    if fresh(services, 'carControl', now):
      hud = cc.get('hudControl') or {}
      left, right = hud.get('leftLaneDepart'), hud.get('rightLaneDepart')
      if type(left) is bool and type(right) is bool:
        departure = (left, right)
        if departure != self.last_lane_departure and any(departure):
          self.diagnostic_trigger('lane_departure', services['carControl']['mono_ns'], now,
                                  left=left, right=right, lat_active=cc.get('latActive'),
                                  audible_alert=hud.get('audibleAlert'),
                                  meaning='host_departure_detection_not_oem_fault_proof')
        self.last_lane_departure = departure
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

  def adas_observation(self, bus, address, data, mono_ns, now, direction, source_mono_ns, identity):
    spec = self.ADAS_STATE_FIELDS.get(address)
    if (self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV'
        or spec is None or len(data) != spec[0]):
      return
    key = direction, bus, address
    previous = self.adas_state.get(key)
    if previous is None and len(self.adas_state) >= 128:
      self.adas_observation_dropped += 1
      self.adas_dropped_since_previous += 1
      return
    if previous is not None and mono_ns < previous[0]:
      self.adas_out_of_order += 1
      return
    bits = int.from_bytes(data, 'little')
    fields = {name: (bits >> start) & ((1 << size)-1) for name, start, size in spec[1]}
    self.adas_state[key] = mono_ns, fields
    gap = previous is not None and mono_ns-previous[0] > 1_000_000_000
    if previous is not None and fields == previous[1] and not gap:
      return
    before = previous[1] if previous is not None and not gap else None
    # Preserve short button pulses even between the 10 Hz raw samples. A
    # noisy stream must not turn this into unbounded full CAN recording.
    while self.adas_observation_times and now-self.adas_observation_times[0] >= 1.0:
      self.adas_observation_times.popleft()
    if len(self.adas_observation_times) >= self.ADAS_OBSERVATIONS_PER_SECOND:
      self.adas_observation_dropped += 1
      self.adas_dropped_since_previous += 1
      return
    self.adas_observation_times.append(now)
    changed = [name for name in fields if before is not None and fields[name] != before[name]]
    setting_fields = {0x160: {'AEB_SETTING'}, 0x1E0: {'HDA_OptUsmSta', 'LFA_OptUsmSta'}}
    # Only an observed settings-candidate edge opens its own bounded window.
    # Automatic host button pulses never open or prolong a window themselves.
    if direction == 'rx' and set(changed) & setting_fields.get(address, set()):
      self.diagnostic_trigger('adas_setting_candidate_edge', mono_ns, now, bus=bus,
                              address=address, changed_fields=changed,
                              meaning='raw_candidate_not_user_action_or_setting_ack')
    row = {'kind': 'event', 'name': 'adas_state_observation',
           **self.can_row(mono_ns, direction, bus, address, data, source_mono_ns, identity),
           'observed_mono_ns': int(now*1e9), 'fields': fields, 'before': before,
           'phase': 'baseline_after_gap' if gap else 'baseline' if previous is None else 'edge',
           'previous_mono_ns': previous[0] if previous is not None else None,
           'same_timestamp_as_previous': previous is not None and mono_ns == previous[0],
           'classification': 'dbc_candidate_not_setting_ack_or_cause',
           'integrity': 'not_validated_by_recorder',
           'dropped_since_previous': self.adas_dropped_since_previous,
           'observation_index': self.adas_observation_count+1,
           'trace_id': self.trace['id'] if self.trace_active(now) else None}
    if self.store.append(row, now) is False:
      self.adas_observation_dropped += 1
      self.adas_dropped_since_previous += 1
    else:
      self.adas_observation_count += 1
      self.adas_dropped_since_previous = 0

  def can_frame(self, bus, address, data, mono_ns, now, direction='rx', source_mono_ns=None, identity=None):
    if (address not in self.ADDRESSES or not 0 <= bus < 256 or len(data) > 64 or
        (address in self.LX3_ONLY_ADDRESSES and self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV')):
      return
    if self.state != 'recording':
      if self.state == 'storage_full_preserving_records' and 0 <= now - mono_ns / 1e9 <= .5:
        self.storage_dropped += 1
        if self.trace_active(now) and address in self.TRACE_ADDRESSES:
          self.trace['dropped_full'] += 1
      return
    if not 0 <= now - mono_ns / 1e9 <= .5:
      self.stale_packets += 1
      self.stale_can_frames += 1
      return
    # Continue a bounded trace across chunks instead of dropping the rest of
    # this receive burst until the next context tick. Quota/free-space checks
    # still apply, and their failures remain explicit loss, not success.
    if not self.rotate_full_chunk(now):
      if self.trace_active(now) and address in self.TRACE_ADDRESSES:
        self.trace['dropped_full'] += 1
      self.storage_dropped += 1
      return
    if direction == 'rx':
      if bus >= 192:
        direction = 'tx_rejected'
      elif bus >= 128:
        direction = 'tx_echo'
    self.adas_observation(bus, address, data, mono_ns, now, direction, source_mono_ns, identity)
    self.button_edges(bus, address, data, mono_ns, now, direction)
    if address == 0x162 and len(data) == 32 and direction == 'tx_requested':
      vibrate = bool(data[6] & 0x10)  # DBC 52|1@0, meaning still needs LX3 qualification.
      if vibrate and self.last_haptic_request is not True:
        self.diagnostic_trigger('haptic_request', mono_ns, now,
                                bus=bus,
                                meaning='host_bit_request_not_ecu_ack_or_motor_response')
      self.last_haptic_request = vibrate
    key = (direction, bus, address, len(data))
    fault_changed = False
    if address == 0x162 and len(data) == 32 and self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      bits = int.from_bytes(data, 'little')
      value = ((bits >> 219) & 7, (bits >> 246) & 7)
      before = self.faults.get(key)
      fault_changed = value != before
      if fault_changed:
        # TX echo/request observations are not evidence that the OEM ECU
        # originated a fault. Only a physical RX observation opens this window.
        if direction == 'rx' and ((before is not None and value != before) or any(value)):
          self.diagnostic_trigger('oem_fault_edge', mono_ns, now, bus=bus, before=before, after=value)
        self.store.append({'kind': 'event', 'name': 'oem_fault_observation', 'mono_ns': mono_ns,
                           'direction': direction, 'bus': bus, 'before': before, 'after': value,
                           'trace_id': self.trace['id'] if self.trace_active(now) else None,
                           'semantic_status': 'dbc_definition_not_causal_diagnosis'}, now)
      self.faults[key] = value
    if address in self.TRACE_ADDRESSES:
      if self.trace_active(now):
        self.append_trace_frame(self.can_row(mono_ns, direction, bus, address, data, source_mono_ns, identity), now)
      if len(self.trace_pre) == self.trace_pre.maxlen:
        self.trace_pre_evicted += 1
      self.trace_pre.append((mono_ns, direction, bus, address, data, source_mono_ns, identity))
      while self.trace_pre and mono_ns - self.trace_pre[0][0] > self.BUTTON_TRACE_PRE_NS:
        self.trace_pre.popleft()
    if key not in self.can_last and len(self.can_last) >= 128:
      self.sampled_out += 1
      return
    if now - self.can_last.get(key, -math.inf) < .1 and not fault_changed:
      self.sampled_out += 1
      return
    self.can_last[key] = now
    self.store.append({'kind': 'can_sample', **self.can_row(mono_ns, direction, bus, address, data, source_mono_ns, identity)}, now)

  def panda_snapshot(self, states, mono_ns, now, source_mono_ns=None):
    if not states:
      return
    if self.state != 'recording':
      if self.state == 'storage_full_preserving_records' and 0 <= now - mono_ns / 1e9 <= .5:
        self.storage_dropped += 1
      return
    if not 0 <= now - mono_ns / 1e9 <= .5:
      self.stale_packets += 1
      self.stale_panda_snapshots += 1
      return
    if not self.rotate_full_chunk(now):
      self.storage_dropped += 1
      return
    current = states[0]
    fields = ('controls_allowed', 'rx_checks_invalid', 'rx_overflow', 'tx_overflow',
              'tx_blocked', 'safety_model', 'safety_param', 'lx3_authority_state')
    # Cumulative counters remain visible in periodic samples. Their sustained
    # growth must not turn ordinary recording into full-rate health logging.
    edge_fields = ('controls_allowed', 'rx_checks_invalid', 'safety_model', 'safety_param', 'lx3_authority_state')
    changed = self.last_panda_state is not None and any(
      self.last_panda_state.get(field) != current.get(field) for field in edge_fields)
    # Keep ordinary sampling bounded, but preserve received LX3 authority or
    # configuration changes even when a short pulse falls between periodic samples.
    lx3_edge = self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV' and changed
    if now - self.last_panda_sample < (.1 if self.trace_active(now) else .5) and not lx3_edge:
      if self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
        self.transport_observation(current, mono_ns, now)
      return
    self.last_panda_sample = now
    self.store.append({'kind': 'panda_snapshot', 'mono_ns': mono_ns, 'states': states[:4],
                       **({'source_log_mono_ns': source_mono_ns} if source_mono_ns is not None else {}),
                       'trace_id': self.trace['id'] if self.trace_active(now) else None}, now)
    edge_details = {}
    if self.last_panda_state is not None:
      for field in fields:
        before, after = self.last_panda_state.get(field), current.get(field)
        if before != after:
          edge_details[field] = {'before': before, 'after': after}
          self.store.append({'kind': 'event', 'name': 'panda_state_edge', 'mono_ns': mono_ns,
                             'field': field, 'before': before, 'after': after,
                             'trace_id': self.trace['id'] if self.trace_active(now) else None}, now)
    self.last_panda_state = current.copy()
    # Preserve the first state/cause before copying the pre-window, which may
    # exhaust a small chunk. Full-rate evidence must not displace its trigger.
    if lx3_edge:
      self.diagnostic_trigger('panda_authority_edge', mono_ns, now, changes=edge_details)
    if self.metadata.get('car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV':
      self.transport_observation(current, mono_ns, now)

  def transport_observation(self, current, mono_ns, now):
    identity = (current.get('safety_model'), current.get('safety_param'),
                (current.get('lx3_authority_state') or {}).get('epoch'))
    counters = {key: current[key] for key in ('spi_checksum_errors', 'rx_overflow', 'tx_overflow', 'tx_blocked')
                if isinstance(current.get(key), int) and current[key] >= 0}
    if self.transport_baseline is None or identity != self.transport_identity:
      self.transport_identity, self.transport_baseline = identity, counters
      self.transport_previous = counters
      self.transport_discontinuous = False
      self.transport_seen = set(counters)
      self.transport_gaps = set()
      self.transport_report_time = now
      self.store.append({'kind': 'event', 'name': 'transport_counter_baseline', 'mono_ns': mono_ns,
                         'boot_id': self.metadata.get('boot_id'), 'identity': identity, 'counters': counters,
                         'spi_saturated': counters.get('spi_checksum_errors') == 65535}, now)
      return
    if any(after < self.transport_previous.get(key, after) for key, after in counters.items()):
      self.transport_discontinuous = True
    self.transport_previous = counters
    self.transport_gaps.update(self.transport_seen - set(counters))
    if counters == self.transport_baseline and not self.transport_gaps:
      return
    # An initial increase opens a bounded window immediately. Subsequent
    # summaries aggregate five seconds instead of creating per-packet records.
    elapsed = now - self.transport_report_time
    if elapsed < 5.0 and now - self.last_event.get('diagnostic:transport_counter_change', -math.inf) < 5.0:
      return
    changes = {}
    for key in self.transport_baseline.keys() | counters.keys():
      after = counters.get(key)
      before = self.transport_baseline.get(key)
      if before == after and key not in self.transport_gaps:
        continue
      if after is None or before is None or key in self.transport_gaps:
        status = 'counter_missing' if after is None else ('counter_returned' if key in self.transport_seen else 'counter_first_observed')
        changes[key] = {'before': before, 'after': after, 'status': status, 'delta': None, 'rate_per_s': None}
        continue
      saturated = key == 'spi_checksum_errors' and (before == 65535 or after == 65535)
      discontinuous = self.transport_discontinuous or after < before
      status = 'decreased_reset_or_wrap' if discontinuous else ('saturated_lower_bound' if saturated else 'increased')
      changes[key] = {'before': before, 'after': after, 'status': status,
                      'delta': after-before if after >= before and not discontinuous else None,
                      'rate_per_s': (after-before)/elapsed if after > before and not saturated and not discontinuous and elapsed >= 1.0 else None}
    if changes:
      self.diagnostic_trigger('transport_counter_change', mono_ns, now, elapsed_s=elapsed, changes=changes,
                              spi_meaning='aggregate_counter_not_proof_of_can_corruption_or_queue_lag')
    self.transport_baseline, self.transport_report_time = counters, now
    self.transport_discontinuous = False
    self.transport_seen.update(counters)
    self.transport_gaps.clear()

  def longitudinal_evidence(self, row, source_ns, now):
    """Forward bounded producer evidence through the existing phone export."""
    if (self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV'
        or self.state != 'recording'):
      return
    if (not isinstance(row, dict) or row.get('schema') != 1
        or type(row.get('mono_ns')) is not int or not 0 <= now - row['mono_ns']/1e9 <= 1.
        or row['mono_ns'] < self.route_observed_ns
        or not isinstance(row.get('producer'), str) or len(row['producer']) != 32
        or type(row.get('sequence')) is not int or row['sequence'] <= 0):
      self.longitudinal_rejected += 1
      return
    try:
      if len(json.dumps(row, separators=(',', ':'), allow_nan=False).encode()) > 32768:
        raise ValueError('oversized evidence')
    except (TypeError, ValueError):
      self.longitudinal_rejected += 1
      return
    if not self.rotate_full_chunk(now):
      self.storage_dropped += 1
      return
    self.store.append({**row, 'kind': 'longitudinal_evidence', 'source_log_mono_ns': source_ns,
                       'route': self.recording_route,
                       'trip_id': self.trip, 'boot_id': self.metadata.get('boot_id')}, now)
    self.longitudinal_frames += 1

  def lateral_evidence(self, row, source_ns, now):
    # Existing event envelope keeps older HUD/PC receivers compatible. This is
    # evidence of a planner request, never a physical displacement or ECU ACK.
    if self.metadata.get('car_fingerprint') != 'HYUNDAI_PALISADE_LX3_HEV' or self.state != 'recording':
      return
    if (not isinstance(row, dict) or row.get('schema') != 1
        or type(row.get('mono_ns')) is not int or row['mono_ns'] < self.route_observed_ns
        or not 0 <= now - row['mono_ns']/1e9 <= 1.):
      return
    try:
      if len(json.dumps(row, separators=(',', ':'), allow_nan=False).encode()) > 16384:
        return
    except (TypeError, ValueError):
      return
    if now - self.last_event.get('lateral_margin_evidence', -math.inf) < .5:
      return
    if not self.rotate_full_chunk(now):
      self.storage_dropped += 1
      return
    self.last_event['lateral_margin_evidence'] = now
    self.store.append({'kind': 'event', 'name': 'lateral_margin_evidence', 'mono_ns': row['mono_ns'],
                       'source_log_mono_ns': source_ns, 'route': self.recording_route,
                       'trip_id': self.trip, 'boot_id': self.metadata.get('boot_id'), 'evidence': row}, now)

  def video_index(self, index, source_ns, now, observed_route, observed_ns, native_offset_ns):
    """Preserve native clock evidence only with a qualified current route."""
    if self.state != 'recording':
      return
    if not 0. <= now - (source_ns - native_offset_ns) / 1e9 <= .5:
      self.video_index_stale += 1
      return
    if (self.recording_route is None or observed_route != self.recording_route
        or int(index['timestampSof']) - native_offset_ns < self.route_observed_ns):
      self.video_index_route_mismatch += 1
      return
    if self.rotate_full_chunk(now):
      self.store.append({'kind': 'video_index', 'index': index, 'route': self.recording_route,
                         'route_observed_mono_ns': observed_ns, 'source_log_mono_ns': source_ns,
                         'native_clock_offset_ns': native_offset_ns}, now)
    else:
      self.video_index_dropped += 1

  def close(self):
    self.close_trace(time.monotonic(), 'server_shutdown')
    self.store.seal('server_shutdown')
    self.state = 'stopped'

  def status(self):
    return {'state': self.state, 'trip_id': self.trip, 'recording_route': self.recording_route,
            'bytes': self.store.usage, 'quota': self.store.quota,
            'error': self.store.error, 'sampled_out': self.sampled_out, 'stale_can_packets': self.stale_packets,
            'stale_can_frames': self.stale_can_frames, 'stale_panda_snapshots': self.stale_panda_snapshots,
            'storage_dropped': self.storage_dropped, 'dropped_no_stream': self.store.dropped_no_stream,
            'recovery_quarantined': self.store.recovery_quarantined,
            'diagnostic_retention': {'policy': 'oldest_completed_first' if self.store.rolling else 'preserve_on_full',
                                     'retired_chunks': self.store.retired_chunks, 'retired_bytes': self.store.retired_bytes,
                                     'skipped_unverified': self.store.retention_skipped},
            'video_index_source': 'completed_qlog_on_pc_not_live_published',
            'chunk_rotations': self.chunk_rotations, 'max_chunk_rotation_ms': self.max_chunk_rotation_ms,
            'button_traces': self.trace_count, 'button_trace_active': self.trace is not None,
            'button_trace_pre_evicted': self.trace_pre_evicted,
            'diagnostic_traces': self.diagnostic_trace_count,
            'steering_diagnostic_traces': self.steering_trace_count,
            'blindspot_diagnostic_traces': self.blindspot_trace_count,
            'settings_diagnostic_traces': self.settings_trace_count,
            'adas_state_observations': self.adas_observation_count,
            'adas_observation_dropped': self.adas_observation_dropped,
            'adas_out_of_order': self.adas_out_of_order,
            'longitudinal_evidence_frames': self.longitudinal_frames,
            'longitudinal_evidence_rejected': self.longitudinal_rejected,
            'lateral_evidence_transport': 'bounded_event_lateral_margin_evidence',
            'video_index_stale': self.video_index_stale, 'video_index_dropped': self.video_index_dropped,
            'video_index_route_mismatch': self.video_index_route_mismatch,
            'control_changes': False, 'phone_backup': False}
