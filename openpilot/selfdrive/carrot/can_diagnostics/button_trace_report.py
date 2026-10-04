"""Offline, read-only correlation of physical button traces and responses."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path


PAIRS = {0x1A0: (2, 128), 0x0CB: (2, 128), 0x0EA: (0, 130), 0x12A: (2, 128)}
KST = timezone(timedelta(hours=9))


def verified_rows(root):
  manifests = sorted(Path(root).glob('*.manifest.json'))
  if not manifests:
    raise ValueError('no sealed automatic diagnostic chunks found')
  for manifest_path in manifests:
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    chunk = manifest_path.parent / (manifest['id'] + '.jsonl.gz')
    payload = chunk.read_bytes()
    if len(payload) != manifest['bytes'] or hashlib.sha256(payload).hexdigest() != manifest['sha256']:
      raise ValueError(f'button trace chunk hash mismatch: {chunk.name}')
    with gzip.open(chunk, 'rt', encoding='utf-8') as stream:
      header = json.loads(next(stream))
      offset = header['utc_ns'] - header['mono_ns']
      for line in stream:
        row = json.loads(line)
        if row.get('trace_id'):
          row['_utc_ns'] = row['mono_ns'] + offset
          row['_car_fingerprint'] = header.get('car_fingerprint')
          yield row


def _relative(row, start_ns):
  return round((row['mono_ns'] - start_ns) / 1e6, 1)


def _can_pair_counts(frames):
  groups = defaultdict(list)
  for frame in frames:
    if frame['address'] in PAIRS:
      groups[(frame['mono_ns'], frame['address'])].append(frame)
  result = {}
  for address, (original_bus, returned_src) in PAIRS.items():
    counts = Counter()
    for (_, addr), rows in groups.items():
      if addr != address:
        continue
      originals = [row for row in rows if row['direction'] == 'rx' and row['bus'] == original_bus]
      returned = [row for row in rows if row['direction'] == 'tx_echo' and row['bus'] == returned_src]
      counts['original_rx'] += len(originals)
      counts['returned_tx'] += len(returned)
      if len(originals) == len(returned) == 1:
        before, after = bytes.fromhex(originals[0]['data']), bytes.fromhex(returned[0]['data'])
        counts['paired'] += 1
        counts['same_counter'] += len(before) > 2 and len(after) > 2 and before[2] == after[2]
        counts['payload_changed'] += before[3:] != after[3:]
        counts['payload_unchanged'] += before[3:] == after[3:]
    result[f'0x{address:03X}'] = dict(counts)
  return result


def summarize(root):
  traces = defaultdict(lambda: {'start': None, 'end': None, 'frames': [], 'events': [], 'samples': []})
  for row in verified_rows(root):
    trace = traces[row['trace_id']]
    if row['kind'] == 'button_trace_can':
      trace['frames'].append(row)
    elif row['kind'] == 'sample':
      trace['samples'].append(row)
    elif row['kind'] == 'event':
      if row['name'] == 'button_trace_start':
        trace['start'] = row
      elif row['name'] == 'button_trace_end':
        trace['end'] = row
      else:
        trace['events'].append(row)
  result = []
  for trace_id, trace in traces.items():
    start = trace['start']
    if start is None:
      raise ValueError(f'button trace start missing: {trace_id}')
    start_ns = start['mono_ns']
    events = sorted(trace['events'], key=lambda row: row['mono_ns'])
    important = {'host_state_edge', 'panda_state_edge', 'oem_fault_observation', 'physical_button_edge'}
    timeline = [{key: value for key, value in row.items() if key in
                 ('name', 'button', 'field', 'before', 'after', 'direction', 'bus', 'raw_cruise', 'lfa_btn')}
                | {'after_button_ms': _relative(row, start_ns)}
                for row in events if row['name'] in important]
    counts = Counter(f"{frame['direction']}/bus{frame['bus']}/0x{frame['address']:03X}"
                     for frame in trace['frames'])
    result.append({
      'trace_id': trace_id, 'button': start['button'], 'car_fingerprint': start.get('_car_fingerprint'),
      'start_kst': datetime.fromtimestamp(start['_utc_ns'] / 1e9, KST).isoformat(timespec='milliseconds'),
      'complete': trace['end'] is not None,
      'end_reason': trace['end']['reason'] if trace['end'] else 'missing_end',
      'recorded_frames': len(trace['frames']),
      'dropped_full': trace['end'].get('dropped_full', 0) if trace['end'] else None,
      'truncated': trace['end'].get('truncated') if trace['end'] else None,
      'stale_packets': trace['end'].get('stale_packets_delta', 0) if trace['end'] else None,
      'context_samples': len(trace['samples']),
      'frame_counts': dict(counts),
      'forward_pairs': _can_pair_counts(trace['frames']) if start.get('_car_fingerprint') == 'HYUNDAI_PALISADE_LX3_HEV' else {},
      'timeline': timeline,
    })
  return {'schema': 1, 'source': str(Path(root)), 'traces': sorted(result, key=lambda row: row['start_kst']),
          'limits': ['TX echo is not OEM acceptance or an actuator response.',
                     'Only selected CAN addresses are captured at full rate during bounded windows.',
                     'A missing host or Panda edge may reflect source publication or recorder gaps; counters only show known gaps.',
                     'Button labels follow the installed vehicle DBC and require physical input review.',
                     'LX3 forwarding pairs are not inferred for EV9 without vehicle-specific bus validation.']}


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('chunk_directory', type=Path)
  parser.add_argument('--out', type=Path)
  args = parser.parse_args()
  result = summarize(args.chunk_directory)
  payload = json.dumps(result, ensure_ascii=False, indent=2) + '\n'
  if args.out:
    args.out.write_text(payload, encoding='utf-8')
  else:
    print(payload, end='')


if __name__ == '__main__':
  main()
