"""Read-only, dated context review of phone trip chunks. No vehicle writes."""
import argparse
import bisect
from collections import Counter, defaultdict
import datetime as dt
import gzip
import json
import math
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.can_auto_sync import digest, fresh, atomic_json, ID, HASH

KST = dt.timezone(dt.timedelta(hours=9))


def iso(ns):
  return dt.datetime.fromtimestamp(ns / 1e9, KST).isoformat(timespec='milliseconds')


def value(sample, name):
  return sample.get('services', {}).get(name, {}).get('data', {})


def valid(sample):
  return (sample.get('mode') != 'unknown' and value(sample, 'carState').get('canValid') is True and
          all(fresh(sample['services'], key, sample['mono_ns']) for key in ('carState', 'carControl', 'selfdriveState')))


def context(sample):
  cs, cc, radar, sd = (value(sample, name) for name in ('carState', 'carControl', 'radarState', 'selfdriveState'))
  lead = radar.get('leadOne') or {}; actuator = cc.get('actuators') or {}
  return {'at': iso(sample['_utc']), 'valid': valid(sample), 'mode': sample.get('mode'),
          'speed_kph': round((cs.get('vEgo') or 0) * 3.6, 2), 'aEgo': cs.get('aEgo'),
          'brake': cs.get('brakePressed'), 'gas': cs.get('gasPressed'), 'hands': cs.get('steeringPressed'),
          'blinkers': [cs.get('leftBlinker'), cs.get('rightBlinker')], 'lat_active': cc.get('latActive'),
          'long_active': cc.get('longActive'), 'requested_accel': actuator.get('accel'),
          'measured_angle': cs.get('steeringAngleDeg'), 'requested_angle': actuator.get('steeringAngleDeg'),
          'lead_fresh': fresh(sample['services'], 'radarState', sample['mono_ns']),
          'lead': {k: lead.get(k) for k in ('status', 'dRel', 'vRel', 'radar', 'radarTrackId', 'modelProb')},
          'alert': sd.get('alertType')}


def review(folder, day):
  groups = defaultdict(list); headers = defaultdict(list); corrupt = []
  count = size = 0; seen = set()
  for manifest in sorted(folder.glob('*.manifest.json')):
    try:
      meta = json.loads(manifest.read_text(encoding='utf-8'))
      if not ID.fullmatch(meta.get('id', '')) or not HASH.fullmatch(meta.get('sha256', '')) or manifest.name != meta['id'] + '.manifest.json':
        raise ValueError('Invalid chunk identity')
      path = manifest.with_name(meta['id'] + '.jsonl.gz')
      if path.stat().st_size != meta['bytes'] or digest(path) != meta['sha256']: raise ValueError('checksum')
      if (meta['id'], meta['sha256']) in seen: continue
      rows = [json.loads(line) for line in gzip.open(path, 'rt', encoding='utf-8')]
      h = rows[0]
      if h['kind'] != 'header' or h['schema'] != 1: raise ValueError('schema')
      selected = []
      for row in rows[1:]:
        row['_utc'] = h['utc_ns'] + row['mono_ns'] - h['mono_ns']
        if iso(row['_utc'])[:10] == day: selected.append(row)
      if not selected: continue
      count += 1; size += path.stat().st_size; seen.add((meta['id'], meta['sha256']))
      key = (h['boot_id'], h['trip_id'])
      groups[key].extend(selected); headers[key].append(h)
    except (ValueError, KeyError, TypeError, OSError, EOFError) as exc:
      corrupt.append({'file': manifest.name, 'reason': type(exc).__name__})
  summary, evidence, durations, alerts = [], [], Counter(), Counter()
  blinker = Counter(); recovered_events = Counter(); long_accels = []
  for key, rows in sorted(groups.items(), key=lambda item: min(r['_utc'] for r in item[1])):
    rows.sort(key=lambda row: row['mono_ns'])
    samples = [r for r in rows if r['kind'] == 'sample']; times = [r['mono_ns'] for r in samples]
    events = [r for r in rows if r['kind'] == 'event']; recovered_events.update(r['name'] for r in events)
    moving = [s for s in samples if valid(s) and (value(s, 'carState').get('vEgo') or 0) > 2]
    summary.append({'first': iso(min(r['_utc'] for r in rows)), 'last': iso(max(r['_utc'] for r in rows)),
                    'chunks': len(headers[key]), 'samples': len(samples), 'valid_moving_samples': len(moving),
                    'route': next((h.get('route') for h in headers[key] if h.get('route')), None),
                    'settings': headers[key][0].get('settings'),
                    'metadata_missing_in_first_header': not bool(headers[key][0].get('car_fingerprint'))})
    for i, sample in enumerate(samples):
      if not valid(sample): continue
      cs, cc = value(sample, 'carState'), value(sample, 'carControl')
      alert = value(sample, 'selfdriveState').get('alertType')
      if alert: alerts[alert] += 1
      if i + 1 < len(samples):
        gap = (samples[i+1]['mono_ns'] - sample['mono_ns']) / 1e9
        if 0 < gap <= 1 and (cs.get('vEgo') or 0) > 2: durations[sample['mode']] += gap
      if (cs.get('vEgo') or 0) > 2 and (cs.get('leftBlinker') or cs.get('rightBlinker')):
        blinker['samples'] += 1; blinker['lateral_active'] += int(cc.get('latActive') is True)
      if cc.get('longActive') is True and (cs.get('vEgo') or 0) > 2 and isinstance(cs.get('aEgo'), (int, float)) and math.isfinite(cs['aEgo']):
        long_accels.append(cs['aEgo'])
    last_flicker = None
    for event in events:
      if not samples: continue
      t = event['mono_ns']; idx = min(bisect.bisect_left(times, t), len(samples)-1)
      before = samples[max(0, bisect.bisect_left(times, t-2_000_000_000))]
      after = samples[min(len(samples)-1, bisect.bisect_left(times, t+2_000_000_000))]
      name = event['name']; extra = {}
      if name == 'brake':
        recent = samples[max(0, bisect.bisect_left(times, t-2_000_000_000)):idx+1]
        if not any(valid(s) and value(s, 'carControl').get('longActive') is True for s in recent): continue
        name = 'brake_after_recent_longitudinal_control'
      elif name == 'lead_flicker_candidate':
        if last_flicker is not None and t-last_flicker <= 10_000_000_000:
          last_flicker = t; continue
        last_flicker = t
      elif name == 'oem_fault_observation':
        if event.get('direction') != 'rx' or not any(event.get('after') or []): continue
        extra = {k: event.get(k) for k in ('direction', 'bus', 'before', 'after', 'semantic_status')}
      elif name not in ('override', 'hard_deceleration'): continue
      evidence.append({'event': name, 'event_at': iso(event['_utc']), 'can_transition': extra,
                       'before_2s': context(before), 'at': context(samples[idx]), 'after_2s': context(after)})
  return {'date_kst': day, 'verified_chunks': count, 'bytes': size, 'corrupt': corrupt, 'segments': summary,
          'moving_seconds_observed': {k: round(v, 2) for k,v in durations.items()}, 'alert_samples': dict(alerts),
          'blinker_observations': dict(blinker), 'raw_event_counts': dict(recovered_events),
          'op_longitudinal_accel_range': [min(long_accels), max(long_accels)] if long_accels else None,
          'context_review': evidence, 'control_changes_applied': False,
          'limitations': ['Intervals longer than 1 second are excluded from duration.',
                         '5 Hz evidence cannot resolve short CAN timing or prove cluster display behavior.',
                         'Manual intervention is not automatically dissatisfaction or a target for learning.',
                         'Recorded wall clocks and available chunks do not establish complete daily coverage.']}


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--folder', type=Path, required=True); parser.add_argument('--date', required=True)
  parser.add_argument('--out', type=Path, required=True); args = parser.parse_args()
  result = review(args.folder, args.date); args.out.mkdir(parents=True, exist_ok=True)
  atomic_json(args.out/'evidence.json', result)
  print(json.dumps({k: result[k] for k in ('date_kst','verified_chunks','moving_seconds_observed','blinker_observations','op_longitudinal_accel_range')}, ensure_ascii=False))
  for event in result['context_review']:
    if event['event'] == 'override': continue
    print(json.dumps({'event': event['event'], 'at': event['event_at'], 'mode':event['at']['mode'],
                      'speed': event['at']['speed_kph'], 'aEgo':event['at']['aEgo'], 'before_accel':event['before_2s']['requested_accel'],
                      'lead':event['at']['lead'], 'can':event['can_transition']}, ensure_ascii=False))
