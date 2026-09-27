"""Download immutable trip chunks and generate evidence reports on a PC.

python tools/can_auto_sync.py --url http://100.98.217.122:7000 --out D:/carrot-records --watch
python tools/can_auto_sync.py --out D:/carrot-records --analyze-only
"""
import argparse
from collections import Counter, defaultdict
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time
from urllib.request import Request, urlopen

ID = re.compile(r'^[a-f0-9]{32}$')
HASH = re.compile(r'^[a-f0-9]{64}$')


def digest(path):
  result = hashlib.sha256()
  with path.open('rb') as stream:
    for data in iter(lambda: stream.read(256 * 1024), b''):
      result.update(data)
  return result.hexdigest()


def atomic_json(path, value):
  temporary = path.with_suffix('.tmp')
  with temporary.open('w', encoding='utf-8') as stream:
    json.dump(value, stream, ensure_ascii=False, indent=2, allow_nan=False)
    stream.flush()
    os.fsync(stream.fileno())
  os.replace(temporary, path)


def download(base, output, row, opener=urlopen):
  ident, size, expected = row.get('id', ''), row.get('bytes'), row.get('sha256', '')
  if not ID.fullmatch(ident) or not HASH.fullmatch(expected) or type(size) is not int or not 0 < size <= 4 * 1024**2:
    raise ValueError('Invalid chunk manifest')
  target = output / (ident + '.jsonl.gz')
  manifest = output / (ident + '.manifest.json')
  if target.exists() and target.stat().st_size == size and digest(target) == expected:
    atomic_json(manifest, row)
    return False
  partial = output / (ident + '.download')
  offset = partial.stat().st_size if partial.exists() else 0
  if offset >= size:
    partial.unlink()
    offset = 0
  headers = {'Range': f'bytes={offset}-'} if offset else {}
  request = Request(base.rstrip('/') + '/api/automatic_drive/chunks/' + ident, headers=headers)
  with opener(request, timeout=15) as response:
    status = response.status
    if status not in (200, 206):
      raise ValueError(f'Unexpected HTTP status {status}')
    if status == 206 and response.headers.get('Content-Range') != f'bytes {offset}-{size - 1}/{size}':
      raise ValueError('Invalid resume range')
    if status == 200:
      offset = 0
    written = offset
    with partial.open('ab' if offset else 'wb') as stream:
      while data := response.read(128 * 1024):
        written += len(data)
        if written > size:
          raise ValueError('Chunk exceeds manifest size')
        stream.write(data)
      stream.flush()
      os.fsync(stream.fileno())
  if partial.stat().st_size != size or digest(partial) != expected:
    partial.unlink()
    raise ValueError('Chunk hash/length verification failed')
  os.replace(partial, target)
  atomic_json(manifest, row)
  return True


def sync(base, output):
  with urlopen(base.rstrip('/') + '/api/automatic_drive/chunks', timeout=15) as response:
    raw = response.read(8 * 1024**2 + 1)
  if len(raw) > 8 * 1024**2:
    raise ValueError('Manifest list too large')
  rows = json.loads(raw)['chunks']
  return sum(download(base, output, row) for row in rows)


def fresh(services, key, now):
  item = services.get(key, {})
  return item.get('valid') is True and 0 <= now - item.get('mono_ns', 0) <= 1_000_000_000


def analyze(output):
  events, modes, histograms = Counter(), Counter(), defaultdict(Counter)
  trips, event_trips = set(), defaultdict(set)
  seen = set()
  coverage = {'verified_chunks': 0, 'corrupt_chunks': 0, 'recovered_chunks': 0, 'invalid_samples': 0}
  for manifest in sorted(output.rglob('*.manifest.json')):
    try:
      meta = json.loads(manifest.read_text(encoding='utf-8'))
      if not ID.fullmatch(meta['id']) or not HASH.fullmatch(meta['sha256']):
        raise ValueError('Invalid manifest')
      path = manifest.parent / (meta['id'] + '.jsonl.gz')
      if path.stat().st_size != meta['bytes'] or digest(path) != meta['sha256']:
        raise ValueError('Hash mismatch')
      identity = (meta['id'], meta['sha256'])
      if identity in seen:
        continue
      # Validate before counting; malformed chunks must not contribute partial statistics.
      with gzip.open(path, 'rt', encoding='utf-8') as stream:
        rows = [json.loads(line) for line in stream]
      if not rows or rows[0].get('kind') != 'header' or rows[0].get('schema') != 1:
        raise ValueError('Unsupported chunk schema')
      header = rows[0]
      seen.add(identity)
      trip = (header.get('boot_id'), header.get('trip_id'))
      trips.add(trip)
      coverage['verified_chunks'] += 1
      coverage['recovered_chunks'] += int('recovered' in meta.get('reason', ''))
      for row in rows[1:]:
        if row.get('kind') == 'event':
          name = row.get('name', 'unknown')
          if name == 'oem_fault_observation':
            after = row.get('after') or []
            if not any(after):
              continue
            name += ':' + row.get('direction', 'unknown')
          events[name] += 1
          event_trips[name].add(trip)
        if row.get('kind') != 'sample':
          continue
        mode, services, now = row.get('mode', 'unknown'), row.get('services', {}), row['mono_ns']
        if mode == 'unknown' or not all(fresh(services, key, now) for key in ('carState', 'carControl', 'selfdriveState')):
          coverage['invalid_samples'] += 1
          continue
        cs = services['carState']['data']
        speed, accel = cs.get('vEgo'), cs.get('aEgo')
        if not cs.get('canValid') or not isinstance(speed, (int, float)) or not math.isfinite(speed) or speed < 2:
          continue
        modes[mode] += 1
        regime = f'{mode}:{int(speed * 3.6 // 20) * 20}-{int(speed * 3.6 // 20) * 20 + 20}kmh'
        if isinstance(accel, (int, float)) and math.isfinite(accel):
          histograms[regime + ':accel_mps2'][f'{math.floor(accel * 4) / 4:.2f}'] += 1
        if fresh(services, 'radarState', now):
          lead = services['radarState']['data'].get('leadOne') or {}
          distance = lead.get('dRel')
          if lead.get('status') and isinstance(distance, (int, float)) and math.isfinite(distance) and 0 < distance < 200:
            histograms[regime + ':gap_seconds'][f'{math.floor(distance / speed * 4) / 4:.2f}'] += 1
    except (ValueError, KeyError, TypeError, OSError, EOFError):
      coverage['corrupt_chunks'] += 1
  candidates = [{'event': name, 'observations': count, 'trips': len(event_trips[name]),
                 'status': 'review_candidate' if len(event_trips[name]) >= 2 else 'collect_more',
                 'parameter_change': None} for name, count in sorted(events.items())]
  result = {'schema': 1, 'trip_count': len(trips), 'coverage': coverage, 'moving_samples_by_control_mode': dict(modes),
            'events': dict(events), 'histograms': {key: dict(value) for key, value in sorted(histograms.items())},
            'review_candidates': candidates, 'control_changes_applied': False,
            'limits': ['Sampled CAN cannot validate counters or prove absence of short events.',
                       'Driving style distributions are observations, not desired settings.',
                       'rlogs must be retrieved separately before device log rotation for full CAN replay.',
                       'Trips separated by telemetry loss/restart can share one physical drive.']}
  atomic_json(output / 'analysis.json', result)
  text = ['# 자동 주행 기록 분석', '', f'기록 구간: {len(trips)} · 검증된 파일: {coverage["verified_chunks"]}',
          f'손상 파일: {coverage["corrupt_chunks"]} · 전원 종료 후 복구 파일: {coverage["recovered_chunks"]}',
          '', '제어값 자동 변경: 없음. 아래 항목은 원인 검토 후보이며 운전자 불만의 확정이 아닙니다.', '']
  text.extend(f'- {row["event"]}: {row["observations"]}회 관측 / {row["trips"]}개 기록 구간 / {row["status"]}' for row in candidates)
  text.extend(['', '속도대·제어 상태별 가속도와 차간 시간 분포는 analysis.json에서 확인할 수 있습니다.',
               '앞차 교체·도로 상황·신호 노후화 등을 rlog와 대조한 뒤 변경 후보를 검증해야 합니다.'])
  (output / 'analysis.md').write_text('\n'.join(text) + '\n', encoding='utf-8')
  return result


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--url', default='http://100.98.217.122:7000')
  parser.add_argument('--out', type=Path, required=True)
  parser.add_argument('--watch', action='store_true')
  parser.add_argument('--analyze-only', action='store_true')
  args = parser.parse_args()
  args.out.mkdir(parents=True, exist_ok=True)
  while True:
    try:
      count = 0 if args.analyze_only else sync(args.url, args.out)
      result = analyze(args.out)
      print(json.dumps({'downloaded': count, **result['coverage']}), flush=True)
    except (OSError, ValueError, KeyError) as exc:
      print(f'Sync incomplete; records preserved: {exc}', flush=True)
      if not args.watch:
        raise
    if not args.watch or args.analyze_only:
      return
    time.sleep(60)


if __name__ == '__main__':
  main()
