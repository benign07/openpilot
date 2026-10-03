"""Compare passive capture and local rlog CAN frames by exact logMonoTime.

Usage on the device:
  python -m selfdrive.carrot.can_diagnostics.compare_rlog --session-dir SESSION --rlog RLOG [RLOG ...] --out comparison.json
"""
from __future__ import annotations

import argparse
from collections import Counter
import gzip
import itertools
import json
from pathlib import Path


def grouped(records):
  previous = -1
  for timestamp, rows in itertools.groupby(records, key=lambda r: r['mono_ns']):
    if timestamp < previous:
      raise ValueError('CAN timestamps are not ordered. Supply rlog segments in chronological order.')
    previous = timestamp
    yield timestamp, Counter((r['bus'], r['address'], r['dlc'], r['data']) for r in rows)


def compare_records(diagnostic, reference):
  d_iter, r_iter = iter(grouped(diagnostic)), iter(grouped(reference))
  d, r = next(d_iter, None), next(r_iter, None)
  result = {'matched_frames': 0, 'diagnostic_only_frames': 0, 'rlog_only_frames': 0,
            'diagnostic_outside_supplied_rlog': 0, 'matched_timestamps': 0,
            'first_shared_mono_ns': None, 'last_shared_mono_ns': None, 'examples': []}
  d_first = d[0] if d else None
  r_first = r[0] if r else None
  d_last = d_first
  while d is not None:
    d_last = d[0]
    while r is not None and r[0] < d[0]:
      if d_first is not None and r[0] >= d_first:
        result['rlog_only_frames'] += sum(r[1].values())
      r = next(r_iter, None)
    if r is None or (r_first is not None and d[0] < r_first):
      result['diagnostic_outside_supplied_rlog'] += sum(d[1].values())
    elif r[0] > d[0]:
      result['diagnostic_only_frames'] += sum(d[1].values())
    else:
      common = d[1] & r[1]
      left, right = d[1] - r[1], r[1] - d[1]
      result['matched_frames'] += sum(common.values())
      result['diagnostic_only_frames'] += sum(left.values())
      result['rlog_only_frames'] += sum(right.values())
      result['matched_timestamps'] += 1
      if result['first_shared_mono_ns'] is None:
        result['first_shared_mono_ns'] = d[0]
      result['last_shared_mono_ns'] = d[0]
      if (left or right) and len(result['examples']) < 20:
        result['examples'].append({'mono_ns': d[0], 'diagnostic_only': list(left.elements())[:5],
                                   'rlog_only': list(right.elements())[:5]})
      r = next(r_iter, None)
    d = next(d_iter, None)
  result['diagnostic_start_mono_ns'] = d_first
  result['diagnostic_end_mono_ns'] = d_last
  result['interpretation'] = ('Exact received packet timestamps are compared; byte differences and capture omissions are separated. '
                              'Use rlogs from the same device boot and all overlapping segments. Boundary packet differences can reflect start/stop timing. '
                              'A missing segment or no time overlap is not evidence of an incorrect CAN definition.')
  return result


def diagnostic_rows(path):
  with gzip.open(path, 'rt', encoding='utf-8') as stream:
    for line in stream:
      yield json.loads(line)


def rlog_rows(paths):
  from openpilot.tools.lib.logreader import LogReader
  for path in paths:
    if not Path(path).is_file():
      raise ValueError(f'Local rlog file not found: {path}')
    for event in LogReader(str(path)):
      if event.which() != 'can' or not event.valid:
        continue
      for frame in event.can:
        data = bytes(frame.dat)
        if 0 <= frame.src <= 7 and 0 < len(data) <= 64:
          yield {'mono_ns': event.logMonoTime, 'bus': frame.src, 'address': frame.address,
                 'dlc': len(data), 'data': data.hex()}


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--session-dir', type=Path, required=True)
  parser.add_argument('--rlog', type=Path, nargs='+', required=True)
  parser.add_argument('--out', type=Path, required=True)
  args = parser.parse_args()
  report = compare_records(diagnostic_rows(args.session_dir / 'raw_can.jsonl.gz'), rlog_rows(args.rlog))
  report['session_id'] = args.session_dir.name
  report['rlog_files'] = [p.name for p in args.rlog]
  with (args.session_dir / 'events.jsonl').open(encoding='utf-8') as stream:
    report['markers'] = [row for line in stream if (row := json.loads(line))['kind'] in ('user_marker', 'automatic_transition')]
  args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
  print(json.dumps({k: v for k, v in report.items() if k not in ('examples', 'markers')}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
  main()
