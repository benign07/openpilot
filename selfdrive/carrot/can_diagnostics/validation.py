"""Observe real driving against installed DBC definitions, with explicit evidence limits."""
from __future__ import annotations

import re

from .analysis import MAX_MESSAGE_KEYS


def decode_signal(payload, signal):
  if signal['byte_order'] == 'little':
    raw = (int.from_bytes(payload, 'little') >> signal['start_bit']) & ((1 << signal['size']) - 1)
  else:
    raw = 0
    for bit in signal['bits']:
      raw = (raw << 1) | ((payload[bit // 8] >> (bit % 8)) & 1)
  signed = raw
  if signal['signed'] and raw & (1 << (signal['size'] - 1)):
    signed -= 1 << signal['size']
  return raw, signed * signal['factor'] + signal['offset']


class DrivingValidator:
  def __init__(self, dbc):
    self.dbc = dbc
    self.rows = {}
    self.previous_counters = {}
    self.plans = {}
    self.public_definitions = {}
    self.known_definitions = []
    for address, definitions in dbc.messages.items():
      for definition in definitions:
        public = {k: v for k, v in definition.items() if k != 'signals'}
        public['signals'] = []
        for signal in definition['signals']:
          item = {k: v for k, v in signal.items() if k != 'bits'}
          if re.search(r'CHECKSUM|CRC', signal['name'], re.I) and not signal['checksum_configured']:
            item['validation_note'] = '현재 DBC 파서에 체크섬 검증 함수가 연결되어 있지 않음'
          public['signals'].append(item)
        self.public_definitions[id(definition)] = public
        self.known_definitions.append(dict(public, address=address))

  def consume(self, bus, address, payload, mono_ns):
    key = (bus, address, len(payload))
    if key not in self.rows:
      if len(self.rows) >= MAX_MESSAGE_KEYS:
        raise RuntimeError('관측 주소 한도를 초과했습니다.')
      definitions = self.dbc.messages.get(address, [])
      binding = self.dbc.bus_bindings.get(bus)
      scoped = [m for m in definitions if binding is None or m['dbc'] in binding]
      exact = [m for m in scoped if m['dlc'] == len(payload)]
      self.rows[key] = {'bus': bus, 'address': address, 'address_hex': f'0x{address:X}',
                        'dlc': len(payload), 'frame_count': 0, 'first_mono_ns': mono_ns,
                        'last_mono_ns': mono_ns, 'max_gap_ms': 0.0, 'length_mismatch': bool(scoped) and not exact,
                        'expected_lengths': sorted({m['dlc'] for m in scoped}),
                        'definition_status': 'unknown_address' if not scoped else ('ambiguous' if len(exact) > 1 else 'defined'),
                        'bus_scope': 'runtime_configured' if binding is not None else 'unverified',
                        'definitions': exact, 'signal_observations': {},
                        'checksum_checks': 0, 'checksum_failures': 0, 'counter_discontinuities': 0,
                        'last_decode_ns': 0, 'mapping_status': 'definition_only'}
      if len(exact) == 1:
        plan = [(signal, self.dbc.checksum_functions.get((exact[0]['dbc'], address, signal['name'])))
                for signal in exact[0]['signals']]
        self.plans[key] = (plan, [(signal, check) for signal, check in plan if signal['counter_configured'] or check is not None])
    row = self.rows[key]
    row['max_gap_ms'] = max(row['max_gap_ms'], (mono_ns - row['last_mono_ns']) / 1e6)
    row['last_mono_ns'] = mono_ns
    row['frame_count'] += 1
    definitions = row['definitions']
    if len(definitions) != 1:
      return
    sampled = mono_ns - row['last_decode_ns'] >= 100_000_000
    for signal, check in self.plans[key][0 if sampled else 1]:
      raw, value = decode_signal(payload, signal)
      if check is not None:
        row['checksum_checks'] += 1
        if raw != check(payload):
          row['checksum_failures'] += 1
      if signal['counter_configured']:
        counter_key = (key, signal['name'])
        previous = self.previous_counters.get(counter_key)
        if previous is not None and raw != (previous + 1) % (1 << signal['size']):
          row['counter_discontinuities'] += 1
        self.previous_counters[counter_key] = raw
      if sampled:
        stats = row['signal_observations'].get(signal['name'])
        if stats is None:
          stats = {'samples': 0, 'minimum_observed': value, 'maximum_observed': value,
                   'last_value': value, 'out_of_declared_range': 0, 'semantic_status': 'unverified'}
          row['signal_observations'][signal['name']] = stats
        stats['samples'] += 1
        stats['minimum_observed'] = min(stats['minimum_observed'], value)
        stats['maximum_observed'] = max(stats['maximum_observed'], value)
        stats['last_value'] = value
        if signal['maximum'] > signal['minimum'] and not signal['minimum'] <= value <= signal['maximum']:
          stats['out_of_declared_range'] += 1
    if sampled:
      row['last_decode_ns'] = mono_ns
    row['mapping_status'] = 'needs_review' if row['checksum_failures'] or row['counter_discontinuities'] else 'observed_shape'

  def summary(self):
    values = list(self.rows.values())
    return {'observed_messages': len(values), 'unknown_messages': sum(r['definition_status'] == 'unknown_address' for r in values),
            'length_mismatches': sum(r['length_mismatch'] for r in values),
            'checksum_checks': sum(r['checksum_checks'] for r in values),
            'checksum_failures': sum(r['checksum_failures'] for r in values),
            'counter_discontinuities': sum(r['counter_discontinuities'] for r in values),
            'checksum_unconfigured_messages': sum(any(re.search(r'CHECKSUM|CRC', s['name'], re.I) and not s['checksum_configured']
                    for d in r['definitions'] for s in d['signals']) for r in values),
            'known_dbc_definitions': sum(len(v) for v in self.dbc.messages.values()),
            'dbc_sources': self.dbc.sources, 'runtime_database': self.dbc.runtime_info}

  def catalog(self):
    # Copy only changing statistics while the caller holds the capture lock.
    # DBC definitions are immutable during a session and already JSON-ready.
    rows = [dict(row, definitions=[self.public_definitions[id(d)] for d in row['definitions']],
                 signal_observations={name: dict(stats) for name, stats in row['signal_observations'].items()})
            for row in self.rows.values()]
    latest = max((r['last_mono_ns'] for r in rows), default=0)
    for row in rows:
      duration = (row['last_mono_ns'] - row['first_mono_ns']) / 1e9
      row['observed_hz'] = round((row['frame_count'] - 1) / duration, 2) if duration > 0 else None
      row['max_gap_ms'] = round(row['max_gap_ms'], 2)
      row['silence_at_end_ms'] = round((latest - row['last_mono_ns']) / 1e6, 2)
      row.pop('last_decode_ns', None)
    return {'schema_version': 1, 'summary': self.summary(), 'known_definitions': self.known_definitions,
            'messages': sorted(rows, key=lambda r: (r['bus'], r['address'], r['dlc'])),
            'evidence_policy': 'DBC 이름이나 상관관계만으로 실제 의미가 검증되지는 않습니다. 미정의 주소는 곧바로 오류가 아닙니다. 카운터 불연속은 수집 누락과 함께 검토해야 합니다.',
            'timebase': 'Device monotonic nanoseconds, same domain as rlog logMonoTime',
            'signal_sample_rate_hz': 10,
            'signal_sample_note': 'Raw CAN is recorded per received frame; value min/max are sampled at up to 10 Hz and can miss brief excursions.'}
