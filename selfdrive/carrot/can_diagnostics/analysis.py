"""Bounded bit-level comparison; results are candidates, never verified mappings."""
from __future__ import annotations

from array import array
from collections import defaultdict
import hashlib
from pathlib import Path
import re

# Historical LX3 HEV captures contain 633 distinct bus/address/length keys.
MAX_MESSAGE_KEYS = 1024


def signal_bits(start: int, size: int, little: bool) -> list[int]:
  """Physical bit offsets, byte*8 + LSB0, for Intel or Motorola DBC signals."""
  if little:
    return list(range(start, start + size))
  bits = []
  for _ in range(size):
    bits.append(start)
    start = start + 15 if start % 8 == 0 else start - 1
  return bits


class DbcIndex:
  def __init__(self, paths=()):
    self.messages = defaultdict(list)
    self.sources = []
    self.bus_bindings = {}
    self.checksum_functions = {}
    self.runtime_info = {'status': 'dbc_files_only', 'bus_binding_verified': False}
    for path in paths:
      path = Path(path)
      if not path.is_file():
        continue
      raw = path.read_bytes()
      self.sources.append({'name': path.name, 'sha256': hashlib.sha256(raw).hexdigest()})
      message = None
      for line in raw.decode('utf-8', 'replace').splitlines():
        match = re.match(r'BO_\s+(\d+)\s+(\w+)\s*:\s*(\d+)', line.strip())
        if match:
          address, name, length = match.groups()
          message = {'name': name, 'dlc': int(length), 'dbc': path.name, 'signals': []}
          self.messages[int(address)].append(message)
        # Multiplexed signals need a multiplexor value: deliberately do not label them.
        match = re.match(r'SG_\s+(\w+)\s*:\s*(\d+)\|(\d+)@([01])([+-])\s+\(([^,]+),([^\)]+)\)\s+\[([^|]+)\|([^\]]+)\]\s+"([^"]*)"', line.strip())
        if match and message:
          name, start, size, endian, signed, factor, offset, lower, upper, unit = match.groups()
          bits = signal_bits(int(start), int(size), endian == '1')
          if not bits or min(bits) < 0 or max(bits) >= message['dlc'] * 8:
            continue
          message['signals'].append({'name': name, 'start_bit': int(start), 'size': int(size),
                                     'byte_order': 'little' if endian == '1' else 'big',
                                     'signed': signed == '-', 'bits': bits,
                                     'factor': float(factor), 'offset': float(offset),
                                     'minimum': float(lower), 'maximum': float(upper), 'unit': unit,
                                     'counter_configured': False, 'checksum_configured': False})

  def lookup(self, address, dlc, bit):
    annotations = []
    for message in self.messages.get(address, []):
      if message['dlc'] != dlc:
        continue
      for signal in message['signals']:
        if bit in signal['bits']:
          annotations.append({k: v for k, v in signal.items() if k != 'bits'} |
                             {'message': message['name'], 'dbc': message['dbc'],
                              'bus_binding_verified': False})
    return annotations


class Window:
  def __init__(self, cycle, phase):
    self.cycle, self.phase = cycle, phase
    self.samples = {}

  def add(self, frames):
    for key, payload in frames.items():
      if key not in self.samples:
        if len(self.samples) >= MAX_MESSAGE_KEYS:
          raise RuntimeError('CAN 주소 종류가 수집 한도를 초과했습니다.')
        self.samples[key] = [0, array('I', [0]) * (len(payload) * 8)]
      row = self.samples[key]
      row[0] += 1
      for byte_index, byte in enumerate(payload):
        while byte:
          lowest = byte & -byte
          row[1][byte_index * 8 + lowest.bit_length() - 1] += 1
          byte ^= lowest


def rank_candidates(windows, dbc=None, cycles=3):
  dbc = dbc or DbcIndex()
  by_phase = {(w.cycle, w.phase): w for w in windows}
  groups = []
  for cycle in range(cycles):
    if not all((cycle, p) in by_phase for p in ('baseline', 'active', 'release')):
      return []
    groups.append(tuple(by_phase[cycle, p] for p in ('baseline', 'active', 'release')))
  keys = set.intersection(*(set(w.samples) for group in groups for w in group))
  candidates = []
  for key in sorted(keys):
    bus, address, dlc = key
    rows = [tuple(w.samples[key] for w in group) for group in groups]
    if any(row[0] < 8 for group in rows for row in group):
      continue
    for bit in range(dlc * 8):
      proportions = [[row[1][bit] / row[0] for row in group] for group in rows]
      deltas = [a - b for b, a, r in proportions]
      direction = 1 if sum(deltas) >= 0 else -1
      repeats = sum(d * direction >= .25 for d in deltas)
      recovery_error = max(abs(r - b) for b, a, r in proportions)
      # Requiring all three active transitions AND returns suppresses counters,
      # checksums, incidental changes, missing frames and one-off coincidences.
      if repeats != cycles or recovery_error > .22:
        continue
      annotations = dbc.lookup(address, dlc, bit)
      if any(re.search(r'CHECKSUM|COUNTER|CRC|ALIVE', a['name'], re.I) for a in annotations):
        continue
      score = sum(abs(d) for d in deltas) / cycles * (1 - recovery_error)
      candidates.append({'bus': bus, 'address': address, 'address_hex': f'0x{address:X}',
                         'dlc': dlc, 'bit_lsb0': bit, 'byte_index': bit // 8, 'bit_in_byte': bit % 8,
                         'score': round(score, 4), 'repeat_count': repeats,
                         'polarity': 'rises' if direction > 0 else 'falls',
                         'phase_proportions': proportions, 'dbc_annotations': annotations,
                         'mapping_status': 'candidate_unverified'})
  return sorted(candidates, key=lambda c: (-c['score'], c['bus'], c['address'], c['bit_lsb0']))[:80]
