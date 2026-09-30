"""Synthetic original display scheduling through actual Panda C forwarding.

No hardware I/O. This tests software phase/jitter behavior, not cluster reception.
"""
import argparse
from collections import Counter
import ctypes as C
import json
from pathlib import Path

from selfdrive.selfdrived.tests.test_lx3_cluster_transport import TestLx3ClusterTransport, ENV, NS
from selfdrive.selfdrived.tests.lx3_host_panda_joint import library


def configure(path):
  lib = library(path)
  ptr = C.POINTER(C.c_uint8)
  lib.lx3_test_packet_tx.argtypes = [C.c_int, C.c_int, C.c_int, ptr]
  lib.lx3_test_packet_tx.restype = C.c_bool
  lib.lx3_test_packet_fwd.argtypes = [C.c_int, C.c_int, C.c_int, ptr, ptr]
  lib.lx3_test_packet_fwd.restype = C.c_int
  return lib


def run_case(lib, phase, jitter, old_cycle):
  case = TestLx3ClusterTransport()
  case.setUp()
  lib.lx3_test_reset()
  parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
  parser._add_message('ADRV_0x161')
  parser._add_message('CCNC_0x162')
  case.cs.cp_cam = parser
  case.cs.adrv_0x161 = parser.vl['ADRV_0x161']
  case.cs.ccnc_0x162 = parser.vl['CCNC_0x162']
  source = dict(case.cs.adrv_0x161, COUNTER=0, LFA_ICON=1)
  next_original, publication = phase, 0
  counts = Counter()
  seen = {}
  last_tx_ms = None
  def observe(ms, data, original):
    previous = seen.get(data[2])
    if previous is not None and previous[1] != data and ms - previous[0] < 100:
      counts['different_payload_same_counter'] += 1
    seen[data[2]] = ms, data
    if original and 300 <= ms < 1800:
      counts['original_leaks_after_engage_settle'] += 1
    if original and ms < 200:
      counts['inactive_originals'] += 1
    if original and ms >= 1800:
      counts['disengaged_originals'] += 1
  for ms in range(2101):
    lib.lx3_test_time(1_000_000 + ms * 1000)
    if ms == next_original:
      source['COUNTER'] = publication % 256
      source['LANELINE_CURVATURE'] = publication % 16
      addr, data, _ = case.env['_make_ccnc_cluster_msg'](case.packer, 'ADRV_0x161', 2, source, True, publication % 256)
      raw = (C.c_uint8 * len(data)).from_buffer_copy(data)
      output = (C.c_uint8 * len(data))()
      destination = lib.lx3_test_packet_fwd(addr, 2, len(data), raw, output)
      if destination == 0:
        assert bytes(output) == data
        observe(ms, data, True)
      else:
        assert destination == -1
        if last_tx_ms is not None:
          assert ms - last_tx_ms < 70, (ms, last_tx_ms)
        counts['originals_blocked_by_actual_c'] += 1
      parser.update([[1_000_000_000 + ms * 1_000_000, [(addr, data, 2)]]])
      next_original += 50 + jitter[publication % len(jitter)]
      publication += 1
    if ms % 10 == 0:
      case.clock = 1_000_000_000 + ms * 1_000_000
      case.cc.latActive = case.cs.out.latEnabled = 200 <= ms < 1800
      case.cluster.begin(case.cs, case.clock, case.cc.latActive)
      if old_cycle and ms % 50:
        continue
      for addr, data, bus in case.ccnc(ms // 10):
        raw = (C.c_uint8 * len(data)).from_buffer_copy(data)
        assert lib.lx3_test_packet_tx(addr, bus, len(data), raw)
        if addr == 0x161:
          counts['actual_c_display_tx'] += 1
          last_tx_ms = ms
          observe(ms, data, False)
  assert counts['inactive_originals'] > 0 and counts['disengaged_originals'] > 0
  if not old_cycle:
    assert counts['original_leaks_after_engage_settle'] == 0, (phase, jitter, counts)
    assert counts['different_payload_same_counter'] <= 1, (phase, jitter, counts)
  return counts


def main():
  args = argparse.ArgumentParser()
  args.add_argument('--library', required=True)
  args.add_argument('--report', required=True)
  options = args.parse_args()
  lib = configure(options.library)
  new, old = Counter(), Counter()
  schedules = 0
  for phase in range(10):
    for jitter in ((0,), (-3, 3), (3, -3), (-3, -2, -1, 0, 1, 2, 3)):
      new.update(run_case(lib, phase, jitter, False))
      old.update(run_case(lib, phase, jitter, True))
      schedules += 1
  assert old['original_leaks_after_engage_settle'] > 0
  report = {'scope': __doc__, 'schedules_each': schedules, 'new_per_publication': dict(new),
            'previous_50ms_claim': dict(old), 'startup_duplicate_limit_per_schedule': 1,
            'qualification': 'synthetic_parser_and_actual_C_forwarding_not_vehicle_cluster'}
  Path(options.report).write_text(json.dumps(report, indent=2), encoding='utf-8')
  print('PASS actual Panda display forwarding phase/jitter:', json.dumps(report))


if __name__ == '__main__':
  main()
