"""Synthetic original display scheduling through actual Panda C forwarding.

No hardware I/O. This tests software phase/jitter behavior, not cluster reception.
"""
import argparse
from collections import Counter
import ctypes as C
import json
from pathlib import Path

from selfdrive.selfdrived.tests.test_lx3_cluster_transport import TestLx3ClusterTransport, ENV, NS
from selfdrive.selfdrived.tests.lx3_host_panda_joint import library, Companion


def configure(path):
  lib = library(path)
  ptr = C.POINTER(C.c_uint8)
  lib.lx3_test_packet_tx.argtypes = [C.c_int, C.c_int, C.c_int, ptr]
  lib.lx3_test_packet_tx.restype = C.c_bool
  lib.lx3_test_packet_fwd.argtypes = [C.c_int, C.c_int, C.c_int, ptr, ptr]
  lib.lx3_test_packet_fwd.restype = C.c_int
  lib.lx3_test_display_timeout.argtypes = [C.c_uint32]
  lib.lx3_test_display_timeout.restype = None
  return lib


def run_case(lib, phase, jitter, old_cycle, can_delays=(0,), usb_delays=(0,), missed_ticks=(), timeout_ms=70):
  case = TestLx3ClusterTransport()
  case.setUp()
  lib.lx3_test_reset()
  lib.lx3_test_display_timeout(timeout_ms * 1000)
  parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
  parser._add_message('ADRV_0x161')
  assert 0x162 in parser.addresses  # Required health is registered at startup.
  case.cs.cp_cam = parser
  case.cs.adrv_0x161 = parser.vl['ADRV_0x161']
  case.cs.ccnc_0x162 = parser.vl['CCNC_0x162']
  source = dict(case.cs.adrv_0x161, COUNTER=0, LFA_ICON=1)
  next_original, publication = phase, 0
  counts = Counter()
  seen = {}
  last_tx_ms = None
  can_pending, usb_pending = [], []
  source_ms = {}
  request_count = 0
  ideal_delivery = can_delays == (0,) and usb_delays == (0,) and not missed_ticks
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
    if original and missed_ticks and min(missed_ticks) <= ms <= max(missed_ticks):
      counts['originals_during_missed_host_ticks'] += 1
    if not original:
      assert data[2] in source_ms
      counts['max_source_to_usb_age_ms'] = max(counts['max_source_to_usb_age_ms'], ms - source_ms[data[2]])
      if ms >= 1800: counts['usb_display_arrivals_after_host_off'] += 1
  def deliver_usb(ms):
    due = [p for p in usb_pending if p[0] <= ms]
    usb_pending[:] = [p for p in usb_pending if p[0] > ms]
    nonlocal last_tx_ms
    for _, addr, data, bus in due:
      raw = (C.c_uint8 * len(data)).from_buffer_copy(data)
      state = Companion()
      lib.lx3_test_state(C.byref(state))
      allowed = lib.lx3_test_packet_tx(addr, bus, len(data), raw)
      assert allowed == bool(state.allowed and state.phase == 2)
      if not allowed:
        counts['delayed_display_rejected_after_native_off'] += 1
        continue
      if addr == 0x161:
        counts['actual_c_display_tx'] += 1
        last_tx_ms = ms
        observe(ms, data, False)
  for ms in range(2101):
    lib.lx3_test_time(1_000_000 + ms * 1000)
    if ms % 10 == 0:
      lib.lx3_test_sensors(1_000_000 + ms * 1000, False, False)
    if ms % 40 == 0:
      data = (C.c_uint8 * 16)()
      assert lib.lx3_test_button(1_000_000 + ms * 1000, 128 if ms == 120 else 0,
                               (250 + 2 * (ms // 40)) % 256, data)
    if ms == 180:
      state = Companion()
      lib.lx3_test_state(C.byref(state))
      assert state.phase == 1 and state.requested == 1 and not state.allowed
      lib.lx3_test_heartbeat(True, state.requested, state.generation, state.counter)
      lib.lx3_test_state(C.byref(state))
      assert state.allowed and state.phase == 2
      counts['actual_button_ack_grants'] += 1
    if ms == 1800:
      lib.lx3_test_heartbeat(False, 0, 0, 0)
    deliver_usb(ms)
    if ms == next_original:
      source['COUNTER'] = publication % 256
      source['LANELINE_CURVATURE'] = publication % 16
      addr, data, _ = case.env['_make_ccnc_cluster_msg'](case.packer, 'ADRV_0x161', 2, source, True, publication % 256)
      source_ms[data[2]] = ms
      raw = (C.c_uint8 * len(data)).from_buffer_copy(data)
      output = (C.c_uint8 * len(data))()
      destination = lib.lx3_test_packet_fwd(addr, 2, len(data), raw, output)
      if destination == 0:
        assert bytes(output) == data
        observe(ms, data, True)
      else:
        assert destination == -1
        if last_tx_ms is not None:
          assert ms - last_tx_ms < timeout_ms, (ms, last_tx_ms)
        counts['originals_blocked_by_actual_c'] += 1
      can_pending.append((ms + can_delays[publication % len(can_delays)], addr, data))
      next_original += 50 + jitter[publication % len(jitter)]
      publication += 1
    due_can = [p for p in can_pending if p[0] <= ms]
    can_pending[:] = [p for p in can_pending if p[0] > ms]
    if due_can:
      parser.update([[1_000_000_000 + ms * 1_000_000, [(addr, data, 2) for _, addr, data in due_can]]])
    if ms % 10 == 0 and ms not in missed_ticks:
      case.clock = 1_000_000_000 + ms * 1_000_000
      case.cc.latActive = case.cs.out.latEnabled = 200 <= ms < 1800
      case.cluster.begin(case.cs, case.clock, case.cc.latActive)
      if old_cycle and ms % 50:
        continue
      for addr, data, bus in case.ccnc(ms // 10):
        if addr == 0x161:
          usb_pending.append((ms + usb_delays[request_count % len(usb_delays)], addr, data, bus))
          request_count += 1
        else:
          usb_pending.append((ms, addr, data, bus))
      deliver_usb(ms)
  assert counts['inactive_originals'] > 0 and counts['disengaged_originals'] > 0
  assert counts['usb_display_arrivals_after_host_off'] == 0
  if not old_cycle and ideal_delivery:
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
  def accumulate(total, counts):
    for key, value in counts.items():
      if key.startswith('max_'): total[key] = max(total[key], value)
      else: total[key] += value
  schedules = 0
  for phase in range(10):
    for jitter in ((0,), (-3, 3), (3, -3), (-3, -2, -1, 0, 1, 2, 3)):
      accumulate(new, run_case(lib, phase, jitter, False))
      accumulate(old, run_case(lib, phase, jitter, True))
      schedules += 1
  assert old['original_leaks_after_engage_settle'] > 0
  # Keep the original ideal-path invariants above. The delayed cases measure
  # known display ownership limits; they do not assert an invented guarantee
  # or enlarge firmware's existing70ms display-only timeout.
  delayed, experimental = [], []
  profiles = [((can,), (usb,), ()) for can in (0, 10, 30) for usb in (0, 10, 30)
              if can or usb]
  profiles.extend((((0, 30), (0,), ()), ((0,), (0, 30), ()),
                   ((0, 30), (30, 0), ()),
                   ((0, 30), (30, 0), (320, 720, 1120, 1520)),
                   ((0,), (0,), tuple(range(700, 1000, 10)))))
  for phase in range(10):
    for jitter in ((0,), (-3, 3), (3, -3), (-3, -2, -1, 0, 1, 2, 3)):
      for can_delay, usb_delay, missed in profiles:
        result = run_case(lib, phase, jitter, False, can_delay, usb_delay, missed)
        delayed.append({'phase': phase, 'jitter': jitter, 'can_delays_ms': can_delay,
                        'usb_delays_ms': usb_delay, 'missed_host_ticks_ms': missed, 'counts': dict(result)})
        # Evaluate Claude's proposed110ms value using the same actual C and
        # fixed profiles. This changes only a desktop fixture, never firmware.
        sensitivity = run_case(lib, phase, jitter, False, can_delay, usb_delay, missed, 110)
        experimental.append({'phase': phase, 'jitter': jitter, 'can_delays_ms': can_delay,
                             'usb_delays_ms': usb_delay, 'missed_host_ticks_ms': missed,
                             'fixture_timeout_ms': 110, 'counts': dict(sensitivity)})
        if len(missed) == 30:
          assert result['originals_during_missed_host_ticks'] > 0
          assert sensitivity['originals_during_missed_host_ticks'] > 0
  report = {'scope': __doc__, 'schedules_each': schedules, 'new_per_publication': dict(new),
            'previous_50ms_claim': dict(old), 'startup_duplicate_limit_per_schedule': 1,
            'delayed_schedules': len(delayed), 'delayed_results': delayed,
            'experimental_110ms_results': experimental,
            'qualification': 'synthetic_parser_and_actual_C_forwarding_not_vehicle_cluster'}
  Path(options.report).write_text(json.dumps(report, indent=2), encoding='utf-8')
  delayed_totals = Counter()
  for result in delayed: accumulate(delayed_totals, result['counts'])
  print('PASS actual Panda display forwarding phase/jitter:', json.dumps({
    k: v for k, v in report.items() if k not in ('delayed_results', 'experimental_110ms_results')}))
  print('MEASURE delayed display schedules (not guarantee):', dict(delayed_totals))
  sensitivity_totals = Counter()
  for result in experimental: accumulate(sensitivity_totals, result['counts'])
  print('EXPERIMENT desktop110ms only (production unchanged):', dict(sensitivity_totals))


if __name__ == '__main__':
  main()
