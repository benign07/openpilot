"""Actual C/production host deterministic scheduling with synthetic RX fixtures.

No vehicle, socket, USB or actuator device is opened. C validates the configured
sensor RX stream and physical CRC/counter/gestures; Python executes production
intent and SelfdriveD/StateMachine bodies via the existing message facade. This
does not measure hardware timing, camera ownership or EPS response.
"""
import argparse
from collections import Counter, deque
import contextlib
import ctypes as C
import io
import itertools
import json
from pathlib import Path
from types import SimpleNamespace as NS

from selfdrive.selfdrived.tests import test_lx3_engagement as host
from selfdrive.selfdrived.tests import test_lx3_inputs as physical


class Companion(C.LittleEndianStructure):
  _pack_ = 1
  _fields_ = [('version', C.c_uint8), ('requested', C.c_uint8), ('accepted', C.c_uint8),
              ('counter', C.c_uint8), ('generation', C.c_uint16), ('age_ms', C.c_uint16),
              ('allowed', C.c_uint8), ('phase', C.c_uint8), ('reserved', C.c_uint16), ('epoch', C.c_uint64)]


def library(path):
  lib = C.CDLL(str(Path(path).resolve()))
  declarations = {
    'reset': ([], None), 'sensors': ([C.c_uint32, C.c_bool, C.c_bool], None),
    'button': ([C.c_uint32, C.c_uint8, C.c_uint8, C.POINTER(C.c_uint8)], C.c_bool),
    'time': ([C.c_uint32], None), 'tick': ([], None), 'rx_valid': ([], C.c_bool),
    'state': ([C.POINTER(Companion)], None),
    'heartbeat': ([C.c_bool, C.c_uint8, C.c_uint16, C.c_uint8], None),
    'active_tx': ([C.c_uint8], C.c_bool),
    'packet_tx': ([C.c_int, C.c_int, C.c_int, C.POINTER(C.c_uint8)], C.c_bool),
    'packet_fwd': ([C.c_int, C.c_int, C.c_int, C.POINTER(C.c_uint8), C.POINTER(C.c_uint8)], C.c_int),
  }
  for name, (args, result) in declarations.items():
    fn = getattr(lib, 'lx3_test_' + name)
    fn.argtypes, fn.restype = args, result
  return lib


def run_case(lib, spec):
  delay, batch, offset, order, scenario = spec
  lib.lx3_test_reset()
  case = host.TestLx3Session()
  case.setUp()
  case.refresh_panda = False
  case.panda.lx3PermissionVersion = 0
  intent = physical.Intent()
  parser = NS(raw_frames=deque(), raw_overflow=False, _last_update_nanos=0)
  deliveries = []
  buttons = {200: 128}
  expected = 1
  if scenario == 'combined':
    buttons = {200: 8}
    expected = 2
  if scenario == 'upgrade':
    buttons = {200: 128, 600: 8}
    expected = 2
  if scenario == 'cancel': buttons[640] = 4
  if scenario == 'quick_denied_retry': buttons.update({360: 128})
  if scenario == 'denied_then_retry': buttons.update({520: 128})
  if scenario == 'rapid_toggle': buttons.update({280: 128})
  if scenario == 'no_entry_before_ack': pass
  if scenario == 'no_entry_after_ack': pass
  if scenario in ('cancel', 'brake', 'rapid_toggle', 'no_entry_before_ack', 'no_entry_after_ack', 'angle_delivery_revoke'):
    expected = 0
  if scenario == 'quick_denied_retry':
    # Golden delivery schedule: refusal reaches C before the second release
    # at400ms for offset90, or when host runs between read and HB at offset0/30.
    # Otherwise that physical release cancels the original outstanding pending.
    expected = 1 if offset == 90 or order == 'between' else 0
  counters = Counter()
  update_calls = 0
  original_update = case.ctx.state_machine.update
  def counted_update(events):
    nonlocal update_calls
    update_calls += 1
    return original_update(events)
  case.ctx.state_machine.update = counted_update
  trace = []
  previous = None
  last_off_host = None
  last_heartbeat = None
  first_grant = None
  delivery_revoke_ms = None
  host_delivery_revoke_ms = None
  def snapshot():
    value = Companion()
    lib.lx3_test_state(C.byref(value))
    assert value.version == 2 and value.reserved == 0 and value.epoch != 0
    assert not (value.phase == 1 and value.allowed)
    return value
  def publish(ms):
    value = snapshot()
    p = case.panda
    p.lx3PermissionVersion = value.version
    p.lx3RequestedMode = value.requested
    p.lx3AcceptedMode = value.accepted
    p.lx3PhysicalCounter = value.counter
    p.lx3RequestGeneration = value.generation
    p.lx3RequestAgeMs = value.age_ms
    p.lx3ControlsAllowed = p.controlsAllowed = bool(value.allowed)
    p.lx3PermissionPhase = value.phase
    p.lx3TransportEpoch = value.epoch
    p.safetyRxChecksInvalid = not lib.lx3_test_rx_valid()
    case.ctx.sm.logMonoTime['pandaStates'] = 1_000_000_000 + ms * 1_000_000
  def step_host(ms):
    nonlocal last_off_host, host_delivery_revoke_ms
    parser._last_update_nanos = 1_000_000_000 + ms * 1_000_000
    due = [x for x in deliveries if x[0] <= ms]
    deliveries[:] = [x for x in deliveries if x[0] > ms]
    for delivered, data in due:
      parser.raw_frames.append((0x10B, 0, data, 1_000_000_000 + delivered * 1_000_000))
    events, ready = intent.from_parser(parser, physical.checksum, with_counter=True)
    case.cs.steerFaultTemporary = not ready
    case.now = 1 + ms / 1000
    host.ENV['time'] = NS(monotonic_ns=lambda: 1_000_000_000 + ms * 1_000_000)
    barriers = ()
    if scenario in ('quick_denied_retry', 'denied_then_retry', 'no_entry_before_ack') and 200 <= ms <= 310:
      barriers = ('tooDistracted',)
    if scenario == 'no_entry_after_ack' and first_grant is not None and ms >= first_grant + 5:
      barriers = ('tooDistracted',)
    if scenario == 'brake' and ms >= 640:
      barriers = ('pedalPressed',)
    before_calls = update_calls
    case.step(*(host.button(name, pressed, counter=counter) for name, pressed, counter in events), events=barriers)
    assert update_calls == before_calls + 1, (spec, ms, before_calls, update_calls)
    counters['single_state_machine_updates'] += 1
    session = case.ctx.lx3_engagement
    if case.ctx.active:
      assert case.ctx.enabled and session.mode in (1, 2) and session.pending is None
      assert not session.ack_valid
      assert case.panda.lx3PermissionPhase == 2 and case.panda.lx3ControlsAllowed
      assert session.accepted_generation == case.panda.lx3RequestGeneration
      for mode in (() if scenario == 'angle_delivery_revoke' else ((1, 2) if session.mode == 2 else (1,))):
        state = snapshot()
        allowed = lib.lx3_test_active_tx(mode)
        assert not (state.phase == 1 and allowed)
        counters['host_active_tx_allowed' if allowed else 'stale_host_tx_blocked'] += 1
    if delivery_revoke_ms is not None and not case.ctx.enabled and host_delivery_revoke_ms is None:
      host_delivery_revoke_ms = ms
      counters['host_disable_after_delivery_revoke'] += 1
    if case.ctx.enabled and not case.ctx.active:
      counters['host_pre_enabled_frames'] += 1
    if barriers and not case.ctx.enabled:
      last_off_host = ms
  def heartbeat(ms):
    nonlocal last_heartbeat, first_grant
    session = case.ctx.lx3_engagement
    ack = session.ack_valid
    lib.lx3_test_heartbeat(case.ctx.enabled, int(session.ack_mode) if ack else 0,
                           session.ack_generation if ack else 0, session.ack_counter if ack else 0)
    state = snapshot()
    if state.allowed and first_grant is None:
      first_grant = ms
    if not case.ctx.enabled:
      assert not state.allowed
    last_heartbeat = ms
    counters['heartbeats'] += 1
  with contextlib.redirect_stdout(io.StringIO()):
    for ms in range(0, 1801):
      us = 1_000_000 + ms * 1000
      lib.lx3_test_time(us)
      if ms % 10 == 0:
        lib.lx3_test_sensors(us, scenario == 'brake' and ms >= 640, False)
      if ms % 40 == 0:
        data = (C.c_uint8 * 16)()
        counter = (250 + 2 * (ms // 40)) % 256
        assert lib.lx3_test_button(us, buttons.get(ms, 0), counter, data)
        delivered = ms + delay
        if batch: delivered = ((delivered + batch - 1) // batch) * batch
        deliveries.append((delivered, bytes(data)))
      if ms % 1000 == 0:
        lib.lx3_test_tick()
      if scenario == 'angle_delivery_revoke':
        if 800 <= ms <= 840 and ms % 10 == 0:
          assert snapshot().allowed and case.ctx.active
          # Native permission came from the actual button/ACK path above.
          # Fresh synthetic MDPS remains zero; valid USB goals ramp by2deg
          # but no original CB slot is provided until the backlog has grown.
          goal = bytearray(24)
          goal[3], goal[6] = 0x20, 25
          goal[4:6] = (20 * (1 + (ms - 800) // 10)).to_bytes(2, 'little')
          raw = (C.c_uint8 * 24).from_buffer_copy(goal)
          assert lib.lx3_test_packet_tx(0xCB, 0, 24, raw)
          counters['usb_ramp_goals_accepted'] += 1
        if ms == 850:
          original = bytearray(24)
          original[:2] = physical.checksum(0xCB, None, original).to_bytes(2, 'little')
          raw = (C.c_uint8 * 24).from_buffer_copy(original)
          output = (C.c_uint8 * 24)()
          assert lib.lx3_test_packet_fwd(0xCB, 2, 24, raw, output) == 0
          state = snapshot()
          assert not state.allowed and state.phase == 0 and state.accepted == 0
          assert ((output[3] >> 4) & 3) != 2
          delivery_revoke_ms = ms
          counters['actual_c_delivery_revokes'] += 1
        if delivery_revoke_ms is not None and ms % 10 == 0:
          assert not lib.lx3_test_active_tx(1)
          counters['stale_tx_blocked_after_delivery_revoke'] += 1
      host_tick = ms % 10 == 0
      panda_tick = ms >= offset and (ms - offset) % 100 == 0
      if host_tick and order == 'host_first': step_host(ms)
      if panda_tick: publish(ms)
      if host_tick and order == 'between': step_host(ms)
      if panda_tick: heartbeat(ms)
      if host_tick and order == 'host_last': step_host(ms)
      state = snapshot()
      current = (state.phase, state.accepted, state.generation, case.ctx.enabled, case.ctx.active,
                 int(case.ctx.lx3_engagement.mode))
      if current != previous:
        trace.append({'ms': ms, 'panda_phase': state.phase, 'panda_mode': state.accepted,
                      'generation': state.generation, 'host_enabled': case.ctx.enabled,
                      'host_active': case.ctx.active, 'host_mode': int(case.ctx.lx3_engagement.mode)})
        previous = current
    state = snapshot()
    actual = int(case.ctx.lx3_engagement.mode)
    assert actual == expected and state.accepted == expected and bool(state.allowed) == bool(expected), (spec, expected, actual, trace)
    assert not expected or case.ctx.active, (spec, trace)
    assert not expected or lib.lx3_test_rx_valid(), (spec, trace)
    if scenario in ('no_entry_before_ack', 'no_entry_after_ack', 'brake'):
      assert last_off_host is not None and last_heartbeat is not None
    if scenario == 'no_entry_after_ack':
      assert first_grant is not None
    if scenario == 'angle_delivery_revoke':
      assert first_grant is not None and delivery_revoke_ms == 850
      assert host_delivery_revoke_ms is not None
      # Includes 10Hz companion publication plus a possible next host tick.
      assert 0 <= host_delivery_revoke_ms - delivery_revoke_ms <= 110, (spec, trace)
      counters['delivery_revoke_host_delay_ms'] = host_delivery_revoke_ms - delivery_revoke_ms
  return {'spec': list(spec), 'counts': dict(counters), 'trace': trace}


def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('--library', required=True)
  parser.add_argument('--report')
  args = parser.parse_args()
  lib = library(args.library)
  specs = list(itertools.product((0, 30, 90), (0, 80), (0, 30, 90),
                                 ('host_first', 'between', 'host_last'), ('lateral', 'combined', 'upgrade', 'cancel', 'brake')))
  # Denial/rapid-toggle deadlines require specified short CAN delivery latency;
  # long transport delays may intentionally reject or reinterpret a later
  # gesture, and are tested separately rather than assigned an invented mode.
  specs.extend(itertools.product((0, 10), (0,), (0, 30, 90), ('host_first', 'between', 'host_last'),
                                  ('quick_denied_retry', 'denied_then_retry', 'rapid_toggle',
                                   'no_entry_before_ack', 'no_entry_after_ack')))
  specs.extend(itertools.product((0, 30, 90), (0, 80), (0, 30, 90),
                                  ('host_first', 'between', 'host_last'), ('angle_delivery_revoke',)))
  results = [run_case(lib, spec) for spec in specs]
  total = Counter()
  for result in results: total.update(result['counts'])
  report = {'scope': __doc__, 'cases': len(results), 'counts': dict(total), 'results': results}
  if args.report: Path(args.report).write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
  print('PASS actual C + production host:', len(results), 'delivery/phase/scenario schedules;', dict(total))


if __name__ == '__main__':
  main()
