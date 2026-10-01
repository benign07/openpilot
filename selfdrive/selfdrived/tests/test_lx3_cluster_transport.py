"""Production cluster builders and real DBC packer/parser; no CAN/socket I/O."""
import ast
import copy
import math
import runpy
import random
from pathlib import Path
from types import SimpleNamespace as NS
import unittest

from selfdrive.carrot.tests.test_lx3_can_time import ENV, DBC_FILE, definitions

ROOT = Path(__file__).resolve().parents[3]
Lx3ClusterTransport = runpy.run_path(str(ROOT / 'opendbc_repo/opendbc/car/hyundai/lx3_cluster.py'))['Lx3ClusterTransport']


class TestLx3ClusterTransport(unittest.TestCase):
  def scc_warning_message(self, warning, guarded, enabled, stopping, override):
    definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', self.env, {'create_acc_control_scc2'})
    self.cs.scc_control = dict.fromkeys(self.packer.dbc.name_to_msg['SCC_CONTROL'].sigs, 0)
    self.cs.scc_control.update(zip(('SysFailState', 'TakeOverReq', 'DriverAlert'), warning))
    self.cs.scc_control['COUNTER'] = 19
    self.cs.out.gasPressed, self.cs.out.vEgo, self.cs.out.aEgo = override, 20.0, -1.0
    original = self.cs.scc_control.copy()
    message = self.env['create_acc_control_scc2'](
      self.env['CANPacker'](str(DBC_FILE)), self.can, enabled, -.9, -1., stopping, override, 80,
      self.hud, NS(carrot_cruise=0, jerk_u=3., jerk_l=3.), self.cs, lx3_guard=guarded)
    self.assertEqual(self.cs.scc_control, original)
    return message[1]

  def test_lx3_scc_preserves_original_fault_takeover_and_driver_alert_bits(self):
    # Test every raw two-bit state, including reserved values, without guessing
    # OEM meanings. Use the actual production builder, DBC and CAN packer.
    import itertools
    for warning in itertools.product(range(4), repeat=3):
      for enabled, stopping, override in itertools.product((True, False), repeat=3):
        raw = self.scc_warning_message(warning, True, enabled, stopping, override)
        for name, expected in zip(('SysFailState', 'TakeOverReq', 'DriverAlert'), warning):
          signal = self.packer.dbc.name_to_msg['SCC_CONTROL'].sigs[name]
          self.assertEqual(self.env['get_raw_value'](raw, signal), expected,
                           (name, warning, enabled, stopping, override))

  def test_lx3_scc_each_original_warning_survives_active_request(self):
    for index, name in enumerate(('SysFailState', 'TakeOverReq', 'DriverAlert')):
      for value in (1, 2, 3):
        with self.subTest(field=name, value=value):
          warning = [0, 0, 0]
          warning[index] = value
          raw = self.scc_warning_message(warning, True, True, False, False)
          signal = self.packer.dbc.name_to_msg['SCC_CONTROL'].sigs[name]
          self.assertEqual(self.env['get_raw_value'](raw, signal), value)

  def test_non_lx3_scc_retains_existing_warning_encoding(self):
    import itertools
    for warning in itertools.product(range(4), repeat=3):
      raw = self.scc_warning_message(warning, False, True, False, False)
      for name in ('SysFailState', 'TakeOverReq', 'DriverAlert'):
        signal = self.packer.dbc.name_to_msg['SCC_CONTROL'].sigs[name]
        self.assertEqual(self.env['get_raw_value'](raw, signal), 0)

  def setUp(self):
    self.errors = []
    self.env = dict(ENV, copy=copy, math=math,
                    carlog=NS(error=lambda *args: self.errors.append(args), warning=lambda *args: None),
                    Params=lambda: NS(get_int=lambda _: 0, get=lambda _: '0'),
                    HyundaiFlags=NS(CAMERA_SCC=NS(value=1)), CV=NS(MS_TO_KPH=3.6, MS_TO_MPH=2.236936),
                    _get_desire_and_lane_changing=lambda _: (0, 0))
    definitions(ROOT / 'opendbc_repo/opendbc/can/packer.py', self.env)
    names = {'create_tcs_messages', 'create_suppress_lfa', 'create_lfahda_cluster', 'create_lfa_icon_non_camera_scc', 'create_ccnc_messages', 'create_steering_messages_camera_scc', '_make_ccnc_cluster_msg',
             '_make_ccnc_values', '_suppress_trailer_mode_warning', '_apply_radar_blink'}
    definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', self.env, names)
    self.packer = self.env['CANPacker'](str(DBC_FILE))
    self.parser = self.env['CANParser'](str(DBC_FILE), [('ADRV_0x161', 20), ('CCNC_0x162', 20)], 0)
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', flags=1, openpilotLongitudinalControl=True)
    self.can, self.cc = NS(CAM=2, ECAN=0), NS(enabled=False, latActive=True, longActive=False, latEnabled=True)
    adrv = dict.fromkeys(self.packer.dbc.name_to_msg['ADRV_0x161'].sigs, 0)
    adrv.update(COUNTER=42, ALERTS_2=1, ALERTS_3=7, ALERTS_5=3, FCA_ALT_ICON=3,
                SOUNDS_1=1, SOUNDS_2=2, SOUNDS_3=3, SOUNDS_4=4, DAW_ICON=2,
                LANELINE_LEFT=5, LANELINE_RIGHT=4, LANELINE_CURVATURE=7)
    faults = dict.fromkeys(self.packer.dbc.name_to_msg['CCNC_0x162'].sigs, 0)
    faults.update(COUNTER=88, FAULT_LSS=1, FAULT_DAS=1)
    self.cs = NS(modelV2=None, lfahda_cluster=None, adrv_0x161=adrv, ccnc_0x162=faults,
                 adrv_0x200=None, adrv_0x1ea=None, cruise_buttons_msg=None, is_metric=True,
                 paddle_button_prev=0, softHoldActive=0, trailer_connected=False,
                 out=NS(cruiseState=NS(available=True), latEnabled=True, leftBlinker=False, rightBlinker=False,
                        leftBlindspot=False, rightBlindspot=False, leftLaneLine=0, rightLaneLine=0, steeringAngleDeg=0))
    self.hud = NS(leadVisible=False, leadDistance=0, activeCarrot=0, setSpeed=15, leadDistanceBars=2,
                  leftLaneVisible=True, rightLaneVisible=True, leftLaneDepart=False, rightLaneDepart=False)
    self.clock = 1_000_000_000
    self.cs.cp_cam = NS(vl={'ADRV_0x161': adrv, 'CCNC_0x162': faults},
                        ts_nanos={name: {'CHECKSUM': self.clock} for name in ('ADRV_0x161', 'CCNC_0x162')})
    self.cluster = Lx3ClusterTransport()

  def ccnc(self, frame=5):
    self.cluster.begin(self.cs, self.clock, self.cc.latActive or self.cc.longActive or self.cs.out.latEnabled)
    return self.env['create_ccnc_messages'](self.cp, self.packer, self.can, frame, self.cc, self.cs,
                                            self.hud, 0, False, False, 0, False, 9, self.cluster)

  def new_publication(self, name='ADRV_0x161', delta=50_000_000):
    self.clock += delta
    self.cs.cp_cam.ts_nanos[name]['CHECKSUM'] = self.clock
    self.cs.cp_cam.vl[name]['COUNTER'] = (self.cs.cp_cam.vl[name]['COUNTER'] + 2) % 256

  def decode(self, messages):
    self.parser.update([[1_000_000_000, messages]])
    return self.parser.vl

  def controller_cluster_calls(self, frame=5):
    path = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                any(isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and
                    x.func.attr == 'create_lfahda_cluster' for x in ast.walk(n)) and
                ast.unparse(n.test) == 'camera_scc and (self.frame % 5 == 0 or self.lx3_cluster is not None)')
    sends = []
    context = NS(frame=frame, camera_scc_params=3, CP=self.cp, packer=self.packer, CAN=self.can,
                 lx3_cluster=self.cluster if self.cp.carFingerprint == 'HYUNDAI_PALISADE_LX3_HEV' else None)
    env = dict(self.env, self=context, camera_scc=True, CC=self.cc, CS=self.cs, can_sends=sends,
               hyundaicanfd=NS(create_lfahda_cluster=lambda *args: [],
                              create_lfa_icon_non_camera_scc=self.env['create_lfa_icon_non_camera_scc']))
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), env)
    if self.cp.openpilotLongitudinalControl:
      sends.extend(self.ccnc(frame))
    return sends

  def test_only_one_lx3_adrv_frame_per_controller_cluster_cycle(self):
    for frame in (1, 3, 5, 7):
      self.new_publication()
      sends = self.controller_cluster_calls(frame)
      self.assertEqual(sum(addr == 0x161 and bus == 0 for addr, _, bus in sends), 1)

  def test_other_platforms_keep_two_existing_cluster_builders(self):
    self.cp.carFingerprint = 'OTHER_PLATFORM'
    sends = self.controller_cluster_calls()
    self.assertEqual(sum(addr == 0x161 and bus == 0 for addr, _, bus in sends), 2)
    decoded = self.decode(sends)['ADRV_0x161']
    self.assertEqual(decoded['ALERTS_3'], 0)

  def test_unsupported_stock_long_and_missing_cache_do_not_synthesize_cluster(self):
    self.cp.openpilotLongitudinalControl = False
    self.assertEqual(self.controller_cluster_calls(), [])
    self.cp.openpilotLongitudinalControl = True
    self.cs.adrv_0x161 = None
    self.assertFalse(any(addr == 0x161 for addr, _, _ in self.controller_cluster_calls()))

  def test_real_packed_lx3_warnings_sounds_and_lane_values_are_preserved(self):
    source = copy.deepcopy(self.cs.adrv_0x161)
    decoded = self.decode(self.ccnc())['ADRV_0x161']
    fields = ('ALERTS_1', 'ALERTS_2', 'ALERTS_3', 'ALERTS_4', 'ALERTS_5', 'SOUNDS_1', 'SOUNDS_2',
              'SOUNDS_3', 'SOUNDS_4', 'FCA_ALT_ICON', 'DAW_ICON', 'LANELINE_LEFT', 'LANELINE_RIGHT', 'LANELINE_CURVATURE')
    for name in fields:
      self.assertEqual(decoded[name], source[name], name)
    self.assertEqual(self.cs.adrv_0x161, source)

  def test_real_packed_lx3_crc_counter_and_fault_fields(self):
    sends = self.ccnc()
    for addr, data, _ in sends:
      self.assertEqual(int.from_bytes(data[:2], 'little'), self.env['hkg_can_fd_checksum'](addr, None, data))
    decoded = self.decode(sends)
    self.assertEqual(decoded['ADRV_0x161']['COUNTER'], 42)
    self.assertEqual(decoded['CCNC_0x162']['COUNTER'], 88)
    self.assertEqual(decoded['CCNC_0x162']['FAULT_LSS'], 1)
    self.assertEqual(decoded['CCNC_0x162']['FAULT_DAS'], 1)
    self.assertEqual(self.errors, [])

  def test_corrupt_original_cluster_cannot_refresh_warning_or_emergency_cache(self):
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    parser._add_message('ADRV_0x161')
    data = next(data for addr, data, _ in self.ccnc() if addr == 0x161)
    parser.update([[1_000_000_000, [(0x161, data, 2)]]])
    self.assertEqual(parser.vl['ADRV_0x161']['ALERTS_3'], 7)
    stamp = parser.ts_nanos['ADRV_0x161']['ALERTS_1']
    corrupt = bytearray(data)
    corrupt[16] = 21  # False emergency AL1, without a matching checksum.
    parser.update([[1_050_000_000, [(0x161, bytes(corrupt), 2)]]])
    self.assertEqual(parser.vl['ADRV_0x161']['ALERTS_1'], 0)
    self.assertEqual(parser.ts_nanos['ADRV_0x161']['ALERTS_1'], stamp)

  def test_inactive_and_pre_enabled_preserve_original_display(self):
    self.cc.latActive = self.cs.out.latEnabled = False
    self.assertEqual(self.ccnc(), [])
    self.cc.enabled = True  # PRE_ENABLE or legacy long-enabled is not display ownership.
    self.assertEqual(self.ccnc(), [])
    self.cc.latActive = True
    self.assertEqual(self.ccnc(), [])  # Don't replay the last inactive publication on engage.
    self.new_publication()
    self.assertEqual([m[0] for m in self.ccnc()], [0x161])

  def test_lateral_only_and_suspended_lateral_use_actual_active_state(self):
    self.assertFalse(self.cc.enabled)
    values = self.decode(self.ccnc())['ADRV_0x161']
    self.assertEqual(values['LFA_ICON'], 2)
    # Preserve the inherited stock-availability ready indication. It is not
    # combined permission: LAT-only must never show the active set-speed3.
    self.assertTrue(self.cs.out.cruiseState.available)
    self.assertEqual(values['SETSPEED'], 1)
    self.assertEqual(values['SETSPEED_HUD'], 1)
    self.assertEqual(values['HDA_ICON'], 1)
    self.new_publication()
    self.cc.latActive = False
    self.assertEqual(self.decode(self.ccnc())['ADRV_0x161']['LFA_ICON'], 1)
    self.cs.out.latEnabled = False
    self.assertEqual(self.ccnc(), [])

  def test_combined_set_speed_display_follows_permission_without_stock_scc_latch(self):
    self.cs.out.cruiseState.available = False
    self.cc.enabled = self.cc.longActive = True
    values = self.decode(self.ccnc())['ADRV_0x161']
    self.assertEqual(values['SETSPEED'], 3)
    self.assertEqual(values['SETSPEED_HUD'], 3)
    self.assertEqual(values['HDA_ICON'], 2)
    self.assertEqual(values['LFA_ICON'], 2)
    self.assertEqual(values['ALERTS_3'], 7)
    self.new_publication()
    self.cc.enabled = self.cc.longActive = False
    values = self.decode(self.ccnc())['ADRV_0x161']
    self.assertEqual(values['SETSPEED'], 0)
    self.assertEqual(values['SETSPEED_HUD'], 0)
    self.assertEqual(values['LFA_ICON'], 2)

  def test_other_platform_set_speed_keeps_stock_availability_contract(self):
    self.cp.carFingerprint = 'OTHER_PLATFORM'
    self.cs.out.cruiseState.available = False
    self.cc.enabled = self.cc.longActive = True
    values = self.decode(self.ccnc())['ADRV_0x161']
    self.assertEqual(values['SETSPEED'], 0)
    self.assertEqual(values['SETSPEED_HUD'], 0)

  def test_one_publication_is_not_repeated_and_new_counter_wrap_is_allowed(self):
    self.cs.adrv_0x161['COUNTER'] = 254
    self.assertEqual(len(self.ccnc()), 2)
    self.clock += 50_000_000
    self.assertEqual(self.ccnc(), [])
    self.new_publication(delta=0)
    sends = self.ccnc()
    self.assertEqual([m[0] for m in sends], [0x161])
    self.assertEqual(sends[0][1][2], 0)

  def test_stale_future_and_unreceived_sources_are_not_transmitted(self):
    for age in (100_000_001, -1):
      self.cluster = Lx3ClusterTransport()
      self.clock = 1_000_000_000 + age
      self.assertEqual(self.ccnc(), [])
    self.clock = 1_000_000_000
    for stamps in self.cs.cp_cam.ts_nanos.values():
      stamps['CHECKSUM'] = 0
    self.assertEqual(self.ccnc(), [])

  def test_parser_or_clock_reset_needs_new_source(self):
    self.assertEqual(len(self.ccnc()), 2)
    self.cs.cp_cam = copy.copy(self.cs.cp_cam)
    self.new_publication()
    self.assertEqual(self.ccnc(), [])
    self.new_publication()
    self.assertEqual([m[0] for m in self.ccnc()], [0x161])
    self.clock -= 100_000_000
    self.assertEqual(self.ccnc(), [])

  def test_detached_cached_values_cannot_claim_a_new_publication(self):
    self.cs.adrv_0x161 = copy.copy(self.cs.adrv_0x161)
    self.assertEqual([m[0] for m in self.ccnc()], [0x162])

  def test_guarded_lx3_has_no_tcs_feedback_rewrite_producer(self):
    self.cs.tcs = dict.fromkeys(self.packer.dbc.name_to_msg['TCS'].sigs, 0)
    self.cs.tcs['DriverBraking'] = 1
    before = dict(self.cs.tcs)
    self.assertEqual(self.env['create_tcs_messages'](self.packer, self.can, self.cs, lx3_guard=True), [])
    self.assertEqual(self.cs.tcs, before)

  def test_legacy_camera_scc_tcs_workaround_keeps_default_behavior(self):
    self.cs.tcs = dict.fromkeys(self.packer.dbc.name_to_msg['TCS'].sigs, 0)
    self.cs.tcs.update(DriverBraking=1, DriverBrakingLowSens=1, ACC_REQ=1, COUNTER=42)
    before = dict(self.cs.tcs)
    messages = self.env['create_tcs_messages'](self.packer, self.can, self.cs)
    self.assertEqual(messages, self.env['create_tcs_messages'](self.packer, self.can, self.cs, lx3_guard=False))
    self.assertEqual([(addr, bus) for addr, _, bus in messages], [(0x175, 2)])
    parser = self.env['CANParser'](str(DBC_FILE), [('TCS', 50)], 2)
    parser.update([[self.clock, messages]])
    self.assertEqual(parser.vl['TCS']['DriverBraking'], 0)
    self.assertEqual(parser.vl['TCS']['DriverBrakingLowSens'], 0)
    self.assertEqual(self.cs.tcs, before)

  def test_all_five_display_headers_validate_and_corruption_does_not_refresh_source(self):
    names = Lx3ClusterTransport.SOURCES
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    self.assertIn(0x162, parser.addresses)
    for name in names:
      if parser.dbc.name_to_msg[name].address not in parser.addresses:
        parser._add_message(name)
      self.assertEqual(parser.dbc.name_to_msg[name].sigs['COUNTER'].type, 0)
      values = dict.fromkeys(self.packer.dbc.name_to_msg[name].sigs, 0)
      values['COUNTER'] = 42
      addr, data, _ = self.env['_make_ccnc_cluster_msg'](self.packer, name, 2, values, True, 42)
      parser.update([[self.clock, [(addr, data, 2)]]])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock)
      corrupt = bytearray(data)
      corrupt[-1] ^= 1
      parser.update([[self.clock + 10_000_000, [(addr, bytes(corrupt), 2)]]])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock, name)
      # Real parser accepts the actual +2 counter; no implicit +1 counter validator.
      values['COUNTER'] = 44
      addr, data, _ = self.env['_make_ccnc_cluster_msg'](self.packer, name, 2, values, True, 44)
      parser.update([[self.clock + 20_000_000, [(addr, data, 2)]]])
      self.assertEqual(parser.vl[name]['COUNTER'], 44, name)

  def test_unknown_original_bits_survive_real_dbc_round_trip(self):
    # Preserve unmapped stock bits without assigning them a guessed meaning.
    # Whole-byte equality also covers signed/scaled and Motorola signals.
    rng = random.Random(161162)
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    self.assertIn(0x162, parser.addresses)
    for name in Lx3ClusterTransport.SOURCES:
      if parser.dbc.name_to_msg[name].address not in parser.addresses:
        parser._add_message(name)
      msg = self.packer.dbc.name_to_msg[name]
      for seq in range(100):
        raw = bytearray(rng.randbytes(msg.size))
        raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
        parser.update([[self.clock + seq * 50_000_000, [(msg.address, bytes(raw), 2)]]])
        values = dict(parser.vl[name])
        _, rebuilt, _ = self.env['_make_ccnc_cluster_msg'](self.packer, name, 0, values, True, raw[2])
        self.assertEqual(rebuilt, raw, (name, seq))

  def test_actual_controller_suppression_branch_requires_lx3_accepted_session(self):
    path = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                any(isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and
                    x.func.attr == 'create_suppress_lfa' for x in ast.walk(n)) and
                ast.unparse(n.test).startswith('self.frame % 5 == 0 and hda2'))
    self.cs.cam_0x362 = dict.fromkeys(self.packer.dbc.name_to_msg['CAM_0x362'].sigs, 0)
    self.cs.cam_0x2a4 = None
    self.can.ACAN = 1
    for lx3 in (False, True):
      for session_active, lat_active in ((False, False), (False, True), (True, False), (True, True)):
        self.cs.out.latEnabled, self.cc.latActive = session_active, lat_active
        sends = []
        context = NS(frame=5, camera_scc_params=3, packer=self.packer, CAN=self.can,
                     lx3_cluster=self.cluster if lx3 else None)
        env = dict(self.env, self=context, camera_scc=True, hda2=True, CC=self.cc, CS=self.cs, can_sends=sends,
                   hyundaicanfd=NS(create_suppress_lfa=self.env['create_suppress_lfa']))
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), env)
        self.assertEqual(len(sends), int(not lx3 or session_active), (lx3, session_active, lat_active))
        if sends:
          self.assertEqual((sends[0][0], sends[0][2]), (0x362, 1))

  def test_display_dbc_has_no_overlapping_fields_and_covers_every_bit(self):
    for name in Lx3ClusterTransport.SOURCES:
      msg = self.packer.dbc.name_to_msg[name]
      covered = bytearray(msg.size)
      for sig in msg.sigs.values():
        bits = bytearray(msg.size)
        self.env['set_value'](bits, sig, (1 << sig.size) - 1)
        self.assertFalse(any(a & b for a, b in zip(covered, bits)), (name, sig.name))
        covered = bytearray(a | b for a, b in zip(covered, bits))
      self.assertEqual(covered, bytes([255] * msg.size), name)

  def lfa_original(self, bit79):
    # Actual CRC-valid factory LFA capture. Bit79 has no validated meaning.
    # Both values must be copied, rather than inventing a fixed enable bit.
    raw = bytearray.fromhex('a4f29500800108000081000400640000')
    raw[9] = (raw[9] & 0x7f) | (bit79 << 7)
    raw[:2] = self.env['hkg_can_fd_checksum'](0x12a, None, raw).to_bytes(2, 'little')
    parser = self.env['CANParser'](str(DBC_FILE), [('LFA', 100)], 2)
    parser.update([[self.clock, [(0x12a, bytes(raw), 2)]]])
    return bytes(raw), dict(parser.vl['LFA'])

  def test_lfa_original_bit79_survives_parser_packer_round_trip(self):
    for bit79 in (0, 1):
      raw, values = self.lfa_original(bit79)
      _, rebuilt, _ = self.packer.make_can_msg('LFA', 0, values)
      self.assertEqual(rebuilt, raw, bit79)

  def test_camera_scc_lfa_keeps_original_bit79_without_changing_carrot_commands(self):
    for bit79 in (0, 1):
      for active in (False, True):
        for emergency in (False, True):
          raw, values = self.lfa_original(bit79)
          self.cs.mdps = None
          self.cs.lfa_alt = None
          self.cs.lfa = values
          self.cs.adrv_0x161 = {'ALERTS_1': 11 if emergency else 0}
          self.cc.latActive = active
          [(addr, rebuilt, bus)] = self.env['create_steering_messages_camera_scc'](
            0, self.packer, self.cp, self.can, self.cc, active, 0, self.cs, 0, 25, True)
          self.assertEqual((addr, bus), (0x12a, 0))
          self.assertEqual(rebuilt[9] & 0x80, raw[9] & 0x80, (bit79, active, emergency))
          if emergency:
            self.assertEqual(rebuilt[3:], raw[3:])
          else:
            # Original carrot camera-SCC values are retained; this fix only
            # preserves an OEM field lost by decode/re-encode.
            # Host buffered templates are restamped by Panda before wire TX;
            # inspect fields directly, without treating their old CRC as RX.
            def value(name):
              sig = self.packer.dbc.name_to_msg['LFA'].sigs[name]
              return self.env['get_raw_value'](rebuilt, sig) * sig.factor + sig.offset
            self.assertEqual(value('TORQUE_REQUEST'), -1024)
            self.assertEqual(value('NEW_SIGNAL_1'), 10)
            self.assertEqual(value('LKA_ACTIVE'), 3 if active else 0)

  def test_lfa_raw_bit79_does_not_overlap_existing_fields_or_enable_validation(self):
    msg = self.packer.dbc.name_to_msg['LFA']
    sig = msg.sigs['RAW_UNMAPPED_79']
    raw_mask = bytearray(msg.size)
    self.env['set_value'](raw_mask, sig, 1)
    self.assertEqual(raw_mask, bytes(9) + b'\x80' + bytes(6))
    self.assertEqual(sig.type, 0)
    self.assertEqual(msg.sigs['COUNTER'].type, 0)
    for other in msg.sigs.values():
      if other.name == sig.name:
        continue
      bits = bytearray(msg.size)
      self.env['set_value'](bits, other, (1 << other.size) - 1)
      self.assertFalse(any(a & b for a, b in zip(bits, raw_mask)), other.name)

  def test_lx3_camera_actuator_templates_only_refresh_from_crc_valid_originals(self):
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[2]
    for name in ('LFA', 'LFA_ALT', 'SCC_CONTROL'):
      parser._add_message(name)
      msg = parser.dbc.name_to_msg[name]
      raw = bytearray(msg.size)
      raw[2] = 42
      raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
      parser.update([[self.clock, [(msg.address, bytes(raw), 2)]]])
      before = dict(parser.vl[name])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock)
      corrupt = bytearray(raw)
      corrupt[3] ^= 1
      parser.update([[self.clock + 10_000_000, [(msg.address, bytes(corrupt), 2)]]])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock, name)
      self.assertEqual(dict(parser.vl[name]), before, name)
      raw[2] = 44  # Retain actual +2 OEM counters; no generic +1 validator.
      raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
      parser.update([[self.clock + 20_000_000, [(msg.address, bytes(raw), 2)]]])
      self.assertEqual(parser.vl[name]['COUNTER'], 44, name)
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock + 20_000_000, name)
      self.assertEqual(msg.sigs['COUNTER'].type, 0, name)

  def test_lx3_physical_mdps_tcs_and_wheels_ignore_corrupt_originals_and_recover(self):
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[0]
    for name in ('WHEEL_SPEEDS', 'MDPS', 'TCS'):
      parser._add_message(name)
      msg = parser.dbc.name_to_msg[name]
      raw = bytearray(msg.size)
      raw[2] = 42
      if name == 'WHEEL_SPEEDS':
        raw[8:10] = (320).to_bytes(2, 'little')
      raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
      parser.update([[self.clock, [(msg.address, bytes(raw), 0)]]])
      before = dict(parser.vl[name])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock)
      if name == 'WHEEL_SPEEDS':
        self.assertEqual(before['WHEEL_SPEED_1'], 10)
      corrupt = bytearray(raw)
      corrupt[8 if name == 'WHEEL_SPEEDS' else 5] ^= 1
      parser.update([[self.clock + 10_000_000, [(msg.address, bytes(corrupt), 0)]]])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock, name)
      self.assertEqual(parser.vl[name], before, name)
      raw[2] = 43  # Actual MDPS/TCS/wheel counters advance by +1.
      raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
      parser.update([[self.clock + 20_000_000, [(msg.address, bytes(raw), 0)]]])
      self.assertEqual(parser.ts_nanos[name]['CHECKSUM'], self.clock + 20_000_000, name)
      self.assertEqual(parser.vl[name]['COUNTER'], 43, name)
      self.assertEqual(msg.sigs['COUNTER'].type, 0, name)


  def test_lx3_hybrid_pedal_rejects_crc_corruption_and_recovers_without_counter_validation(self):
    parser = ENV['get_can_parsers_canfd'](None, NS(carFingerprint='lx3', flags=1))[0]
    parser._add_message('ACCELERATOR_ALT')
    name = 'ACCELERATOR_ALT'
    msg = parser.dbc.name_to_msg[name]
    raw = bytearray(32)
    raw[2] = 42
    raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
    parser.update([[self.clock, [(msg.address, bytes(raw), 0)]]])
    self.assertEqual(parser.vl[name]['ACCELERATOR_PEDAL'], 0)
    corrupt = bytearray(raw)
    corrupt[12] ^= 0x80  # Actual little-endian pedal bit103, not bit80.
    parser.update([[self.clock + 10_000_000, [(msg.address, bytes(corrupt), 0)]]])
    self.assertEqual(parser.vl[name]['ACCELERATOR_PEDAL'], 0)
    self.assertEqual(parser.ts_nanos[name]['ACCELERATOR_PEDAL'], self.clock)
    raw[12] = 0x80
    for step, counter in enumerate((44, 48), 2):  # Observed header increments +2/+4.
      raw[2] = counter
      raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
      parser.update([[self.clock + step * 10_000_000, [(msg.address, bytes(raw), 0)]]])
      self.assertEqual(parser.vl[name]['ACCELERATOR_PEDAL'], 0.25)
      self.assertEqual(parser.ts_nanos[name]['ACCELERATOR_PEDAL'], self.clock + step * 10_000_000)
    self.assertNotIn('COUNTER', msg.sigs)  # Header byte2 semantics stay unclaimed.
    self.assertIsNotNone(msg.sigs['CHECKSUM'].calc_checksum)
    corrupt_release = bytearray(raw)
    corrupt_release[12] = 0
    parser.update([[self.clock + 40_000_000, [(msg.address, bytes(corrupt_release), 0)]]])
    self.assertEqual(parser.vl[name]['ACCELERATOR_PEDAL'], 0.25)
    self.assertEqual(parser.ts_nanos[name]['ACCELERATOR_PEDAL'], self.clock + 30_000_000)
    raw[12] = 0
    raw[:2] = self.env['hkg_can_fd_checksum'](msg.address, None, raw).to_bytes(2, 'little')
    parser.update([[self.clock + 50_000_000, [(msg.address, bytes(raw), 0)]]])
    self.assertEqual(parser.vl[name]['ACCELERATOR_PEDAL'], 0)
    self.assertEqual(parser.ts_nanos[name]['ACCELERATOR_PEDAL'], self.clock + 50_000_000)


if __name__ == '__main__':
  unittest.main()
