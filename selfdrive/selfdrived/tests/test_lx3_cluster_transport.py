"""Production cluster builders and real DBC packer/parser; no CAN/socket I/O."""
import ast
import copy
import math
from pathlib import Path
from types import SimpleNamespace as NS
import unittest

from selfdrive.carrot.tests.test_lx3_can_time import ENV, DBC_FILE, definitions

ROOT = Path(__file__).resolve().parents[3]


class TestLx3ClusterTransport(unittest.TestCase):
  def setUp(self):
    self.errors = []
    self.env = dict(ENV, copy=copy, math=math,
                    carlog=NS(error=lambda *args: self.errors.append(args), warning=lambda *args: None),
                    Params=lambda: NS(get_int=lambda _: 0, get=lambda _: '0'),
                    HyundaiFlags=NS(CAMERA_SCC=NS(value=1)), CV=NS(MS_TO_KPH=3.6, MS_TO_MPH=2.236936),
                    _get_desire_and_lane_changing=lambda _: (0, 0))
    definitions(ROOT / 'opendbc_repo/opendbc/can/packer.py', self.env)
    names = {'create_lfa_icon_non_camera_scc', 'create_ccnc_messages', '_make_ccnc_cluster_msg',
             '_make_ccnc_values', '_suppress_trailer_mode_warning', '_apply_radar_blink'}
    definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', self.env, names)
    self.packer = self.env['CANPacker'](str(DBC_FILE))
    self.parser = self.env['CANParser'](str(DBC_FILE), [('ADRV_0x161', 20), ('CCNC_0x162', 20)], 0)
    self.cp = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV', flags=1, openpilotLongitudinalControl=True)
    self.can, self.cc = NS(CAM=2, ECAN=0), NS(enabled=False, latActive=False, longActive=False, latEnabled=False)
    adrv = dict.fromkeys(self.packer.dbc.name_to_msg['ADRV_0x161'].sigs, 0)
    adrv.update(COUNTER=42, ALERTS_2=1, ALERTS_3=7, ALERTS_5=3, FCA_ALT_ICON=3,
                SOUNDS_1=1, SOUNDS_2=2, SOUNDS_3=3, SOUNDS_4=4, DAW_ICON=2,
                LANELINE_LEFT=5, LANELINE_RIGHT=4, LANELINE_CURVATURE=7)
    faults = dict.fromkeys(self.packer.dbc.name_to_msg['CCNC_0x162'].sigs, 0)
    faults.update(COUNTER=88, FAULT_LSS=1, FAULT_DAS=1)
    self.cs = NS(modelV2=None, lfahda_cluster=None, adrv_0x161=adrv, ccnc_0x162=faults,
                 adrv_0x200=None, adrv_0x1ea=None, cruise_buttons_msg=None, is_metric=True,
                 paddle_button_prev=0, softHoldActive=0, trailer_connected=False,
                 out=NS(cruiseState=NS(available=True), latEnabled=False, leftBlinker=False, rightBlinker=False,
                        leftBlindspot=False, rightBlindspot=False, leftLaneLine=0, rightLaneLine=0, steeringAngleDeg=0))
    self.hud = NS(leadVisible=False, leadDistance=0, activeCarrot=0, setSpeed=15, leadDistanceBars=2,
                  leftLaneVisible=True, rightLaneVisible=True, leftLaneDepart=False, rightLaneDepart=False)

  def ccnc(self, frame=5):
    return self.env['create_ccnc_messages'](self.cp, self.packer, self.can, frame, self.cc, self.cs,
                                            self.hud, 0, False, False, 0, False, 9)

  def decode(self, messages):
    self.parser.update([[1_000_000_000, messages]])
    return self.parser.vl

  def controller_cluster_calls(self, frame=5):
    path = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
    tree = ast.parse(path.read_text(encoding='utf-8'))
    node = next(n for n in ast.walk(tree) if isinstance(n, ast.If) and
                any(isinstance(x, ast.Call) and isinstance(x.func, ast.Attribute) and
                    x.func.attr == 'create_lfahda_cluster' for x in ast.walk(n)) and
                ast.unparse(n.test) == 'self.frame % 5 == 0 and camera_scc')
    sends = []
    context = NS(frame=frame, camera_scc_params=3, CP=self.cp, packer=self.packer, CAN=self.can)
    env = dict(self.env, self=context, camera_scc=True, CC=self.cc, CS=self.cs, can_sends=sends,
               hyundaicanfd=NS(create_lfahda_cluster=lambda *args: [],
                              create_lfa_icon_non_camera_scc=self.env['create_lfa_icon_non_camera_scc']))
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(path), 'exec'), env)
    if self.cp.openpilotLongitudinalControl:
      sends.extend(self.ccnc(frame))
    return sends

  def test_only_one_lx3_adrv_frame_per_controller_cluster_cycle(self):
    for frame in (5, 10, 15, 20):
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
    for addr, data, _ in self.ccnc():
      self.assertEqual(int.from_bytes(data[:2], 'little'), self.env['hkg_can_fd_checksum'](addr, None, data))
    decoded = self.decode(self.ccnc())
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


if __name__ == '__main__':
  unittest.main()
