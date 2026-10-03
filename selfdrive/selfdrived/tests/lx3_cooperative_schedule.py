"""Production torque block + suspension + DBC packer + actual Panda C.

Synthetic 100 Hz sensors, isolated native grant, fixed zero angle target.
No device I/O; this is a scheduling test, not vehicle/whole-controller replay.
"""
import argparse
import ast
import copy
import ctypes as C
import hashlib
import json
import math
from pathlib import Path
from types import SimpleNamespace as NS

import numpy as np

from selfdrive.carrot.tests.test_lx3_can_time import DBC_FILE, ENV, ROOT, definitions
from selfdrive.selfdrived.tests.lx3_host_panda_joint import Companion, library


def torque_function():
  path = ROOT / 'opendbc_repo/opendbc/car/hyundai/carcontroller.py'
  tree = ast.parse(path.read_text(encoding='utf-8'))
  cls = next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'CarController')
  update = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == 'update')
  start = next(i for i, n in enumerate(update.body) if isinstance(n, ast.Assign) and
               isinstance(n.targets[0], ast.Name) and n.targets[0].id == 'steering_pressed_rising')
  stop = next(i for i, n in enumerate(update.body) if isinstance(n, ast.Assign) and
              isinstance(n.targets[0], ast.Attribute) and n.targets[0].attr == 'steering_pressed_prev')
  function = ast.parse('def step(self, CS, CC):\n  pass').body[0]
  function.body = update.body[start:stop + 1]
  constants = [n for n in tree.body if isinstance(n, ast.Assign) and isinstance(n.targets[0], ast.Name)
               and n.targets[0].id in ('DRIVER_TORQUE_FILTER_TAU', 'PRE_OVERRIDE_PREDICTION_TIME',
                                       'PRE_OVERRIDE_START_RATIO', 'PRE_OVERRIDE_MAX_TORQUE_DELTA')]
  env = {'np': np, 'DT_CTRL': .01}
  exec(compile(ast.fix_missing_locations(ast.Module(body=constants + [function], type_ignores=[])), str(path), 'exec'), env)
  return env['step']


def suspension():
  env = {'math': math, 'DT_CTRL': .01,
         'Params': lambda: NS(get_int=lambda name: {'LatSuspendAngleDeg': 300, 'LaneChangeNeedTorque': -1}[name])}
  definitions(ROOT / 'selfdrive/carrot/carrot_controls.py', env, {'CarrotControls'})
  return env['CarrotControls'](NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'))


def run(lib, delay, sign):
  lib.lx3_test_cooperative_start()
  step = torque_function()
  suspend = suspension()
  context = NS(lx3_cluster=object(), params=NS(STEER_THRESHOLD=250., ANGLE_MIN_TORQUE=25.),
               lkas_max_torque=250., angle_max_torque=250., steering_pressed_prev=False,
               full_recovery_frames=0, recovering_from_override=False, repeated_override_count=0,
               override_latched=False, override_release_frames=0, driver_torque_filtered=0.,
               driver_torque_filtered_prev=0.)
  env = dict(ENV, copy=copy)
  definitions(ROOT / 'opendbc_repo/opendbc/can/packer.py', env)
  definitions(ROOT / 'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py', env,
              {'create_steering_messages_camera_scc', 'oem_emergency_steering'})
  packer = env['CANPacker'](str(DBC_FILE))
  cp, can, cc = NS(carFingerprint='HYUNDAI_PALISADE_LX3_HEV'), NS(ECAN=0, CAM=2), NS(latActive=True)
  samples = [0] * 20 + [170, 236] + [sign * 497] * 100 + [0] * 300
  caps = []
  pressed_count = 0
  for index, effort in enumerate(samples):
    us = 1_000_000 + index * 10_000
    lib.lx3_test_cooperative_sensor(us, 0, effort)
    received = samples[max(0, index - delay)]
    pressed_count = pressed_count + 1 if abs(received) > 250 else 0
    out = NS(steeringTorque=received, steeringPressed=pressed_count > 5, steeringAngleDeg=0.,
             leftBlinker=False, rightBlinker=False, latEnabled=True)
    cs = NS(out=out, modelV2=None, mdps=None, steer_touch_2af=None, adrv_0x161=None, lfa=None,
            lfa_alt={'COUNTER': index % 256, 'LKAS_ANGLE_ACTIVE': 1})
    cc.latActive = suspend.lat_suspend_control(out, True, us * 1000)
    assert cc.latActive, (delay, sign, index, 'ordinary effort suspended lateral')
    step(context, cs, cc)
    sends = env['create_steering_messages_camera_scc'](
      index, packer, cp, can, cc, True, 0, cs, 0., context.lkas_max_torque, True)
    address, payload, bus = next(message for message in sends if message[0] == 0xCB)
    assert lib.lx3_test_packet_tx(address, bus, len(payload), (C.c_uint8 * len(payload)).from_buffer_copy(payload)), (
      delay, sign, index, 'TX rejected', effort, received, context.lkas_max_torque)
    original = bytearray(24)
    original[2], original[3] = index % 256, 0x10
    original[:2] = env['hkg_can_fd_checksum'](0xCB, None, original).to_bytes(2, 'little')
    output = (C.c_uint8 * 24)()
    assert lib.lx3_test_packet_fwd(0xCB, 2, 24, (C.c_uint8 * 24).from_buffer_copy(original), output) == 0
    assert (output[3] >> 4) & 3 == 2, (delay, sign, index, 'no active insertion')
    state = Companion()
    lib.lx3_test_state(C.byref(state))
    assert state.allowed and state.accepted == 1, (delay, sign, index, 'permission lost')
    caps.append(output[6])
  assert min(caps) == 25 and caps[-1] == 250
  return {'delay_ticks': delay, 'sign': sign, 'ticks': len(samples), 'min_cap': min(caps),
          'final_cap': caps[-1], 'tx_rejected': 0, 'lateral_suspensions': 0}


def main():
  parser = argparse.ArgumentParser(description=__doc__)
  parser.add_argument('--library', required=True)
  parser.add_argument('--report', required=True)
  args = parser.parse_args()
  lib = library(args.library)
  lib.lx3_test_cooperative_start.argtypes, lib.lx3_test_cooperative_start.restype = [], None
  lib.lx3_test_cooperative_sensor.argtypes = [C.c_uint32, C.c_int, C.c_int]
  lib.lx3_test_cooperative_sensor.restype = None
  records = [run(lib, delay, sign) for delay in (0, 1, 2, 3) for sign in (-1, 1)]
  paths = ('selfdrive/carrot/carrot_controls.py', 'opendbc_repo/opendbc/car/hyundai/carcontroller.py',
           'opendbc_repo/opendbc/car/hyundai/hyundaicanfd.py',
           'opendbc_repo/opendbc/safety/safety/safety_hyundai_canfd.h')
  result = {'scope': __doc__, 'schedules': records, 'source_sha256': {
    name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in paths}}
  Path(args.report).write_text(json.dumps(result, indent=2), encoding='utf-8')
  print(f'PASS: {len(records)} cooperative schedules, {sum(x["ticks"] for x in records)} ticks, production torque/DBC/native C')


if __name__ == '__main__':
  main()
