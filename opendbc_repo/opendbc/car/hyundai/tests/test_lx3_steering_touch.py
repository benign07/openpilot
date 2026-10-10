"""LX3 original-bus wheel contact must be fresh, intact, and receive-only."""
from unittest.mock import Mock

from opendbc.can import CANParser
from opendbc.car import Bus, structs
from opendbc.car.hyundai.hyundaicanfd import hkg_can_fd_checksum
from opendbc.car.hyundai.steering_touch import LX3SteeringTouch
from opendbc.car.hyundai.values import CAR


DBC = "hyundai_canfd_lx3_hev_generated"
# Sanitized original-bus vectors at the recorded parked hand-on/off transition.
RELEASE = bytes.fromhex("5070ee00000000000000000100000100")
TOUCH_TRANSITION = bytes.fromhex("3fd3f000000000000000030120190100")
TOUCH_HELD = bytes.fromhex("c3a7f200000000000000040138320100")
TOUCH_NEXT = bytes.fromhex("98a9f40000000000000004013b320100")
RELEASE_TRANSITION = bytes.fromhex("4a03b400000000000000010105040100")


def receive(cp, monitor, timestamp, data, bus=0):
  cp.update([timestamp, [(LX3SteeringTouch.ADDR, data, bus)]])
  return monitor.update(cp)


def repair_crc(data):
  data = bytearray(data)
  data[:2] = hkg_can_fd_checksum(LX3SteeringTouch.ADDR, None, data).to_bytes(2, "little")
  return bytes(data)


def setup():
  cp = CANParser(DBC, [], 0)
  cp.controls_ready = True  # Production discovers optional messages after startup.
  return cp, LX3SteeringTouch()


def test_recorded_parked_release_to_contact_and_monitoring_contract():
  cp, monitor = setup()
  assert not monitor.update(cp)["available"]
  assert not receive(cp, monitor, 1_000_000_000, RELEASE)["valid"]  # optional discovery
  assert not receive(cp, monitor, 1_200_000_000, TOUCH_TRANSITION)["valid"]  # first parsed frame
  state = receive(cp, monitor, 1_400_000_000, TOUCH_HELD)
  assert state["valid"] and state["touched"] and state["rawStatus"] == 4
  assert state["rawTouch1"] == 56 and state["rawTouch2"] == 50
  car_state = structs.CarState(steeringTouch=state, steeringPressed=False)
  assert car_state.steeringTouch.touched and not car_state.steeringPressed
  assert receive(cp, monitor, 1_600_000_000, TOUCH_NEXT)["touched"]

  # Release uses a real original-bus frame, with no torque threshold or camera.
  released = bytearray(RELEASE_TRANSITION)
  released[2] = (TOUCH_NEXT[2] + 2) % 256
  state = receive(cp, monitor, 1_800_000_000, repair_crc(released))
  assert state["valid"] and not state["touched"]


def test_crc_counter_layout_and_staleness_revoke_contact():
  cp, monitor = setup()
  receive(cp, monitor, 1_000_000_000, RELEASE)
  receive(cp, monitor, 1_200_000_000, TOUCH_TRANSITION)
  assert receive(cp, monitor, 1_400_000_000, TOUCH_HELD)["touched"]

  bad_crc = bytearray(TOUCH_NEXT)
  bad_crc[0] ^= 1
  assert not receive(cp, monitor, 1_600_000_000, bad_crc)["touched"]
  assert not receive(cp, monitor, 1_800_000_000, TOUCH_NEXT)["touched"]  # reconnect first

  repeat = bytearray(TOUCH_NEXT)
  repeat[2] = (TOUCH_NEXT[2] + 2) % 256
  assert receive(cp, monitor, 2_000_000_000, repair_crc(repeat))["touched"]
  cp.update([2_250_000_001, []])
  assert not monitor.update(cp)["touched"]

  jumped = bytearray(TOUCH_NEXT)
  jumped[2] = (repeat[2] + 4) % 256
  assert not receive(cp, monitor, 2_400_000_000, repair_crc(jumped))["touched"]
  assert not receive(cp, monitor, 2_600_000_000, TOUCH_NEXT[:-1])["touched"]
  malformed = bytearray(TOUCH_NEXT)
  malformed[11] = 2
  assert not receive(cp, monitor, 2_800_000_000, repair_crc(malformed))["touched"]


def test_recorded_continuous_regrip_above_bit_five_window():
  cp, monitor = setup()
  # Original CRC-valid bus-0 frames across 11:29:30.468–31.066: a continuous
  # grip reaches byte-12 magnitude 64, clearing bit 5 without release.
  frames = (
    "3d010a00000000000000040121220100",  # discover 0x208
    "4a3b0c0000000000000004013c330100",  # +2, magnitude 60
    "63f00e0000000000000004013e330100",  # +2, magnitude 62
    "ed0a1000000000000000040140350100",  # +2, magnitude 64
    "844a1200000000000000040140350100",  # +2, magnitude 64
  )
  states = [receive(cp, monitor, 1_000_000_000 + index * 200_000_000, bytes.fromhex(frame))
            for index, frame in enumerate(frames)]
  assert all(state["valid"] and state["touched"] for state in states[2:])
  assert [state["rawTouch1"] for state in states[2:]] == [62, 64, 64]


def test_forwarded_buses_do_not_create_original_contact_or_can_fault():
  cp, monitor = setup()
  for source in (2, 128, 130):
    assert not receive(cp, monitor, 1_000_000_000, TOUCH_HELD, source)["available"]
  assert LX3SteeringTouch.ADDR not in cp.addresses
  assert cp.can_valid
  receive(cp, monitor, 1_200_000_000, RELEASE)
  assert LX3SteeringTouch.ADDR in cp.addresses
  assert cp.message_states[LX3SteeringTouch.ADDR].ignore_alive
  assert not receive(cp, monitor, 1_400_000_000, TOUCH_TRANSITION)["valid"]
  assert receive(cp, monitor, 1_600_000_000, TOUCH_HELD)["touched"]


def test_lx3_carstate_selects_private_receive_profile(monkeypatch):
  from opendbc.car.hyundai import carstate, hyundaicanfd
  params = Mock()
  params.get_int.return_value = 0
  params.get_bool.return_value = False
  params.get.return_value = '{0: {}, 1: {}, 2: {}}'
  monkeypatch.setattr(carstate, 'Params', lambda: params)
  monkeypatch.setattr(hyundaicanfd, 'Params', lambda: params)
  platform = CAR.HYUNDAI_PALISADE_LX3_HEV
  cp_config = structs.CarParams(carFingerprint=platform, flags=int(platform.config.flags), safetyConfigs=[{}])
  state = carstate.CarState(cp_config)
  assert isinstance(state.steering_touch, LX3SteeringTouch)
  cp = state.get_can_parsers_canfd(cp_config)[Bus.pt]
  cp.controls_ready = True
  receive(cp, state.steering_touch, 1_000_000_000, RELEASE)
  receive(cp, state.steering_touch, 1_200_000_000, TOUCH_TRANSITION)
  assert receive(cp, state.steering_touch, 1_400_000_000, TOUCH_HELD)["touched"]
  assert 0x2AF not in cp.addresses
