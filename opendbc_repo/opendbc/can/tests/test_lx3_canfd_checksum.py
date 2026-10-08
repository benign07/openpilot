"""LX3 checksum and non-unit OEM counter-stride regression tests."""
import pytest

from opendbc.can import CANPacker, CANParser
from opendbc.can.dbc import DBC, SignalType
from opendbc.car.hyundai.hyundaicanfd import hkg_can_fd_checksum


DBC_NAME = 'hyundai_canfd_lx3_hev_generated'


@pytest.mark.parametrize('name', ('ADRV_0x161', 'CCNC_0x162', 'ADRV_0x1ea',
                                  'ADRV_0x200', 'LFAHDA_CLUSTER', 'LFA', 'LFA_ALT', 'MDPS'))
def test_lx3_packed_crc(name):
  packer = CANPacker(DBC_NAME)
  address, data, bus = packer.make_can_msg(name, 0, {})
  assert bus == 0
  assert packer.dbc.addr_to_msg[address].sigs['CHECKSUM'].type == SignalType.HKG_CAN_FD_CHECKSUM
  assert int.from_bytes(data[:2], 'little') == hkg_can_fd_checksum(address, None, bytearray(data))
  parser = CANParser(DBC_NAME, [(name, 20)], 0)
  assert parser.update([1_000_000_000, [(address, data, 0)]]) == {address}


@pytest.mark.parametrize('name,stride', (('GEAR_SHIFTER', 2), ('RADAR_0x21b', 3)))
def test_lx3_non_unit_counter_stride_is_not_rejected(name, stride):
  dbc = DBC(DBC_NAME)
  message = dbc.name_to_msg[name]
  assert message.sigs['COUNTER_RAW'].type == SignalType.DEFAULT
  packer = CANPacker(DBC_NAME)
  parser = CANParser(DBC_NAME, [(name, 20)], 0)
  for n in range(16):
    address, data, _ = packer.make_can_msg(name, 0, {'COUNTER_RAW': n * stride})
    assert parser.update([1_000_000_000 + n * 50_000_000, [(address, data, 0)]]) == {address}
  assert parser.message_states[message.address].counter_fail == 0
